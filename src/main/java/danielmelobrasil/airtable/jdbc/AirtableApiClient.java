package danielmelobrasil.airtable.jdbc;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Minimal Airtable API client used by the JDBC implementation.
 */
class AirtableApiClient {

    private static final Logger LOGGER = Logger.getLogger(AirtableApiClient.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new JsonFactory().enable(JsonParser.Feature.AUTO_CLOSE_SOURCE));
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {};

    private final AirtableConfig config;

    AirtableApiClient(AirtableConfig config) {
        this.config = config;
    }

    List<Map<String, Object>> select(AirtableQuery query) throws SQLException {
        return executeSelect(
                query.getTableName(),
                query.getRequiredFieldsForBaseTable(),
                query.getFilterFormula(),
                query.getMaxRecords(),
                query.getSorts(),
                config.getDefaultView()
        );
    }

    List<Map<String, Object>> selectJoinTable(AirtableQuery query) throws SQLException {
        AirtableQuery.Join join = query.getJoin()
                .orElseThrow(() -> new SQLException("Join não definido para a consulta."));
        List<String> fields = query.getRequiredFieldsForJoinTable();
        if (fields.isEmpty()) {
            return Collections.emptyList();
        }
        return executeSelect(
                join.getTableName(),
                fields,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Optional.empty()
        );
    }

    private HttpURLConnection openConnection(String url, Duration timeout) throws IOException {
        URL endpoint = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) endpoint.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Authorization", "Bearer " + config.getApiKey());
        connection.setRequestProperty("Accept", "application/json");
        connection.setConnectTimeout((int) timeout.toMillis());
        connection.setReadTimeout((int) timeout.toMillis());
        return connection;
    }

    private List<Map<String, Object>> executeSelect(
            String tableName,
            List<String> selectedFields,
            Optional<String> filterFormula,
            Optional<Integer> maxRecords,
            List<AirtableQuery.Sort> sorts,
            Optional<String> view
    ) throws SQLException {
        HttpURLConnection connection = null;
        try {
            String url = buildSelectUrl(tableName, selectedFields, filterFormula, maxRecords, sorts, view);
            connection = openConnection(url, config.getTimeout());
            int status = connection.getResponseCode();
            String body = readResponseBody(connection, status);

            if (status >= 200 && status < 300) {
                return parseRecords(body);
            }

            LOGGER.log(Level.WARNING, "Airtable API request failed. Status: {0}, Body: {1}", new Object[]{status, body});
            throw new SQLException("Airtable API request failed with status " + status + ": " + body);
        } catch (IOException ex) {
            throw new SQLException("Unable to execute request against Airtable API.", ex);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private String buildSelectUrl(
            String tableName,
            List<String> selectedFields,
            Optional<String> filterFormula,
            Optional<Integer> maxRecords,
            List<AirtableQuery.Sort> sorts,
            Optional<String> view
    ) throws UnsupportedEncodingException {
        StringBuilder url = new StringBuilder();
        url.append(config.getApiBaseUrl());
        if (!config.getApiBaseUrl().endsWith("/")) {
            url.append('/');
        }
        url.append(config.getBaseId()).append('/');
        url.append(encodeComponent(tableName));

        Map<String, List<String>> params = new HashMap<>();

        if (!selectedFields.isEmpty()) {
            params.put("fields[]", new ArrayList<>(selectedFields));
        }

        view.ifPresent(v -> params.computeIfAbsent("view", key -> new ArrayList<>()).add(v));

        filterFormula.ifPresent(formula ->
                params.computeIfAbsent("filterByFormula", key -> new ArrayList<>()).add(formula));

        maxRecords.ifPresent(value ->
                params.computeIfAbsent("maxRecords", key -> new ArrayList<>()).add(String.valueOf(value)));

        if (!sorts.isEmpty()) {
            for (int i = 0; i < sorts.size(); i++) {
                AirtableQuery.Sort sort = sorts.get(i);
                params.computeIfAbsent(String.format(Locale.ENGLISH, "sort[%d][field]", i),
                        key -> new ArrayList<>()).add(sort.getField());
                params.computeIfAbsent(String.format(Locale.ENGLISH, "sort[%d][direction]", i),
                        key -> new ArrayList<>()).add(sort.getDirection().toString());
            }
        }

        if (!params.isEmpty()) {
            url.append('?').append(encodeQueryParameters(params));
        }

        return url.toString();
    }

    private static String encodeQueryParameters(Map<String, List<String>> params) throws UnsupportedEncodingException {
        StringJoiner joiner = new StringJoiner("&");
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            String key = encodeComponent(entry.getKey());
            for (String value : entry.getValue()) {
                joiner.add(key + "=" + encodeComponent(value));
            }
        }
        return joiner.toString();
    }

    private static String encodeComponent(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
    }

    private static String readResponseBody(HttpURLConnection connection, int status) throws IOException {
        InputStream stream = status >= 200 && status < 300 ? connection.getInputStream() : connection.getErrorStream();
        if (stream == null) {
            return "";
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[2048];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                builder.append(buffer, 0, read);
            }
            return builder.toString();
        }
    }

    private List<Map<String, Object>> parseRecords(String body) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(body);
        JsonNode recordsNode = node.get("records");
        if (recordsNode == null || !recordsNode.isArray()) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> records = new ArrayList<>();
        for (JsonNode recordNode : recordsNode) {
            JsonNode fieldsNode = recordNode.get("fields");
            if (fieldsNode == null || fieldsNode.isNull()) {
                records.add(Collections.<String, Object>emptyMap());
            } else {
                Map<String, Object> fields = OBJECT_MAPPER.convertValue(fieldsNode, MAP_TYPE);
                records.add(fields);
            }
        }
        return records;
    }
}
