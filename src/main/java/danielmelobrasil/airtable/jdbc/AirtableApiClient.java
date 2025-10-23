package danielmelobrasil.airtable.jdbc;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Minimal Airtable API client used by the JDBC implementation.
 */
class AirtableApiClient {

    private static final Logger LOGGER = Logger.getLogger(AirtableApiClient.class.getName());

    private final AirtableConfig config;
    private final Object fieldTypesCacheLock = new Object();
    private final Map<String, Map<String, String>> fieldTypesCache = new HashMap<>();

    AirtableApiClient(AirtableConfig config) {
        this.config = config;
    }

    List<Map<String, Object>> select(AirtableQuery query) throws SQLException {
        Map<String, String> fieldTypes = getFieldTypes(query.getTableName());
        return executeSelect(
                query.getTableName(),
                query.getRequiredFieldsForBaseTable(),
                query.getFilterFormula(),
                query.getMaxRecords(),
                query.getSorts(),
                config.getDefaultView(),
                fieldTypes
        );
    }

    List<Map<String, Object>> selectJoinTable(AirtableQuery query) throws SQLException {
        AirtableQuery.Join join = query.getJoin()
                .orElseThrow(() -> new SQLException("Join não definido para a consulta."));
        List<String> fields = query.getRequiredFieldsForJoinTable();
        if (fields.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, String> fieldTypes = getFieldTypes(join.getTableName());
        return executeSelect(
                join.getTableName(),
                fields,
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Optional.empty(),
                fieldTypes
        );
    }

    List<MetaTable> fetchTablesMetadata() throws SQLException {
        HttpURLConnection connection = null;
        String url = buildTablesMetadataUrl();
        try {
            connection = openConnection(url, config.getTimeout());
            int status = connection.getResponseCode();
            String body = readResponseBody(connection, status);

            if (status >= 200 && status < 300) {
                MetaTablesResponse response = parseMetaTables(body);
                if (response.tables == null) {
                    return Collections.emptyList();
                }
                return response.tables;
            }

            LOGGER.log(Level.WARNING, "Airtable metadata request failed. Status: {0}, Body: {1}, URL: {2}", new Object[]{status, body, url});
            throw new SQLException("Airtable metadata request failed with status " + status + ": " + body);
        } catch (IOException ex) {
            throw new SQLException("Unable to fetch metadata from Airtable API.", ex);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
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
            Optional<String> view,
            Map<String, String> fieldTypes
    ) throws SQLException {
        List<Map<String, Object>> allRecords = new ArrayList<>();
        String offset = null;

        do {
            HttpURLConnection connection = null;
            String url = null;
            try {
                url = buildSelectUrl(tableName, selectedFields, filterFormula, maxRecords, sorts, view, Optional.ofNullable(offset));
                connection = openConnection(url, config.getTimeout());
                int status = connection.getResponseCode();
                String body = readResponseBody(connection, status);

                if (status >= 200 && status < 300) {
                    RecordsResponse response = parseRecordsResponse(body);
                    for (Map<String, Object> row : response.records) {
                        allRecords.add(applyFieldTypeConversions(row, fieldTypes));
                    }
                    offset = response.offset;
                } else {
                    LOGGER.log(Level.WARNING, "Airtable API request failed. Status: {0}, Body: {1}, URL: {2}", new Object[]{status, body, url});
                    throw new SQLException("Airtable API request failed with status " + status + ": " + body);
                }
            } catch (IOException ex) {
                throw new SQLException("Unable to execute request against Airtable API.", ex);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        } while (offset != null && !offset.isEmpty());

        return allRecords;
    }

    private String buildSelectUrl(
            String tableName,
            List<String> selectedFields,
            Optional<String> filterFormula,
            Optional<Integer> maxRecords,
            List<AirtableQuery.Sort> sorts,
            Optional<String> view,
            Optional<String> offset
    ) throws UnsupportedEncodingException {
        StringBuilder url = new StringBuilder();
        String baseUrl = config.getApiBaseUrl().replaceAll("/+$", "");
        url.append(baseUrl).append('/');
        url.append(config.getBaseId()).append('/');
        url.append(encodePathSegment(tableName));

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
                        key -> new ArrayList<>()).add(sort.getDirection().toString().toLowerCase(Locale.ENGLISH));
            }
        }

        if (!params.isEmpty()) {
            url.append('?').append(encodeQueryParameters(params));
        }

        if (offset.isPresent()) {
            url.append(params.isEmpty() ? '?' : '&')
                    .append("offset=")
                    .append(encodeComponent(offset.get()));
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

    private static String encodePathSegment(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, StandardCharsets.UTF_8.name()).replace("+", "%20");
    }

    private String buildTablesMetadataUrl() {
        String baseUrl = config.getApiBaseUrl().replaceAll("/+$", "");
        return baseUrl + "/meta/bases/" + config.getBaseId() + "/tables";
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

    private RecordsResponse parseRecordsResponse(String body) throws IOException {
        Object json = SimpleJsonParser.parse(body);
        RecordsResponse response = new RecordsResponse();
        if (!(json instanceof Map)) {
            response.records = Collections.emptyList();
            response.offset = null;
            return response;
        }
        Map<?, ?> map = (Map<?, ?>) json;
        Object recordsObj = map.get("records");
        if (!(recordsObj instanceof List)) {
            response.records = Collections.emptyList();
        } else {
            List<Map<String, Object>> records = new ArrayList<>();
            for (Object recordObj : (List<?>) recordsObj) {
                if (!(recordObj instanceof Map)) {
                    continue;
                }
                Map<?, ?> recordMap = (Map<?, ?>) recordObj;
                Object fieldsObj = recordMap.get("fields");
                if (!(fieldsObj instanceof Map)) {
                    records.add(Collections.<String, Object>emptyMap());
                    continue;
                }
                Map<String, Object> row = new LinkedHashMap<>();
            Object idObj = recordMap.get("id");
            if (idObj != null) {
                row.put("id", stringValue(idObj));
            }
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) fieldsObj).entrySet()) {
                row.put(String.valueOf(entry.getKey()), normaliseFieldValue(entry.getValue()));
            }
            records.add(row);
        }
        response.records = records;
        }

        Object offsetObj = map.get("offset");
        response.offset = offsetObj != null ? String.valueOf(offsetObj) : null;
        return response;
    }

    Map<String, String> getFieldTypes(String tableName) throws SQLException {
        if (tableName == null || tableName.isEmpty()) {
            return Collections.emptyMap();
        }
        String normalizedTableName = tableName.toLowerCase(Locale.ENGLISH);
        synchronized (fieldTypesCacheLock) {
            Map<String, String> cached = fieldTypesCache.get(normalizedTableName);
            if (cached != null) {
                return cached;
            }
            // Fetch metadata and only cache the requested table's schema
            List<MetaTable> tables = fetchTablesMetadata();
            Map<String, String> result = null;
            for (MetaTable table : tables) {
                if (table.name == null) {
                    continue;
                }
                if (table.name.equalsIgnoreCase(tableName)) {
                    Map<String, String> types = new HashMap<>();
                    if (table.fields != null) {
                        for (MetaField field : table.fields) {
                            if (field.name != null && field.type != null) {
                                types.put(field.name.toLowerCase(Locale.ENGLISH), field.type.toLowerCase(Locale.ENGLISH));
                            }
                        }
                    }
                    result = Collections.unmodifiableMap(types);
                    fieldTypesCache.put(normalizedTableName, result);
                    break;
                }
            }
            if (result == null) {
                result = Collections.emptyMap();
                fieldTypesCache.put(normalizedTableName, result);
            }
            return result;
        }
    }

    private Map<String, Object> applyFieldTypeConversions(Map<String, Object> row, Map<String, String> fieldTypes) {
        if (row == null || row.isEmpty() || fieldTypes.isEmpty()) {
            return row;
        }
        Map<String, Object> converted = null;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String fieldName = entry.getKey();
            String type = fieldTypes.get(fieldName.toLowerCase(Locale.ENGLISH));
            if (type == null) {
                continue;
            }
            Object newValue = convertFieldValue(type, entry.getValue());
            if (!Objects.equals(newValue, entry.getValue())) {
                if (converted == null) {
                    converted = new LinkedHashMap<>(row);
                }
                converted.put(fieldName, newValue);
            }
        }
        return converted != null ? converted : row;
    }

    private Object convertFieldValue(String airtableType, Object value) {
        if (value == null) {
            return null;
        }
        switch (airtableType) {
            case "date":
            case "datewithtimezone":
                if (value instanceof String) {
                    try {
                        return java.sql.Date.valueOf((String) value);
                    } catch (IllegalArgumentException ex) {
                        LOGGER.log(Level.FINE, "Unable to convert Airtable date value \"{0}\": {1}", new Object[]{value, ex.getMessage()});
                    }
                }
                return value;
            default:
                // Normalize Lookup fields to a user-friendly string (comma-separated), instead of [a, b]
                if (airtableType != null && airtableType.contains("lookup") && value instanceof List) {
                    return joinListValues((List<?>) value);
                }
                return value;
        }
    }

    private static String joinListValues(List<?> list) {
        if (list == null || list.isEmpty()) {
            return "";
        }
        List<String> parts = new ArrayList<>(list.size());
        for (Object element : list) {
            if (element == null) {
                continue;
            }
            if (element instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) element;
                Object preferred = map.get("name");
                if (preferred == null) preferred = map.get("value");
                if (preferred == null) preferred = map.get("email");
                if (preferred == null) preferred = map.get("id");
                if (preferred == null && !map.isEmpty()) {
                    preferred = map.values().iterator().next();
                }
                parts.add(preferred != null ? String.valueOf(preferred) : "");
            } else if (element instanceof List) {
                parts.add(joinListValues((List<?>) element));
            } else {
                parts.add(String.valueOf(element));
            }
        }
        return String.join(", ", parts);
    }

    private MetaTablesResponse parseMetaTables(String body) throws IOException {
        Object json = SimpleJsonParser.parse(body);
        MetaTablesResponse response = new MetaTablesResponse();
        if (!(json instanceof Map)) {
            response.tables = Collections.emptyList();
            return response;
        }
        Object tablesObj = ((Map<?, ?>) json).get("tables");
        if (!(tablesObj instanceof List)) {
            response.tables = Collections.emptyList();
            return response;
        }

        List<MetaTable> tables = new ArrayList<>();
        for (Object tableObj : (List<?>) tablesObj) {
            if (!(tableObj instanceof Map)) {
                continue;
            }
            Map<?, ?> tableMap = (Map<?, ?>) tableObj;
            MetaTable table = new MetaTable();
            table.id = stringValue(tableMap.get("id"));
            table.name = stringValue(tableMap.get("name"));

            Object fieldsObj = tableMap.get("fields");
            if (fieldsObj instanceof List) {
                List<MetaField> fields = new ArrayList<>();
                for (Object fieldObj : (List<?>) fieldsObj) {
                    if (!(fieldObj instanceof Map)) {
                        continue;
                    }
                    Map<?, ?> fieldMap = (Map<?, ?>) fieldObj;
                    MetaField field = new MetaField();
                    field.id = stringValue(fieldMap.get("id"));
                    field.name = stringValue(fieldMap.get("name"));
                    field.type = stringValue(fieldMap.get("type"));
                    fields.add(field);
                }
                table.fields = fields;
            }
            tables.add(table);
        }
        response.tables = tables;
        return response;
    }

    private static String stringValue(Object value) {
        return value != null ? String.valueOf(value) : null;
    }

    private static Object normaliseFieldValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Object email = map.get("email");
            if (email != null) {
                return String.valueOf(email);
            }
            return value;
        }

        if (value instanceof List) {
            List<?> list = (List<?>) value;
            if (list.isEmpty()) {
                return Collections.emptyList();
            }
            boolean allEmailMaps = true;
            List<String> emails = new ArrayList<>();
            for (Object element : list) {
                if (element instanceof Map) {
                    Object email = ((Map<?, ?>) element).get("email");
                    if (email != null) {
                        emails.add(String.valueOf(email));
                        continue;
                    }
                }
                allEmailMaps = false;
                break;
            }
            if (allEmailMaps && !emails.isEmpty()) {
                return String.join(", ", emails);
            }
        }

        return value;
    }

    static final class MetaTablesResponse {
        public List<MetaTable> tables;
    }

    static final class MetaTable {
        public String id;
        public String name;
        public List<MetaField> fields;
    }

    static final class MetaField {
        public String id;
        public String name;
        public String type;
    }

    static final class RecordsResponse {
        List<Map<String, Object>> records = Collections.emptyList();
        String offset;
    }
}
