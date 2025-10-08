package danielmelobrasil.airtable.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Encapsulates configuration information required to communicate with Airtable.
 */
final class AirtableConfig {

    private static final String DEFAULT_API_BASE_URL = "https://api.airtable.com/v0";
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final String apiKey;
    private final String baseId;
    private final String apiBaseUrl;
    private final Duration timeout;
    private final Optional<String> defaultView;

    private AirtableConfig(
            String apiKey,
            String baseId,
            String apiBaseUrl,
            Duration timeout,
            Optional<String> defaultView
    ) {
        this.apiKey = Objects.requireNonNull(apiKey, "apiKey");
        this.baseId = Objects.requireNonNull(baseId, "baseId");
        this.apiBaseUrl = Objects.requireNonNull(apiBaseUrl, "apiBaseUrl");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.defaultView = Objects.requireNonNull(defaultView, "defaultView");
    }

    static AirtableConfig from(String url, Properties info) throws SQLException {
        Objects.requireNonNull(url, "url");
        if (!url.startsWith(AirtableDriver.URL_PREFIX)) {
            throw new SQLException("Invalid Airtable JDBC URL: " + url);
        }

        String connectionPart = url.substring(AirtableDriver.URL_PREFIX.length());
        URI uri = parseUri(connectionPart);
        Map<String, String> params = mergeParameters(uri.getQuery(), info);

        String apiKey = firstNonEmpty(
                params.get("apiKey"),
                uri.getUserInfo(),
                info != null ? info.getProperty("apiKey") : null
        );
        if (apiKey == null) {
            throw new SQLException("apiKey parameter is required to authenticate with Airtable.");
        }

        String baseId = determineBaseId(uri, params, info);
        if (baseId == null) {
            throw new SQLException("Base ID is required. Provide it in the JDBC URL (jdbc:airtable://<baseId>) or via the 'baseId' property.");
        }

        String apiBaseUrl = firstNonEmpty(
                params.get("apiBaseUrl"),
                info != null ? info.getProperty("apiBaseUrl") : null,
                DEFAULT_API_BASE_URL
        );

        Duration timeout = parseTimeout(params, info);
        Optional<String> defaultView = Optional.ofNullable(firstNonEmpty(
                params.get("view"),
                info != null ? info.getProperty("view") : null
        ));

        return new AirtableConfig(apiKey, baseId, apiBaseUrl, timeout, defaultView);
    }

    private static URI parseUri(String connectionPart) throws SQLException {
        String candidate = connectionPart;
        if (candidate.startsWith("//")) {
            candidate = "airtable:" + candidate;
        } else if (!candidate.startsWith("airtable://")) {
            candidate = "airtable://" + candidate;
        }
        try {
            return new URI(candidate);
        } catch (URISyntaxException ex) {
            throw new SQLException("Malformed Airtable JDBC URL segment: " + connectionPart, ex);
        }
    }

    private static Map<String, String> mergeParameters(String query, Properties info) {
        Map<String, String> result = new HashMap<>();
        if (info != null) {
            for (String name : info.stringPropertyNames()) {
                result.put(name, info.getProperty(name));
            }
        }

        if (query == null || query.isEmpty()) {
            return result;
        }

        for (String pair : query.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            String[] parts = pair.split("=", 2);
            String key = urlDecode(parts[0]);
            String value = parts.length > 1 ? urlDecode(parts[1]) : "";
            result.put(key, value);
        }
        return result;
    }

    private static String urlDecode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static Duration parseTimeout(Map<String, String> params, Properties info) throws SQLException {
        String timeoutValue = firstNonEmpty(
                params.get("timeoutSeconds"),
                info != null ? info.getProperty("timeoutSeconds") : null
        );

        if (timeoutValue == null || timeoutValue.isEmpty()) {
            return DEFAULT_TIMEOUT;
        }

        try {
            long seconds = Long.parseLong(timeoutValue);
            if (seconds <= 0) {
                throw new NumberFormatException("timeoutSeconds must be positive.");
            }
            return Duration.ofSeconds(seconds);
        } catch (NumberFormatException ex) {
            throw new SQLException("Invalid timeoutSeconds parameter: " + timeoutValue, ex);
        }
    }

    private static String determineBaseId(URI uri, Map<String, String> params, Properties info) {
        if (uri.getHost() != null && !uri.getHost().isEmpty()) {
            return uri.getHost();
        }
        String path = uri.getPath();
        if (path != null && path.length() > 1) {
            return path.replaceFirst("^/+", "");
        }

        return firstNonEmpty(
                params.get("baseId"),
                info != null ? info.getProperty("baseId") : null
        );
    }

    private static String firstNonEmpty(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    String getApiKey() {
        return apiKey;
    }

    String getBaseId() {
        return baseId;
    }

    String getApiBaseUrl() {
        return apiBaseUrl;
    }

    Duration getTimeout() {
        return timeout;
    }

    Optional<String> getDefaultView() {
        return defaultView;
    }

    Map<String, String> toPropertiesMap() {
        Map<String, String> map = new HashMap<>();
        map.put("apiKey", apiKey);
        map.put("baseId", baseId);
        map.put("apiBaseUrl", apiBaseUrl);
        map.put("timeoutSeconds", String.valueOf(timeout.getSeconds()));
        defaultView.ifPresent(view -> map.put("view", view));
        return Collections.unmodifiableMap(map);
    }
}
