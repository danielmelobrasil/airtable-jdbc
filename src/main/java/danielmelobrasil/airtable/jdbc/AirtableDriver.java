package danielmelobrasil.airtable.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver implementation for Airtable.
 */
public final class AirtableDriver implements Driver {

    static final String URL_PREFIX = "jdbc:airtable:";
    private static final Logger LOGGER = Logger.getLogger(AirtableDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new AirtableDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        AirtableConfig config = AirtableConfig.from(url, info);
        AirtableApiClient apiClient = new AirtableApiClient(config);

        return new AirtableConnection(config, apiClient);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        AirtableConfig config = null;
        try {
            if (url != null) {
                config = AirtableConfig.from(url, info);
            }
        } catch (SQLException ex) {
            LOGGER.fine(() -> "Unable to fully parse configuration for property info: " + ex.getMessage());
        }

        DriverPropertyInfo apiKey = new DriverPropertyInfo("apiKey", config != null ? config.getApiKey() : null);
        apiKey.required = true;
        DriverPropertyInfo baseId = new DriverPropertyInfo("baseId", config != null ? config.getBaseId() : null);
        baseId.required = true;
        DriverPropertyInfo apiUrl = new DriverPropertyInfo("apiBaseUrl", config != null ? config.getApiBaseUrl() : null);
        apiUrl.required = false;
        return new DriverPropertyInfo[]{apiKey, baseId, apiUrl};
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Logging hierarchy is not supported.");
    }

    /**
     * For explicit registration in environments that do not use service loaders.
     *
     * @throws SQLException if registration fails
     */
    public static void register() throws SQLException {
        LOGGER.fine("Registering Airtable JDBC Driver");
        DriverManager.registerDriver(new AirtableDriver());
    }
}
