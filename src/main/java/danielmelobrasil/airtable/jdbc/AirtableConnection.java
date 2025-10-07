package danielmelobrasil.airtable.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Basic {@link Connection} implementation backed by the Airtable REST API.
 */
class AirtableConnection implements Connection {

    private final AirtableConfig config;
    private final AirtableApiClient apiClient;
    private volatile boolean closed = false;

    AirtableConnection(AirtableConfig config, AirtableApiClient apiClient) {
        this.config = Objects.requireNonNull(config, "config");
        this.apiClient = Objects.requireNonNull(apiClient, "apiClient");
    }

    AirtableApiClient getApiClient() {
        return apiClient;
    }

    AirtableConfig getConfig() {
        return config;
    }

    @Override
    public Statement createStatement() throws SQLException {
        ensureOpen();
        return new AirtableStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Prepared statements are not supported.");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported.");
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (!autoCommit) {
            throw new SQLFeatureNotSupportedException("Transactions are not supported.");
        }
    }

    @Override
    public boolean getAutoCommit() {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        ensureOpen();
        return new AirtableDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (!readOnly) {
            throw new SQLFeatureNotSupportedException("Writable connections are not supported.");
        }
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLFeatureNotSupportedException("Catalogs are not supported.");
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException("Transactions are not supported.");
        }
    }

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // nothing to clear
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Only forward-only, read-only result sets are supported.");
        }
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Prepared statements are not supported.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported.");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type maps are not supported.");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("Unsupported holdability: " + holdability);
        }
    }

    @Override
    public int getHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Prepared statements are not supported.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Prepared statements are not supported.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Prepared statements are not supported.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Prepared statements are not supported.");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("LOBs are not supported.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("LOBs are not supported.");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("LOBs are not supported.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported.");
    }

    @Override
    public boolean isValid(int timeout) {
        return !closed;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public String getClientInfo(String name) {
        return null;
    }

    @Override
    public Properties getClientInfo() {
        Properties properties = new Properties();
        properties.putAll(config.toPropertiesMap());
        return properties;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Arrays are not supported.");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Structs are not supported.");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new SQLFeatureNotSupportedException("Schemas are not supported.");
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Timeout configuration per connection is not supported.");
    }

    @Override
    public int getNetworkTimeout() {
        return (int) config.getTimeout().toMillis();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLFeatureNotSupportedException("Cannot unwrap to " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Connection already closed.");
        }
    }
}
