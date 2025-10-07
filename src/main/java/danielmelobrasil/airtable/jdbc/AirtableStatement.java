package danielmelobrasil.airtable.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Forward-only, read-only statement implementation.
 */
class AirtableStatement implements Statement {

    private final AirtableConnection connection;
    private AirtableResultSet currentResultSet;
    private int maxRows = 0;
    private boolean closed = false;

    AirtableStatement(AirtableConnection connection) {
        this.connection = Objects.requireNonNull(connection, "connection");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        closeCurrentResultSet();
        AirtableQuery query = translateSql(sql);
        AirtableQuery effectiveQuery = applyLimit(query);
        List<Map<String, Object>> baseRecords = connection.getApiClient().select(effectiveQuery);
        List<Map<String, Object>> records = mergeJoinIfNeeded(effectiveQuery, baseRecords);
        currentResultSet = new AirtableResultSet(this, records, effectiveQuery.getColumnLabels());
        return currentResultSet;
    }

    private AirtableQuery applyLimit(AirtableQuery query) {
        if (maxRows <= 0) {
            return query;
        }
        if (query.getMaxRecords().isPresent() && query.getMaxRecords().get() <= maxRows) {
            return query;
        }
        return new AirtableQuery(
                query.getTableName(),
                query.getSelectedFields(),
                query.getFilterFormula(),
                Optional.of(maxRows),
                query.getSorts(),
                query.getJoin()
        );
    }

    private static AirtableQuery translateSql(String sql) throws SQLException {
        try {
            return AirtableSqlParser.parse(sql);
        } catch (AirtableSqlParseException ex) {
            throw new SQLException("Failed to translate SQL for Airtable: " + ex.getMessage(), ex);
        }
    }

    private void closeCurrentResultSet() throws SQLException {
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        currentResultSet = null;
    }

    private List<Map<String, Object>> mergeJoinIfNeeded(AirtableQuery query, List<Map<String, Object>> baseRecords) throws SQLException {
        if (!query.getJoin().isPresent()) {
            return baseRecords;
        }

        AirtableQuery.Join join = query.getJoin().get();
        List<Map<String, Object>> joinRecords = connection.getApiClient().selectJoinTable(query);
        Map<Object, List<Map<String, Object>>> joinIndex = new HashMap<>();
        for (Map<String, Object> record : joinRecords) {
            Object key = record.get(join.getRightField());
            joinIndex.computeIfAbsent(key, unused -> new ArrayList<>()).add(record);
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> baseRecord : baseRecords) {
            Object baseKey = baseRecord.get(join.getLeftField());
            List<Map<String, Object>> matches = joinIndex.get(baseKey);
            if (matches == null || matches.isEmpty()) {
                rows.add(buildJoinedRow(query, baseRecord, Optional.empty()));
            } else {
                for (Map<String, Object> joinRecord : matches) {
                    rows.add(buildJoinedRow(query, baseRecord, Optional.of(joinRecord)));
                }
            }
        }
        return rows;
    }

    private Map<String, Object> buildJoinedRow(
            AirtableQuery query,
            Map<String, Object> baseRecord,
            Optional<Map<String, Object>> joinRecord
    ) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (AirtableQuery.SelectedField field : query.getSelectedFields()) {
            Object value;
            if (field.getOrigin() == AirtableQuery.SelectedField.Origin.BASE) {
                value = baseRecord.get(field.getField());
            } else {
                value = joinRecord.map(record -> record.get(field.getField())).orElse(null);
            }
            row.put(field.getLabel(), value);
        }
        return row;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("UPDATE statements are not supported.");
    }

    @Override
    public void close() throws SQLException {
        closeCurrentResultSet();
        closed = true;
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) {
        // ignored
    }

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) {
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        // ignored
    }

    @Override
    public int getQueryTimeout() {
        return (int) connection.getConfig().getTimeout().getSeconds();
    }

    @Override
    public void setQueryTimeout(int seconds) {
        // ignored
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cancel is not supported.");
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
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursors are not supported.");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return true;
    }

    @Override
    public ResultSet getResultSet() {
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() {
        return -1;
    }

    @Override
    public boolean getMoreResults() {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported.");
        }
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {
        // ignored
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch operations are not supported.");
    }

    @Override
    public void clearBatch() {
        // ignored
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch operations are not supported.");
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported.");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (poolable) {
            throw new SQLFeatureNotSupportedException("Statement pooling is not supported.");
        }
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException("closeOnCompletion is not supported.");
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
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
            throw new SQLException("Statement already closed.");
        }
    }
}
