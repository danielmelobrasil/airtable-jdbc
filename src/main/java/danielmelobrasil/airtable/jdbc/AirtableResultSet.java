package danielmelobrasil.airtable.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.RowId;
import java.sql.Statement;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Forward-only result set backed by a materialised list of Airtable records.
 */
class AirtableResultSet implements ResultSet {

    private final AirtableStatement statement;
    private final List<Map<String, Object>> records;
    private final List<String> columnOrder;
    private int cursor = -1;
    private boolean closed = false;
    private final ResultSetMetaData metaData;

    AirtableResultSet(AirtableStatement statement,
                      List<Map<String, Object>> records,
                      List<String> requestedColumns) {
        this.statement = Objects.requireNonNull(statement, "statement");
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
        this.columnOrder = determineColumnOrder(records, requestedColumns);
        this.metaData = new AirtableResultSetMetaData(columnOrder);
    }

    private static List<String> determineColumnOrder(List<Map<String, Object>> records, List<String> requestedColumns) {
        if (!requestedColumns.isEmpty()) {
            return new ArrayList<>(requestedColumns);
        }
        if (records.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, Object> first = records.get(0);
        return new ArrayList<>(first.keySet());
    }

    @Override
    public boolean next() throws SQLException {
        ensureOpen();
        if (cursor + 1 >= records.size()) {
            cursor = records.size();
            return false;
        }
        cursor++;
        return true;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean wasNull() {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        return value != null ? String.valueOf(value) : null;
        }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        throw new SQLException("Cannot convert value to boolean: " + value);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        throw new SQLException("Cannot convert value to byte: " + value);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        throw new SQLException("Cannot convert value to short: " + value);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new SQLException("Cannot convert value to int: " + value);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new SQLException("Cannot convert value to long: " + value);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        throw new SQLException("Cannot convert value to float: " + value);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        throw new SQLException("Cannot convert value to double: " + value);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValue(columnIndex);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object value = getValue(columnIndex);
        return convertValue(value, type);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getValue(columnLabel);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        Object value = getValue(columnLabel);
        return convertValue(value, type);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        return value != null ? String.valueOf(value) : null;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public java.math.BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof java.math.BigDecimal) {
            return (java.math.BigDecimal) value;
        }
        if (value instanceof java.math.BigInteger) {
            return new java.math.BigDecimal((java.math.BigInteger) value);
        }
        if (value instanceof Number) {
            return java.math.BigDecimal.valueOf(((Number) value).doubleValue());
        }
        if (value instanceof String) {
            try {
                return new java.math.BigDecimal((String) value);
            } catch (NumberFormatException ex) {
                throw new SQLException("Cannot convert value to BigDecimal: " + value, ex);
            }
        }
        throw new SQLException("Cannot convert value to BigDecimal: " + value);
    }

    @Override
    public java.math.BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public java.math.BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        java.math.BigDecimal decimal = getBigDecimal(columnIndex);
        if (decimal == null) {
            return null;
        }
        return decimal.setScale(scale, java.math.RoundingMode.HALF_UP);
    }

    @Override
    @Deprecated
    public java.math.BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        java.math.BigDecimal decimal = getBigDecimal(columnLabel);
        if (decimal == null) {
            return null;
        }
        return decimal.setScale(scale, java.math.RoundingMode.HALF_UP);
    }

    @Override
    public java.io.Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public java.io.Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public java.io.InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    public java.io.InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    @Deprecated
    public java.io.InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode streams are not supported.");
    }

    @Override
    @Deprecated
    public java.io.InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode streams are not supported.");
    }

    @Override
    public java.io.InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII streams are not supported.");
    }

    @Override
    public java.io.InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII streams are not supported.");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        }
        throw new SQLException("Cannot convert value to byte[]: " + value);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metaData;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < columnOrder.size(); i++) {
            if (columnOrder.get(i).equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
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
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cursor names are not supported.");
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        ensureOpen();
        return cursor < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        ensureOpen();
        return cursor >= records.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        ensureOpen();
        return cursor == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        ensureOpen();
        return cursor == records.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Scrolling is not supported.");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Scrolling is not supported.");
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("Scrolling is not supported.");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("Scrolling is not supported.");
    }

    @Override
    public int getRow() throws SQLException {
        ensureOpen();
        return cursor + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("Scrolling is not supported.");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Scrolling is not supported.");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("Scrolling is not supported.");
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
    public void setFetchSize(int rows) throws SQLException {
        // ignored
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBigDecimal(int columnIndex, java.math.BigDecimal x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateTime(int columnIndex, java.sql.Time x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x, int length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBigDecimal(String columnLabel, java.math.BigDecimal x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateDate(String columnLabel, java.sql.Date x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateTime(String columnLabel, java.sql.Time x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateTimestamp(String columnLabel, java.sql.Timestamp x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x, int length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x, int length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader reader, int length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void insertRow() throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateRow() throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Refresh is not supported.");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw unmodifiable();
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, java.util.Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type maps are not supported.");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref types are not supported.");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob types are not supported.");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob types are not supported.");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array types are not supported.");
    }

    @Override
    public Object getObject(String columnLabel, java.util.Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type maps are not supported.");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref types are not supported.");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob types are not supported.");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob types are not supported.");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array types are not supported.");
    }

    @Override
    public java.sql.Date getDate(int columnIndex, java.util.Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calendar conversions are not supported.");
    }

    @Override
    public java.sql.Date getDate(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof java.sql.Date) {
            return (java.sql.Date) value;
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Date(((java.util.Date) value).getTime());
        }
        throw new SQLException("Cannot convert value to java.sql.Date: " + value);
    }

    @Override
    public java.sql.Time getTime(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof java.sql.Time) {
            return (java.sql.Time) value;
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Time(((java.util.Date) value).getTime());
        }
        throw new SQLException("Cannot convert value to java.sql.Time: " + value);
    }

    @Override
    public java.sql.Time getTime(int columnIndex, java.util.Calendar cal) throws SQLException {
        java.sql.Time time = getTime(columnIndex);
        if (time == null || cal == null) {
            return time;
        }
        long millis = time.getTime();
        int targetOffset = cal.getTimeZone().getOffset(millis);
        int defaultOffset = java.util.TimeZone.getDefault().getOffset(millis);
        if (targetOffset == defaultOffset) {
            return time;
        }
        return new java.sql.Time(millis - (defaultOffset - targetOffset));
    }

    @Override
    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value instanceof java.sql.Timestamp) {
            return (java.sql.Timestamp) value;
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Timestamp(((java.util.Date) value).getTime());
        }
        throw new SQLException("Cannot convert value to java.sql.Timestamp: " + value);
    }

    @Override
    public java.sql.Timestamp getTimestamp(int columnIndex, java.util.Calendar cal) throws SQLException {
        java.sql.Timestamp timestamp = getTimestamp(columnIndex);
        if (timestamp == null || cal == null) {
            return timestamp;
        }
        long time = timestamp.getTime();
        int targetOffset = cal.getTimeZone().getOffset(time);
        int defaultOffset = java.util.TimeZone.getDefault().getOffset(time);
        if (targetOffset == defaultOffset) {
            return timestamp;
        }
        return new java.sql.Timestamp(time - (defaultOffset - targetOffset));
    }

    @Override
    public java.sql.Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public java.sql.Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public java.sql.Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public java.sql.Date getDate(String columnLabel, java.util.Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calendar conversions are not supported.");
    }

    @Override
    public java.sql.Time getTime(String columnLabel, java.util.Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public java.sql.Timestamp getTimestamp(String columnLabel, java.util.Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public java.net.URL getURL(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        if (value == null) {
            return null;
        }
        try {
            return new java.net.URL(value.toString());
        } catch (java.net.MalformedURLException ex) {
            throw new SQLException("Cannot convert value to URL: " + value, ex);
        }
    }

    @Override
    public java.net.URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId is not supported.");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId is not supported.");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public int getHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob is not supported.");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob is not supported.");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported.");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported.");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex);
        return value != null ? value.toString() : null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public java.io.Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public java.io.Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, java.io.Reader x, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBlob(int columnIndex, java.io.InputStream inputStream, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateClob(int columnIndex, java.io.Reader reader, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateClob(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNClob(int columnIndex, java.io.Reader reader, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNClob(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, java.io.Reader x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, java.io.Reader reader) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader reader) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBlob(int columnIndex, java.io.InputStream inputStream) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateClob(int columnIndex, java.io.Reader reader) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateClob(String columnLabel, java.io.Reader reader) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNClob(int columnIndex, java.io.Reader reader) throws SQLException {
        throw unmodifiable();
    }

    @Override
    public void updateNClob(String columnLabel, java.io.Reader reader) throws SQLException {
        throw unmodifiable();
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
            throw new SQLException("Result set is closed.");
        }
    }

    private SQLException unmodifiable() {
        return new SQLFeatureNotSupportedException("Result sets are read-only.");
    }

    private Object getValue(int columnIndex) throws SQLException {
        ensureOpen();
        if (columnIndex < 1 || columnIndex > columnOrder.size()) {
            throw new SQLException("Column index out of bounds: " + columnIndex);
        }
        if (cursor < 0 || cursor >= records.size()) {
            throw new SQLException("Cursor is not positioned on a row.");
        }
        Map<String, Object> row = records.get(cursor);
        String column = columnOrder.get(columnIndex - 1);
        return row.get(column);
    }

    private Object getValue(String columnLabel) throws SQLException {
        return getValue(findColumn(columnLabel));
    }

    private <T> T convertValue(Object value, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("Target type cannot be null.");
        }
        if (value == null) {
            return null;
        }
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        if (type == String.class) {
            return type.cast(String.valueOf(value));
        }
        if (Number.class.isAssignableFrom(type) && value instanceof Number) {
            Number number = (Number) value;
            if (type == Integer.class) {
                return type.cast(number.intValue());
            }
            if (type == Long.class) {
                return type.cast(number.longValue());
            }
            if (type == Double.class) {
                return type.cast(number.doubleValue());
            }
            if (type == Float.class) {
                return type.cast(number.floatValue());
            }
            if (type == Short.class) {
                return type.cast(number.shortValue());
            }
            if (type == Byte.class) {
                return type.cast(number.byteValue());
            }
        }
        if (type == Boolean.class) {
            if (value instanceof Boolean) {
                return type.cast(value);
            }
            if (value instanceof Number) {
                return type.cast(((Number) value).intValue() != 0);
            }
            if (value instanceof String) {
                return type.cast(Boolean.parseBoolean((String) value));
            }
        }
        throw new SQLException("Cannot convert value of type " + value.getClass() + " to " + type.getName());
    }
}
