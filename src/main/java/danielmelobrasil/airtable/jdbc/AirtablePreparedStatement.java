package danielmelobrasil.airtable.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class AirtablePreparedStatement extends AirtableStatement implements PreparedStatement {

    private final String sql;
    private final List<Object> parameters;

    AirtablePreparedStatement(AirtableConnection connection, String sql) throws SQLException {
        super(connection);
        this.sql = Objects.requireNonNull(sql, "sql");
        int paramCount = countPlaceholders(sql);
        this.parameters = new ArrayList<>(paramCount);
        for (int i = 0; i < paramCount; i++) {
            parameters.add(null);
        }
    }

    private static int countPlaceholders(String sql) {
        int count = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            } else if (ch == '?' && !inSingleQuote && !inDoubleQuote) {
                count++;
            }
        }
        return count;
    }

    private void setParameter(int parameterIndex, Object value) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > parameters.size()) {
            throw new SQLException("Parameter index out of bounds: " + parameterIndex);
        }
        parameters.set(parameterIndex - 1, value);
    }

    private void ensureAllParametersBound() throws SQLException {
        for (int i = 0; i < parameters.size(); i++) {
            if (parameters.get(i) == null) {
                throw new SQLException("Parameter " + (i + 1) + " is not set.");
            }
        }
    }

    private String buildSqlWithParameters() throws SQLException {
        ensureAllParametersBound();
        StringBuilder result = new StringBuilder();
        int paramIndex = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                result.append(ch);
                continue;
            }
            if (ch == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                result.append(ch);
                continue;
            }
            if (ch == '?' && !inSingleQuote && !inDoubleQuote) {
                Object value = parameters.get(paramIndex++);
                result.append(serializeParameter(value));
            } else {
                result.append(ch);
            }
        }
        return result.toString();
    }

    private String serializeParameter(Object value) throws SQLException {
        if (value == null) {
            throw new SQLException("Null parameters are not supported.");
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        String text = value.toString();
        return "'" + text.replace("'", "''") + "'";
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        String expandedSql = buildSqlWithParameters();
        return super.executeQuery(expandedSql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("UPDATE statements are not supported.");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLException("Null parameters are not supported.");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, (int) x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, (int) x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, (double) x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParameter(parameterIndex, x != null ? new String(x, StandardCharsets.UTF_8) : null);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x == null) {
            throw new SQLException("Null parameters are not supported.");
        }
        setParameter(parameterIndex, x.toString());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x == null) {
            throw new SQLException("Null parameters are not supported.");
        }
        setParameter(parameterIndex, x.toString());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x == null) {
            throw new SQLException("Null parameters are not supported.");
        }
        setParameter(parameterIndex, x.toString());
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void clearParameters() {
        for (int i = 0; i < parameters.size(); i++) {
            parameters.set(i, null);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        executeQuery();
        return true;
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch operations are not supported.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public void setRef(int parameterIndex, java.sql.Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref types are not supported.");
    }

    @Override
    public void setBlob(int parameterIndex, java.sql.Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob types are not supported.");
    }

    @Override
    public void setClob(int parameterIndex, java.sql.Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob types are not supported.");
    }

    @Override
    public void setArray(int parameterIndex, java.sql.Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array types are not supported.");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Prepared statement metadata is not supported.");
    }

    @Override
    public void setDate(int parameterIndex, Date x, java.util.Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, java.util.Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, java.util.Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLException("Null parameters are not supported.");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setParameter(parameterIndex, x != null ? x.toString() : null);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameter metadata is not supported.");
    }

    @Override
    public void setRowId(int parameterIndex, java.sql.RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId is not supported.");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public void setNClob(int parameterIndex, java.sql.NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob types are not supported.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported.");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode streams are not supported.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader)	throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported.");
    }
}
