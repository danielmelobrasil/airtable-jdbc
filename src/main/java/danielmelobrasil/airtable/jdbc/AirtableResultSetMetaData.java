package danielmelobrasil.airtable.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;
import java.util.Objects;

/**
 * Minimal {@link ResultSetMetaData} implementation.
 */
class AirtableResultSetMetaData implements ResultSetMetaData {

    private final List<String> columns;

    AirtableResultSetMetaData(List<String> columns) {
        this.columns = Objects.requireNonNull(columns, "columns");
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        if (column < 1 || column > columns.size()) {
            throw new SQLException("Column index out of bounds: " + column);
        }
        return columns.get(column - 1);
    }

    @Override
    public int getColumnType(int column) {
        return Types.JAVA_OBJECT;
    }

    @Override
    public String getColumnTypeName(int column) {
        return "OBJECT";
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public int isNullable(int column) {
        return columnNoNulls;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    @Override
    public boolean isSearchable(int column) {
        return true;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public boolean isSigned(int column) {
        return true;
    }

    @Override
    public String getSchemaName(int column) {
        return "";
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 0;
    }

    @Override
    public String getTableName(int column) {
        return "";
    }

    @Override
    public String getCatalogName(int column) {
        return "";
    }

    @Override
    public boolean isReadOnly(int column) {
        return true;
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) {
        return Object.class.getName();
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
}
