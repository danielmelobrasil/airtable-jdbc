package danielmelobrasil.airtable.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Minimal {@link ResultSetMetaData} implementation.
 */
class AirtableResultSetMetaData implements ResultSetMetaData {

    private final List<String> columns;
    private final Map<String, ColumnTypeInfo> columnTypes;

    AirtableResultSetMetaData(List<String> columns) {
        this(columns, Collections.emptyMap());
    }

    AirtableResultSetMetaData(List<String> columns, Map<String, ColumnTypeInfo> columnTypes) {
        this.columns = Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(columnTypes, "columnTypes");
        this.columnTypes = Collections.unmodifiableMap(new LinkedHashMap<>(columnTypes));
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
    public int getColumnType(int column) throws SQLException {
        ColumnTypeInfo info = columnTypeFor(column);
        return info != null ? info.jdbcType : Types.JAVA_OBJECT;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        ColumnTypeInfo info = columnTypeFor(column);
        return info != null ? info.typeName : "OBJECT";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        ColumnTypeInfo info = columnTypeFor(column);
        return info != null ? info.getPrecision() : 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        ColumnTypeInfo info = columnTypeFor(column);
        return info != null ? info.getScale() : 0;
    }

    @Override
    public int isNullable(int column) {
        return columnNullable;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        ColumnTypeInfo info = columnTypeFor(column);
        return info != null ? info.caseSensitive : true;
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
    public boolean isSigned(int column) throws SQLException {
        ColumnTypeInfo info = columnTypeFor(column);
        return info != null ? info.signed : false;
    }

    @Override
    public String getSchemaName(int column) {
        return "";
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        ColumnTypeInfo info = columnTypeFor(column);
        return info != null ? info.getDisplaySize() : 0;
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
    public String getColumnClassName(int column) throws SQLException {
        ColumnTypeInfo info = columnTypeFor(column);
        return info != null ? info.className : Object.class.getName();
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

    private ColumnTypeInfo columnTypeFor(int column) throws SQLException {
        String columnName = getColumnName(column);
        return columnTypes.get(columnName);
    }

    static final class ColumnTypeInfo {
        private final int jdbcType;
        private final String typeName;
        private final Integer precision;
        private final Integer scale;
        private final Integer radix;
        private final Integer charOctetLength;
        private final boolean signed;
        private final boolean caseSensitive;
        private final String className;

        private ColumnTypeInfo(int jdbcType,
                               String typeName,
                               Integer precision,
                               Integer scale,
                               Integer radix,
                               Integer charOctetLength,
                               boolean signed,
                               boolean caseSensitive,
                               String className) {
            this.jdbcType = jdbcType;
            this.typeName = typeName;
            this.precision = precision;
            this.scale = scale;
            this.radix = radix;
            this.charOctetLength = charOctetLength;
            this.signed = signed;
            this.caseSensitive = caseSensitive;
            this.className = className;
        }

        static ColumnTypeInfo fromSqlTypeInfo(AirtableDatabaseMetaData.SqlTypeInfo typeInfo) {
            int jdbcType = typeInfo.jdbcType;
            return new ColumnTypeInfo(
                    jdbcType,
                    typeInfo.typeName,
                    typeInfo.columnSize,
                    typeInfo.decimalDigits,
                    typeInfo.radix,
                    typeInfo.charOctetLength,
                    isSignedType(jdbcType),
                    isCaseSensitiveType(jdbcType),
                    resolveClassName(jdbcType)
            );
        }

        static ColumnTypeInfo forRecordId() {
            int size = 24;
            return new ColumnTypeInfo(
                    Types.VARCHAR,
                    "VARCHAR",
                    size,
                    null,
                    null,
                    size,
                    false,
                    true,
                    String.class.getName()
            );
        }

        private static boolean isSignedType(int jdbcType) {
            return jdbcType == Types.DOUBLE
                    || jdbcType == Types.FLOAT
                    || jdbcType == Types.REAL
                    || jdbcType == Types.DECIMAL
                    || jdbcType == Types.NUMERIC
                    || jdbcType == Types.BIGINT
                    || jdbcType == Types.INTEGER
                    || jdbcType == Types.SMALLINT
                    || jdbcType == Types.TINYINT;
        }

        private static boolean isCaseSensitiveType(int jdbcType) {
            return jdbcType == Types.VARCHAR
                    || jdbcType == Types.CHAR
                    || jdbcType == Types.LONGVARCHAR
                    || jdbcType == Types.CLOB;
        }

        private static String resolveClassName(int jdbcType) {
            switch (jdbcType) {
                case Types.BOOLEAN:
                case Types.BIT:
                    return Boolean.class.getName();
                case Types.BIGINT:
                    return Long.class.getName();
                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.TINYINT:
                    return Integer.class.getName();
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.REAL:
                    return Double.class.getName();
                case Types.DECIMAL:
                case Types.NUMERIC:
                    return java.math.BigDecimal.class.getName();
                case Types.DATE:
                    return java.sql.Date.class.getName();
                case Types.TIMESTAMP:
                    return java.sql.Timestamp.class.getName();
                case Types.TIME:
                    return java.sql.Time.class.getName();
                case Types.VARCHAR:
                case Types.CHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                    return String.class.getName();
                default:
                    return Object.class.getName();
            }
        }

        int getPrecision() {
            return precision != null ? precision : 0;
        }

        int getScale() {
            return scale != null ? scale : 0;
        }

        int getDisplaySize() {
            if (precision != null) {
                return precision;
            }
            if (charOctetLength != null) {
                return charOctetLength;
            }
            return 0;
        }
    }
}
