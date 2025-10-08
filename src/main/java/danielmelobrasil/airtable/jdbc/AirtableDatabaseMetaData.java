package danielmelobrasil.airtable.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Minimal {@link DatabaseMetaData} implementation.
 */
class AirtableDatabaseMetaData implements DatabaseMetaData {

    private final AirtableConnection connection;
    private volatile List<AirtableApiClient.MetaTable> tablesCache;

    AirtableDatabaseMetaData(AirtableConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public String getURL() {
        return AirtableDriver.URL_PREFIX + "//" + connection.getConfig().getBaseId();
    }

    @Override
    public String getUserName() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public String getDatabaseProductName() {
        return "Airtable";
    }

    @Override
    public String getDatabaseProductVersion() {
        return "REST";
    }

    @Override
    public String getDriverName() {
        return "Airtable JDBC Driver";
    }

    @Override
    public String getDriverVersion() {
        return "0.1";
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 1;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public String getSQLKeywords() {
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return "";
    }

    @Override
    public String getStringFunctions() {
        return "";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return false;
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() {
        return false;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsUnion() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return true;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 10;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 1;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return true;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Procedures are not supported.");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Procedures are not supported.");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        List<AirtableApiClient.MetaTable> tables = getTablesMetadata();
        String tablePattern = tableNamePattern != null ? tableNamePattern : "%";
        String typeFilter = (types == null || types.length == 0) ? null : types[0];

        List<Map<String, Object>> rows = new ArrayList<>();
        for (AirtableApiClient.MetaTable table : tables) {
            if (!matchesPattern(table.name, tablePattern)) {
                continue;
            }
            if (typeFilter != null && !"TABLE".equalsIgnoreCase(typeFilter)) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("TABLE_CAT", null);
            row.put("TABLE_SCHEM", null);
            row.put("TABLE_NAME", table.name);
            row.put("TABLE_TYPE", "TABLE");
            row.put("REMARKS", "");
            row.put("TYPE_CAT", null);
            row.put("TYPE_SCHEM", null);
            row.put("TYPE_NAME", null);
            row.put("SELF_REFERENCING_COL_NAME", null);
            row.put("REF_GENERATION", null);
            rows.add(row);
        }

        List<String> columns = Arrays.asList(
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "TABLE_TYPE",
                "REMARKS",
                "TYPE_CAT",
                "TYPE_SCHEM",
                "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION"
        );

        return new AirtableResultSet(new AirtableStatement(connection), rows, columns);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        List<Map<String, Object>> rows = Collections.emptyList();
        List<String> columns = Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG");
        return createResultSet(rows, columns);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TABLE_CAT", connection.getConfig().getBaseId());
        List<Map<String, Object>> rows = Collections.singletonList(row);
        List<String> columns = Collections.singletonList("TABLE_CAT");
        return createResultSet(rows, columns);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("TABLE_TYPE", "TABLE");
        List<Map<String, Object>> rows = Collections.singletonList(row);
        List<String> columns = Collections.singletonList("TABLE_TYPE");
        return createResultSet(rows, columns);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        List<AirtableApiClient.MetaTable> tables = getTablesMetadata();
        String tablePattern = tableNamePattern != null ? tableNamePattern : "%";
        String columnPattern = columnNamePattern != null ? columnNamePattern : "%";

        List<Map<String, Object>> rows = new ArrayList<>();
        for (AirtableApiClient.MetaTable table : tables) {
            if (!matchesPattern(table.name, tablePattern)) {
                continue;
            }
            if (table.fields == null) {
                continue;
            }
            int ordinal = 1;
            for (AirtableApiClient.MetaField field : table.fields) {
                if (!matchesPattern(field.name, columnPattern)) {
                    ordinal++;
                    continue;
                }

                SqlTypeInfo typeInfo = mapFieldType(field.type);
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("TABLE_CAT", null);
                row.put("TABLE_SCHEM", null);
                row.put("TABLE_NAME", table.name);
                row.put("COLUMN_NAME", field.name);
                row.put("DATA_TYPE", typeInfo.jdbcType);
                row.put("TYPE_NAME", typeInfo.typeName);
                row.put("COLUMN_SIZE", typeInfo.columnSize);
                row.put("BUFFER_LENGTH", null);
                row.put("DECIMAL_DIGITS", typeInfo.decimalDigits);
                row.put("NUM_PREC_RADIX", typeInfo.radix);
                row.put("NULLABLE", DatabaseMetaData.columnNullable);
                row.put("REMARKS", "");
                row.put("COLUMN_DEF", null);
                row.put("SQL_DATA_TYPE", null);
                row.put("SQL_DATETIME_SUB", null);
                row.put("CHAR_OCTET_LENGTH", typeInfo.charOctetLength);
                row.put("ORDINAL_POSITION", ordinal);
                row.put("IS_NULLABLE", "YES");
                row.put("SCOPE_CATALOG", null);
                row.put("SCOPE_SCHEMA", null);
                row.put("SCOPE_TABLE", null);
                row.put("SOURCE_DATA_TYPE", null);
                row.put("IS_AUTOINCREMENT", "NO");
                row.put("IS_GENERATEDCOLUMN", typeInfo.generated ? "YES" : "NO");
                rows.add(row);
                ordinal++;
            }
        }

        List<String> columns = Arrays.asList(
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "TYPE_NAME",
                "COLUMN_SIZE",
                "BUFFER_LENGTH",
                "DECIMAL_DIGITS",
                "NUM_PREC_RADIX",
                "NULLABLE",
                "REMARKS",
                "COLUMN_DEF",
                "SQL_DATA_TYPE",
                "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION",
                "IS_NULLABLE",
                "SCOPE_CATALOG",
                "SCOPE_SCHEMA",
                "SCOPE_TABLE",
                "SOURCE_DATA_TYPE",
                "IS_AUTOINCREMENT",
                "IS_GENERATEDCOLUMN"
        );

        return new AirtableResultSet(new AirtableStatement(connection), rows, columns);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Privileges are not supported.");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Privileges are not supported.");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Best row identifier is not supported.");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("Version columns are not supported.");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        List<Map<String, Object>> rows = Collections.emptyList();
        List<String> columns = Arrays.asList(
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "COLUMN_NAME",
                "KEY_SEQ",
                "PK_NAME"
        );
        return createResultSet(rows, columns);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("Keys metadata is not supported.");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("Keys metadata is not supported.");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Cross reference metadata is not supported.");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("Type metadata is not supported.");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLFeatureNotSupportedException("Index metadata is not supported.");
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new SQLFeatureNotSupportedException("UDTs are not supported.");
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Super types metadata is not supported.");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Super tables metadata is not supported.");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Attributes metadata is not supported.");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 0;
    }

    @Override
    public int getSQLStateType() {
        return DatabaseMetaData.sqlStateSQL99;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Schemas are not supported.");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return true;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException("Client info properties are not supported.");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Functions metadata is not supported.");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Functions metadata is not supported.");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("Pseudo columns are not supported.");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    private static boolean matchesPattern(String value, String pattern) {
        if (pattern == null) {
            return true;
        }
        if ("%".equals(pattern)) {
            return true;
        }
        if (value == null) {
            return false;
        }
        String regex = "^" + pattern
                .replace("\\", "\\\\")
                .replace(".", "\\.")
                .replace("_", ".")
                .replace("%", ".*") + "$";
        return value.matches(regex);
    }

    private List<AirtableApiClient.MetaTable> getTablesMetadata() throws SQLException {
        List<AirtableApiClient.MetaTable> snapshot = tablesCache;
        if (snapshot != null) {
            return snapshot;
        }
        synchronized (this) {
            if (tablesCache == null) {
                tablesCache = Collections.unmodifiableList(connection.getApiClient().fetchTablesMetadata());
            }
            return tablesCache;
        }
    }

    private static SqlTypeInfo mapFieldType(String airtableType) {
        String type = airtableType != null ? airtableType.toLowerCase(Locale.ENGLISH) : "singlelinetext";
        switch (type) {
            case "number":
            case "percent":
            case "currency":
            case "rating":
            case "duration":
                return SqlTypeInfo.numeric(Types.DOUBLE, "DOUBLE", false);
            case "count":
            case "rollup":
            case "formula":
                return SqlTypeInfo.numeric(Types.DOUBLE, "DOUBLE", true);
            case "checkbox":
                return SqlTypeInfo.simple(Types.BOOLEAN, "BOOLEAN", false);
            case "date":
            case "datewithtimezone":
                return SqlTypeInfo.simple(Types.DATE, "DATE", false);
            case "datetime":
            case "datetimewithtimezone":
            case "lastmodifiedtime":
            case "createdtime":
                return SqlTypeInfo.simple(Types.TIMESTAMP, "TIMESTAMP", true);
            case "autonumber":
            case "integer":
                return SqlTypeInfo.numeric(Types.BIGINT, "BIGINT", true);
            case "lookup":
                return SqlTypeInfo.variable(Types.VARCHAR, "VARCHAR", null, true);
            case "email":
            case "phonenumber":
            case "url":
            case "attachments":
            case "barcode":
            case "singlelinetext":
            case "multilinetext":
            case "singleselect":
            case "multipleselects":
            case "richtext":
            default:
                return SqlTypeInfo.variable(Types.VARCHAR, "VARCHAR", null, false);
        }
    }

    private static final class SqlTypeInfo {
        final int jdbcType;
        final String typeName;
        final Integer columnSize;
        final Integer decimalDigits;
        final Integer radix;
        final Integer charOctetLength;
        final boolean generated;

        private SqlTypeInfo(int jdbcType, String typeName, Integer columnSize, Integer decimalDigits, Integer radix, Integer charOctetLength, boolean generated) {
            this.jdbcType = jdbcType;
            this.typeName = typeName;
            this.columnSize = columnSize;
            this.decimalDigits = decimalDigits;
            this.radix = radix;
            this.charOctetLength = charOctetLength;
            this.generated = generated;
        }

        static SqlTypeInfo simple(int jdbcType, String typeName, boolean generated) {
            return new SqlTypeInfo(jdbcType, typeName, null, null, null, null, generated);
        }

        static SqlTypeInfo numeric(int jdbcType, String typeName, boolean generated) {
            return new SqlTypeInfo(jdbcType, typeName, null, null, 10, null, generated);
        }

        static SqlTypeInfo variable(int jdbcType, String typeName, Integer columnSize, boolean generated) {
            Integer size = columnSize != null ? columnSize : 255;
            return new SqlTypeInfo(jdbcType, typeName, size, null, null, size, generated);
        }
    }

    private ResultSet createResultSet(List<Map<String, Object>> rows, List<String> columns) throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>(rows);
        List<String> columnOrder = new ArrayList<>(columns);
        return new AirtableResultSet(new AirtableStatement(connection), data, columnOrder);
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
