package danielmelobrasil.airtable.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AirtableStatementTest {

    private AirtableConfig config;
    private CapturingApiClient apiClient;
    private AirtableConnection connection;

    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.setProperty("apiKey", "test");
        config = AirtableConfig.from("jdbc:airtable://app123456", props);
        apiClient = new CapturingApiClient(config);
        connection = new AirtableConnection(config, apiClient);
    }

    @Test
    public void executeQueryDelegatesToApiClient() throws Exception {
        apiClient.addRecord(record("Name", "Alice"));

        ResultSet rs = connection.createStatement()
                .executeQuery("SELECT Name FROM Contacts WHERE Status = 'Active' ORDER BY Name DESC LIMIT 5");

        assertNotNull(apiClient.getLastQuery());
        assertEquals("Contacts", apiClient.getLastQuery().getTableName());
        assertTrue(apiClient.getLastQuery().getFilterFormula().isPresent());
        assertEquals("{Status} = 'Active'", apiClient.getLastQuery().getFilterFormula().get());
        assertTrue(apiClient.getLastQuery().getMaxRecords().isPresent());
        assertEquals(Integer.valueOf(5), apiClient.getLastQuery().getMaxRecords().get());
        assertEquals(1, apiClient.getLastQuery().getSorts().size());
        assertEquals("Name", apiClient.getLastQuery().getSorts().get(0).getField());
        assertEquals(AirtableQuery.Sort.Direction.DESC, apiClient.getLastQuery().getSorts().get(0).getDirection());

        assertTrue(rs.next());
        assertEquals("Alice", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void statementMaxRowsOverridesQueryLimit() throws Exception {
        apiClient.addRecord(record("Name", "Bob"));
        AirtableStatement statement = (AirtableStatement) connection.createStatement();
        statement.setMaxRows(1);

        statement.executeQuery("SELECT Name FROM Contacts");

        assertTrue(apiClient.getLastQuery().getMaxRecords().isPresent());
        assertEquals(Integer.valueOf(1), apiClient.getLastQuery().getMaxRecords().get());
    }

    @Test
    public void executeQuerySupportsTableAlias() throws Exception {
        apiClient.addRecord(record("Name", "Alice"));

        ResultSet rs = connection.createStatement()
                .executeQuery("SELECT c.Name FROM Contacts AS c");

        assertNotNull(apiClient.getLastQuery());
        assertEquals("Contacts", apiClient.getLastQuery().getTableName());
        assertEquals(1, apiClient.getLastQuery().getSelectedFields().size());
        assertEquals("Name", apiClient.getLastQuery().getSelectedFields().get(0).getField());

        assertTrue(rs.next());
        assertEquals("Alice", rs.getString(1));
        assertEquals("c.Name", rs.getMetaData().getColumnLabel(1));
        assertFalse(rs.next());
    }

    @Test
    public void preparedStatementBindsParameters() throws Exception {
        apiClient.addRecord(record("Name", "Alice"));

        try (PreparedStatement ps = connection.prepareStatement("SELECT Name FROM Contacts WHERE Name = ?")) {
            ps.setString(1, "Alice");

            try (ResultSet rs = ps.executeQuery()) {
                assertNotNull(apiClient.getLastQuery());
                assertEquals("{Name} = 'Alice'", apiClient.getLastQuery().getFilterFormula().get());

                assertTrue(rs.next());
                assertEquals("Alice", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void leftJoinProducesCombinedRows() throws Exception {
        Map<String, Object> baseRecord = new LinkedHashMap<>();
        baseRecord.put("Name", "Alice");
        baseRecord.put("OrgId", "org1");
        apiClient.addRecord(baseRecord);

        Map<String, Object> joinRecord = new LinkedHashMap<>();
        joinRecord.put("Id", "org1");
        joinRecord.put("Industry", "Tech");
        apiClient.addJoinRecord(joinRecord);

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT Contacts.Name, Organizations.Industry FROM Contacts LEFT JOIN Organizations ON Contacts.OrgId = Organizations.Id"
        );

        assertNotNull(apiClient.getLastJoinQuery());
        assertEquals("Organizations", apiClient.getLastJoinQuery().getJoin().get().getTableName());

        assertTrue(rs.next());
        assertEquals("Alice", rs.getString(1));
        assertEquals("Tech", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void getMetaDataReturnsTablesAndColumns() throws Exception {
        apiClient.addMetaTable("Contacts",
                fieldMeta("Name", "singleLineText"),
                fieldMeta("Status", "singleSelect"));

        DatabaseMetaData metaData = connection.getMetaData();
        assertNotNull(metaData);

        try (ResultSet tables = metaData.getTables(null, null, "Con%", null)) {
            System.out.println("MetaData tables:");
            assertTrue(tables.next());
            assertEquals("Contacts", tables.getString("TABLE_NAME"));
            assertEquals("TABLE", tables.getString("TABLE_TYPE"));
            System.out.println("- " + tables.getString("TABLE_NAME") + " (" + tables.getString("TABLE_TYPE") + ")");
            assertFalse(tables.next());
        }

        try (ResultSet columns = metaData.getColumns(null, null, "Contacts", "%")) {
            System.out.println("Columns for Contacts:");
            assertTrue(columns.next());
            assertEquals("Contacts", columns.getString("TABLE_NAME"));
            assertEquals("Name", columns.getString("COLUMN_NAME"));
            assertEquals("VARCHAR", columns.getString("TYPE_NAME"));
            System.out.println("- " + columns.getString("COLUMN_NAME") + " : " + columns.getString("TYPE_NAME"));

            assertTrue(columns.next());
            assertEquals("Status", columns.getString("COLUMN_NAME"));
            assertEquals("VARCHAR", columns.getString("TYPE_NAME"));
            System.out.println("- " + columns.getString("COLUMN_NAME") + " : " + columns.getString("TYPE_NAME"));
            assertFalse(columns.next());
        }
    }

    private static Map<String, Object> record(String key, Object value) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    private static final class CapturingApiClient extends AirtableApiClient {

        private final List<Map<String, Object>> records = new ArrayList<>();
        private final List<Map<String, Object>> joinRecords = new ArrayList<>();
        private final List<MetaTable> metadataTables = new ArrayList<>();
        private AirtableQuery lastQuery;
        private AirtableQuery lastJoinQuery;

        CapturingApiClient(AirtableConfig config) {
            super(config);
        }

        void addRecord(Map<String, Object> record) {
            records.add(record);
        }

        void addJoinRecord(Map<String, Object> record) {
            joinRecords.add(record);
        }

        @Override
        List<Map<String, Object>> select(AirtableQuery query) {
            this.lastQuery = query;
            return new ArrayList<>(records);
        }

        @Override
        List<Map<String, Object>> selectJoinTable(AirtableQuery query) {
            this.lastJoinQuery = query;
            return new ArrayList<>(joinRecords);
        }

        void addMetaTable(String name, MetaField... fields) {
            MetaTable table = new MetaTable();
            table.name = name;
            table.fields = new ArrayList<>();
            Collections.addAll(table.fields, fields);
            metadataTables.add(table);
        }

        @Override
        List<MetaTable> fetchTablesMetadata() {
            return new ArrayList<>(metadataTables);
        }

        AirtableQuery getLastQuery() {
            return lastQuery;
        }

        AirtableQuery getLastJoinQuery() {
            return lastJoinQuery;
        }
    }

    private static AirtableApiClient.MetaField fieldMeta(String name, String type) {
        AirtableApiClient.MetaField field = new AirtableApiClient.MetaField();
        field.name = name;
        field.type = type;
        return field;
    }
}
