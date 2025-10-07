package danielmelobrasil.airtable.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
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

    private static Map<String, Object> record(String key, Object value) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    private static final class CapturingApiClient extends AirtableApiClient {

        private final List<Map<String, Object>> records = new ArrayList<>();
        private final List<Map<String, Object>> joinRecords = new ArrayList<>();
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

        AirtableQuery getLastQuery() {
            return lastQuery;
        }

        AirtableQuery getLastJoinQuery() {
            return lastJoinQuery;
        }
    }
}
