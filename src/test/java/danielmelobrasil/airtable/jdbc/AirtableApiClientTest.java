package danielmelobrasil.airtable.jdbc;

import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AirtableApiClientTest {

    @Test
    public void buildSelectUrlIncludesAllSortParameters() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse(
                "SELECT c.`Capacidade`, c.`Grupo`, c.`Tipo`, c.`Status`, c.`Início`, c.`Término`, c.`Publicação Oficial`, c.`Ordem` " +
                        "FROM `Capacidade nsApps` AS c ORDER BY c.`Tipo`, c.`Ordem`"
        );

        AirtableConfig config = createConfig();
        AirtableApiClient client = new AirtableApiClient(config);

        Method buildSelectUrl = AirtableApiClient.class.getDeclaredMethod(
                "buildSelectUrl",
                String.class,
                List.class,
                Optional.class,
                Optional.class,
                List.class,
                Optional.class,
                Optional.class
        );
        buildSelectUrl.setAccessible(true);

        String url = (String) buildSelectUrl.invoke(
                client,
                query.getTableName(),
                query.getRequiredFieldsForBaseTable(),
                query.getFilterFormula(),
                query.getMaxRecords(),
                query.getSorts(),
                config.getDefaultView(),
                Optional.empty()
        );

        assertTrue(url.contains("sort%5B0%5D%5Bfield%5D=Tipo"));
        assertTrue(url.contains("sort%5B1%5D%5Bfield%5D=Ordem"));
        assertTrue(url.contains("sort%5B0%5D%5Bdirection%5D=asc"));
        assertTrue(url.contains("sort%5B1%5D%5Bdirection%5D=asc"));
    }

    @Test
    public void buildSelectUrlIncludesMaxRecordsWhenLimitUsed() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse(
                "SELECT `Name` FROM Contacts LIMIT 5"
        );

        AirtableConfig config = createConfig();
        AirtableApiClient client = new AirtableApiClient(config);

        Method buildSelectUrl = AirtableApiClient.class.getDeclaredMethod(
                "buildSelectUrl",
                String.class,
                List.class,
                Optional.class,
                Optional.class,
                List.class,
                Optional.class,
                Optional.class
        );
        buildSelectUrl.setAccessible(true);

        String url = (String) buildSelectUrl.invoke(
                client,
                query.getTableName(),
                query.getRequiredFieldsForBaseTable(),
                query.getFilterFormula(),
                query.getMaxRecords(),
                query.getSorts(),
                config.getDefaultView(),
                Optional.empty()
        );

        assertTrue("URL should include maxRecords=5", url.contains("maxRecords=5"));
    }

    @Test
    public void statementPassesSortsToApiClient() throws Exception {
        AirtableConfig config = createConfig();
        RecordingAirtableApiClient client = new RecordingAirtableApiClient(config);
        AirtableConnection connection = new AirtableConnection(config, client);
        AirtableStatement statement = new AirtableStatement(connection);

        try (ResultSet ignored = statement.executeQuery(
                "SELECT c.`Capacidade`, c.`Grupo`, c.`Tipo`, c.`Status`, c.`Início`, c.`Término`, c.`Publicação Oficial`, c.`Ordem` " +
                        "FROM `Capacidade nsApps` AS c ORDER BY c.`Tipo`, c.`Ordem`"
        )) {
            // no-op
        }

        List<AirtableQuery.Sort> sorts = client.getLastSorts();
        assertNotNull("Sorts should be captured", sorts);
        assertEquals(2, sorts.size());
        assertEquals("Tipo", sorts.get(0).getField());
        assertEquals("Ordem", sorts.get(1).getField());
    }

    private static AirtableConfig createConfig() throws Exception {
        Properties props = new Properties();
        props.setProperty("apiKey", "testApiKey");
        return AirtableConfig.from("jdbc:airtable://testBase", props);
    }

    private static final class RecordingAirtableApiClient extends AirtableApiClient {
        private List<AirtableQuery.Sort> lastSorts = Collections.emptyList();

        RecordingAirtableApiClient(AirtableConfig config) {
            super(config);
        }

        @Override
        Map<String, String> getFieldTypes(String tableName) throws SQLException {
            return Collections.emptyMap();
        }

        @Override
        List<Map<String, Object>> select(AirtableQuery query) throws SQLException {
            lastSorts = new ArrayList<>(query.getSorts());
            return Collections.emptyList();
        }

        List<AirtableQuery.Sort> getLastSorts() {
            return lastSorts;
        }
    }
}
