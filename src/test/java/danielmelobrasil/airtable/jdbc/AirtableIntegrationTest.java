package danielmelobrasil.airtable.jdbc;

import org.junit.Assume;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import static org.junit.Assert.assertNotNull;

/**
 * Integration test that exercises the driver against a live Airtable base.
 * Requires the environment variable AIRTABLE_API_KEY to be defined and network access.
 */
public class AirtableIntegrationTest {

    private static final String TABLE_QUERY = "SELECT p.Plano, t.Tarefa, t.`Agrupamento (Plano de Ação)`, t.`Responsáveis` FROM `Planos de Ação` as p LEFT JOIN `Tarefas` t ON (p.id = t.`Planos de Ação`) WHERE p.id = ?";

    @Test
    public void queryPlanosDeAcao() throws Exception {
        String apiKey = System.getenv("AIRTABLE_API_KEY");
        Assume.assumeTrue("AIRTABLE_API_KEY environment variable must be set to run this test.", apiKey != null && !apiKey.isEmpty());
        String baseId = System.getenv("AIRTABLE_BASE_ID");
        Assume.assumeTrue("AIRTABLE_BASE_ID environment variable must be set to run this test.", baseId != null && !baseId.isEmpty());

        String url = "jdbc:airtable://" + apiKey + "@" + baseId;

        try (Connection connection = DriverManager.getConnection(url);
             java.sql.PreparedStatement ps = connection.prepareStatement(TABLE_QUERY)) {
            ps.setString(1, "recomLauldghNXbGI");

            try (ResultSet resultSet = ps.executeQuery()) {
                assertNotNull("Result set should not be null", resultSet);

                System.out.println("Resultados para '" + TABLE_QUERY + "':");
                int columnCount = resultSet.getMetaData().getColumnCount();
                while (resultSet.next()) {
                    StringBuilder row = new StringBuilder();
                    for (int i = 1; i <= columnCount; i++) {
                        if (i > 1) {
                            row.append(" | ");
                        }
                        row.append(resultSet.getMetaData().getColumnLabel(i))
                                .append(": ")
                                .append(resultSet.getString(i));
                    }
                    System.out.println(row);
                }
            }

            DatabaseMetaData metaData = connection.getMetaData();
            assertNotNull(metaData);

            System.out.println("Tabelas disponíveis:");
            String firstTable = null;
            try (ResultSet tables = metaData.getTables(null, null, "%", null)) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("- " + tableName);
                    if (firstTable == null) {
                        firstTable = tableName;
                    }
                }
            }

            if (firstTable != null) {
                System.out.println("Colunas de '" + firstTable + "':");
                try (ResultSet columns = metaData.getColumns(null, null, firstTable, "%")) {
                    while (columns.next()) {
                        System.out.println("  - " + columns.getString("COLUMN_NAME") + " : " + columns.getString("TYPE_NAME"));
                    }
                }
            }
        }
    }
}
