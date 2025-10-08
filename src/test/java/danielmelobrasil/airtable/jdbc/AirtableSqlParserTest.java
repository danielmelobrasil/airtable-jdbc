package danielmelobrasil.airtable.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AirtableSqlParserTest {

    @Test
    public void parseSimpleSelectAll() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse("SELECT * FROM Contacts;");
        assertEquals("Contacts", query.getTableName());
        assertTrue(query.getSelectedFields().isEmpty());
        assertFalse(query.getFilterFormula().isPresent());
        assertFalse(query.getMaxRecords().isPresent());
        assertTrue(query.getSorts().isEmpty());
    }

    @Test
    public void parseSelectWithClauses() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse(
                "SELECT Name, Email FROM Contacts WHERE Status = 'Active' AND Score = 10 ORDER BY Name DESC, Email ASC LIMIT 42"
        );
        assertEquals("Contacts", query.getTableName());
        assertEquals(2, query.getSelectedFields().size());
        assertEquals("Name", query.getSelectedFields().get(0).getField());
        assertEquals("Email", query.getSelectedFields().get(1).getField());
        assertTrue(query.getFilterFormula().isPresent());
        assertEquals("AND({Status} = 'Active',{Score} = 10)", query.getFilterFormula().get());
        assertTrue(query.getMaxRecords().isPresent());
        assertEquals(Integer.valueOf(42), query.getMaxRecords().get());
        assertEquals(2, query.getSorts().size());
        assertEquals("Name", query.getSorts().get(0).getField());
        assertEquals(AirtableQuery.Sort.Direction.DESC, query.getSorts().get(0).getDirection());
        assertEquals("Email", query.getSorts().get(1).getField());
        assertEquals(AirtableQuery.Sort.Direction.ASC, query.getSorts().get(1).getDirection());
        assertFalse(query.getJoin().isPresent());
    }

    @Test
    public void parseSelectWithTableAlias() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse(
                "SELECT c.Name FROM Contacts AS c WHERE c.Status = 'Active'"
        );

        assertEquals("Contacts", query.getTableName());
        assertEquals(1, query.getSelectedFields().size());
        AirtableQuery.SelectedField field = query.getSelectedFields().get(0);
        assertEquals("Name", field.getField());
        assertEquals("c.Name", field.getLabel());
        assertTrue(query.getFilterFormula().isPresent());
        assertEquals("{Status} = 'Active'", query.getFilterFormula().get());
    }

    @Test
    public void parseWhereOnRecordId() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse(
                "SELECT Name FROM Contacts WHERE id = 'rec123'"
        );

        assertTrue(query.getFilterFormula().isPresent());
        assertEquals("RECORD_ID() = 'rec123'", query.getFilterFormula().get());
    }

    @Test
    public void parseWhereOnAliasedRecordId() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse(
                "SELECT c.Name FROM Contacts c WHERE c.id = 'recABC'"
        );

        assertTrue(query.getFilterFormula().isPresent());
        assertEquals("RECORD_ID() = 'recABC'", query.getFilterFormula().get());
    }

    @Test
    public void parseSelectWithLeftJoin() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse(
                "SELECT Contacts.Name AS contact_name, Organizations.Name AS org_name " +
                        "FROM Contacts LEFT JOIN Organizations ON Contacts.OrgId = Organizations.Id " +
                        "WHERE Contacts.Status = 'Active'"
        );

        assertTrue(query.getJoin().isPresent());
        AirtableQuery.Join join = query.getJoin().get();
        assertEquals("Organizations", join.getTableName());
        assertEquals("OrgId", join.getLeftField());
        assertEquals("Id", join.getRightField());

        assertEquals(2, query.getSelectedFields().size());
        AirtableQuery.SelectedField first = query.getSelectedFields().get(0);
        assertEquals(AirtableQuery.SelectedField.Origin.BASE, first.getOrigin());
        assertEquals("contact_name", first.getLabel());

        AirtableQuery.SelectedField second = query.getSelectedFields().get(1);
        assertEquals(AirtableQuery.SelectedField.Origin.JOIN, second.getOrigin());
        assertEquals("org_name", second.getLabel());

        assertTrue(query.getFilterFormula().isPresent());
        assertEquals("{Status} = 'Active'", query.getFilterFormula().get());
    }

    @Test(expected = AirtableSqlParseException.class)
    public void parseRejectsSelectStarWithJoin() throws Exception {
        AirtableSqlParser.parse("SELECT * FROM Contacts LEFT JOIN Organizations ON Contacts.OrgId = Organizations.Id");
    }

    @Test(expected = AirtableSqlParseException.class)
    public void parseRejectsNonSelectStatements() throws Exception {
        AirtableSqlParser.parse("DELETE FROM Contacts");
    }

    @Test(expected = AirtableSqlParseException.class)
    public void parseRejectsUnsupportedWhere() throws Exception {
        AirtableSqlParser.parse("SELECT * FROM Contacts WHERE Status LIKE 'A%'");
    }

    @Test
    public void parseAcceptsLeftJointTypo() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse(
                "SELECT Contacts.Name, Organizations.Name FROM Contacts LEFT JOINT Organizations ON Contacts.OrgId = Organizations.Id"
        );
        assertTrue(query.getJoin().isPresent());
    }

    @Test
    public void parseSelectWithJoinAliases() throws Exception {
        AirtableQuery query = AirtableSqlParser.parse(
                "SELECT c.Name, o.Name FROM Contacts c LEFT JOIN Organizations AS o ON c.OrgId = o.Id"
        );

        assertEquals("Contacts", query.getTableName());
        assertTrue(query.getJoin().isPresent());
        AirtableQuery.Join join = query.getJoin().get();
        assertEquals("Organizations", join.getTableName());
        assertEquals("OrgId", join.getLeftField());
        assertEquals("Id", join.getRightField());

        assertEquals(2, query.getSelectedFields().size());
        assertEquals("c.Name", query.getSelectedFields().get(0).getLabel());
        assertEquals("o.Name", query.getSelectedFields().get(1).getLabel());
    }
}
