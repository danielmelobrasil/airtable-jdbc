package danielmelobrasil.airtable.jdbc;

/**
 * Exception thrown when the SQL query cannot be translated into an Airtable request.
 */
class AirtableSqlParseException extends Exception {
    AirtableSqlParseException(String message) {
        super(message);
    }

    AirtableSqlParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
