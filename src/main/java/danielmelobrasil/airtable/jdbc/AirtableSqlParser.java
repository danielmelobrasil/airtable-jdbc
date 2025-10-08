package danielmelobrasil.airtable.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates a subset of SQL SELECT statements into Airtable query descriptors.
 *
 * Supported constructs:
 *  - SELECT column list (optional aliases) without aggregations
 *  - FROM tableName
 *  - Optional LEFT JOIN table2 ON base.field = join.field
 *  - Optional WHERE clause with equality comparisons combined using AND (base table only)
 *  - Optional ORDER BY with ASC/DESC (base table only)
 *  - Optional LIMIT
 */
final class AirtableSqlParser {

    private static final Pattern AND_SPLIT = Pattern.compile("(?i)\\s+AND\\s+");
    private static final Pattern COMPARISON = Pattern.compile("^([\\w\\s\\.]+?)\\s*=\\s*(.+)$");
    private static final Pattern ORDER_BY_SPLIT = Pattern.compile("\\s*,\\s*");
    private static final Pattern ORDER_BY_TOKEN = Pattern.compile("^(.+?)\\s+(ASC|DESC)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern AS_ALIAS = Pattern.compile("(?i)^(.*?)\\s+AS\\s+(.*)$");
    private static final Pattern TABLE_TOKEN = Pattern.compile("(?i)^(?<table>`[^`]+`|\"[^\"]+\"|'[^']+'|[^\\s]+)(?:\\s+(?:AS\\s+)?(?<alias>[^\\s]+))?$");
    private static final Pattern ON_CLAUSE = Pattern.compile("(?i)\\sON\\s");

    private AirtableSqlParser() {
        // utility class
    }

    static AirtableQuery parse(String sql) throws AirtableSqlParseException {
        if (sql == null) {
            throw new AirtableSqlParseException("SQL cannot be null.");
        }
        String trimmed = sql.trim();
        trimmed = trimmed.replaceAll("\\s+", " ");
        if (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }

        if (!trimmed.regionMatches(true, 0, "SELECT", 0, "SELECT".length())) {
            throw new AirtableSqlParseException("Only SELECT statements are supported.");
        }

        int fromIndex = indexOfKeyword(trimmed, "FROM");
        if (fromIndex < 0) {
            throw new AirtableSqlParseException("Missing FROM clause.");
        }

        String columnSegment = trimmed.substring("SELECT".length(), fromIndex).trim();
        int whereIndex = indexOfKeyword(trimmed, "WHERE");
        int orderIndex = indexOfKeyword(trimmed, "ORDER BY");
        int limitIndex = indexOfKeyword(trimmed, "LIMIT");

        int tableEnd = firstPositive(whereIndex, orderIndex, limitIndex, trimmed.length());
        int tableStart = skipKeyword(trimmed, fromIndex, "FROM");
        String tableSegment = trimmed.substring(tableStart, tableEnd).trim();
        if (tableSegment.isEmpty()) {
            throw new AirtableSqlParseException("Table name is required in FROM clause.");
        }

        TableContext tableContext = parseTableSegment(tableSegment);
        String whereSegment = extractWhereSegment(trimmed, whereIndex, orderIndex, limitIndex);
        String orderSegment = extractOrderSegment(trimmed, orderIndex, limitIndex);
        Optional<Integer> limit = extractLimit(trimmed, limitIndex);

        List<AirtableQuery.SelectedField> columns = parseColumns(columnSegment, tableContext);
        Optional<String> filterFormula = parseWhere(whereSegment, tableContext);
        List<AirtableQuery.Sort> sorts = parseOrderBy(orderSegment, tableContext);

        return new AirtableQuery(
                tableContext.baseTable().name(),
                columns,
                filterFormula,
                limit,
                sorts,
                tableContext.join()
        );
    }

    private static List<AirtableQuery.SelectedField> parseColumns(String columnSegment, TableContext context) throws AirtableSqlParseException {
        if ("*".equals(columnSegment)) {
            if (context.join().isPresent()) {
                throw new AirtableSqlParseException("SELECT * não é suportado em consultas com LEFT JOIN.");
            }
            return Collections.emptyList();
        }

        String[] parts = columnSegment.split(",");
        List<AirtableQuery.SelectedField> columns = new ArrayList<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                columns.add(parseSelectedField(trimmed, context));
            }
        }

        if (columns.isEmpty()) {
            throw new AirtableSqlParseException("A lista de colunas não pode estar vazia.");
        }
        return columns;
    }

    private static AirtableQuery.SelectedField parseSelectedField(String token, TableContext context) throws AirtableSqlParseException {
        String expression = token.trim();
        String alias = null;

        Matcher asMatcher = AS_ALIAS.matcher(token);
        if (asMatcher.find()) {
            expression = asMatcher.group(1).trim();
            alias = asMatcher.group(2).trim();
        } else {
            SplitExpression split = splitExpressionAndAlias(expression);
            expression = split.expression;
            alias = split.alias;
        }

        FieldReference reference = parseFieldReference(expression);
        AirtableQuery.SelectedField.Origin origin = AirtableQuery.SelectedField.Origin.BASE;
        String fieldName = reference.field;

        if (reference.table != null) {
            if (context.isBaseTable(reference.table)) {
                origin = AirtableQuery.SelectedField.Origin.BASE;
            } else if (context.join().isPresent() && context.join().get().matchesAlias(reference.table)) {
                origin = AirtableQuery.SelectedField.Origin.JOIN;
            } else {
                throw new AirtableSqlParseException("Coluna " + expression + " referencia uma tabela desconhecida.");
            }
        }

        if (origin == AirtableQuery.SelectedField.Origin.JOIN && !context.join().isPresent()) {
            throw new AirtableSqlParseException("Coluna " + expression + " referencia tabela de JOIN inexistente.");
        }

        if (origin == AirtableQuery.SelectedField.Origin.BASE && fieldName.isEmpty()) {
            throw new AirtableSqlParseException("Coluna base inválida: " + expression);
        }

        String label;
        if (alias != null && !alias.isEmpty()) {
            label = alias;
        } else if (reference.table != null) {
            label = reference.table + "." + fieldName;
        } else {
            label = fieldName;
        }

        return new AirtableQuery.SelectedField(origin, fieldName, label);
    }

    private static Optional<String> parseWhere(String whereSegment, TableContext context) throws AirtableSqlParseException {
        if (whereSegment == null || whereSegment.isEmpty()) {
            return Optional.empty();
        }
        String[] expressions = AND_SPLIT.split(whereSegment);
        if (expressions.length == 0) {
            return Optional.empty();
        }

        List<String> formulas = new ArrayList<>();
        for (String expression : expressions) {
            String trimmed = expression.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            Matcher matcher = COMPARISON.matcher(trimmed);
            if (!matcher.matches()) {
                throw new AirtableSqlParseException("Unsupported WHERE predicate: " + trimmed);
            }
            String fieldToken = matcher.group(1).trim();
            FieldReference reference = resolveBaseFieldReference(fieldToken, context, "WHERE");
            String valueLiteral = matcher.group(2).trim();
            formulas.add(buildEqualityFormula(reference, valueLiteral));
        }

        if (formulas.isEmpty()) {
            return Optional.empty();
        }

        if (formulas.size() == 1) {
            return Optional.of(formulas.get(0));
        }

        String joined = "AND(" + String.join(",", formulas) + ")";
        return Optional.of(joined);
    }

    private static String buildEqualityFormula(FieldReference reference, String valueLiteral) throws AirtableSqlParseException {
        if (reference.isRecordId()) {
            String content = isQuoted(valueLiteral) ? unquote(valueLiteral) : valueLiteral;
            if (content.isEmpty()) {
                throw new AirtableSqlParseException("RECORD_ID() requires a value.");
            }
            return String.format("RECORD_ID() = '%s'", escapeFormulaValue(content));
        }
        return buildFieldEqualityFormula(reference.field, valueLiteral);
    }

    private static String buildFieldEqualityFormula(String field, String valueLiteral) throws AirtableSqlParseException {
        if (isQuoted(valueLiteral)) {
            String content = unquote(valueLiteral);
            return String.format("{%s} = '%s'", field, escapeFormulaValue(content));
        }

        if (isNumeric(valueLiteral)) {
            return String.format(Locale.ENGLISH, "{%s} = %s", field, valueLiteral);
        }

        throw new AirtableSqlParseException("WHERE values must be quoted strings or numeric literals: " + valueLiteral);
    }

    private static List<AirtableQuery.Sort> parseOrderBy(String orderSegment, TableContext context) throws AirtableSqlParseException {
        if (orderSegment == null || orderSegment.isEmpty()) {
            return Collections.emptyList();
        }
        String[] items = ORDER_BY_SPLIT.split(orderSegment);
        List<AirtableQuery.Sort> sorts = new ArrayList<>();
        for (String item : items) {
            String trimmed = item.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            Matcher matcher = ORDER_BY_TOKEN.matcher(trimmed);
            if (!matcher.matches()) {
                throw new AirtableSqlParseException("Unsupported ORDER BY clause: " + trimmed);
            }
            String fieldToken = matcher.group(1).trim();
            String field = resolveBaseField(fieldToken, context, "ORDER BY");
            String directionToken = matcher.group(2);
            AirtableQuery.Sort.Direction direction = AirtableQuery.Sort.Direction.ASC;
            if (directionToken != null) {
                direction = "DESC".equalsIgnoreCase(directionToken)
                        ? AirtableQuery.Sort.Direction.DESC
                        : AirtableQuery.Sort.Direction.ASC;
            }
            sorts.add(new AirtableQuery.Sort(field, direction));
        }
        return sorts;
    }

    private static Optional<Integer> extractLimit(String sql, int limitIndex) throws AirtableSqlParseException {
        if (limitIndex < 0) {
            return Optional.empty();
        }
        int start = skipKeyword(sql, limitIndex, "LIMIT");
        String limitPart = sql.substring(start).trim();
        if (limitPart.isEmpty()) {
            throw new AirtableSqlParseException("LIMIT requires a numeric value.");
        }
        try {
            return Optional.of(Integer.parseInt(limitPart));
        } catch (NumberFormatException ex) {
            throw new AirtableSqlParseException("Invalid LIMIT value: " + limitPart, ex);
        }
    }

    private static String extractWhereSegment(String sql, int whereIndex, int orderIndex, int limitIndex) {
        if (whereIndex < 0) {
            return null;
        }
        int endIndex = firstPositive(orderIndex, limitIndex, sql.length());
        int start = skipKeyword(sql, whereIndex, "WHERE");
        return sql.substring(start, endIndex).trim();
    }

    private static String extractOrderSegment(String sql, int orderIndex, int limitIndex) {
        if (orderIndex < 0) {
            return null;
        }
        int endIndex = limitIndex >= 0 ? limitIndex : sql.length();
        int start = skipKeyword(sql, orderIndex, "ORDER BY");
        return sql.substring(start, endIndex).trim();
    }

    private static TableContext parseTableSegment(String segment) throws AirtableSqlParseException {
        int joinIndex = indexOfJoinKeyword(segment);
        if (joinIndex < 0) {
            TableSpec baseSpec = parseTableSpec(segment.trim());
            if (baseSpec.name().isEmpty()) {
                throw new AirtableSqlParseException("Table name is required in FROM clause.");
            }
            return new TableContext(baseSpec, Optional.empty());
        }

        String baseToken = segment.substring(0, joinIndex).trim();
        TableSpec baseSpec = parseTableSpec(baseToken);
        if (baseSpec.name().isEmpty()) {
            throw new AirtableSqlParseException("Table name is required before LEFT JOIN.");
        }

        String joinSegment = segment.substring(joinIndex).trim();
        AirtableQuery.Join join = parseJoin(baseSpec, joinSegment);
        return new TableContext(baseSpec, Optional.of(join));
    }

    private static AirtableQuery.Join parseJoin(TableSpec baseTable, String joinSegment) throws AirtableSqlParseException {
        String normalized = joinSegment.trim();
        String lower = normalized.toLowerCase(Locale.ENGLISH);
        boolean isJoint = lower.startsWith("left joint ");
        boolean isJoin = lower.startsWith("left join ");
        if (!isJoin && !isJoint) {
            throw new AirtableSqlParseException("Only LEFT JOIN clauses are supported.");
        }

        int keywordLength = (isJoint ? "left joint " : "left join ").length();
        String remainder = normalized.substring(keywordLength).trim();
        Matcher onMatcher = ON_CLAUSE.matcher(remainder);
        if (!onMatcher.find()) {
            throw new AirtableSqlParseException("LEFT JOIN requires an ON clause.");
        }

        String tableToken = remainder.substring(0, onMatcher.start()).trim();
        if (tableToken.isEmpty()) {
            throw new AirtableSqlParseException("Join table name is required.");
        }

        String condition = remainder.substring(onMatcher.end()).trim();
        String[] equality = condition.split("=");
        if (equality.length != 2) {
            throw new AirtableSqlParseException("Only equality conditions are supported in LEFT JOIN.");
        }

        FieldReference left = parseFieldReference(stripEnclosingParentheses(equality[0].trim()));
        FieldReference right = parseFieldReference(stripEnclosingParentheses(equality[1].trim()));

        if (left.table == null || !baseTable.matches(left.table)) {
            throw new AirtableSqlParseException(
                    "LEFT JOIN só suporta condições onde o lado esquerdo referencia a tabela base (recebido: "
                            + (left.table == null ? "<null>" : left.table) + ")."
            );
        }

        TableSpec joinSpec = parseTableSpec(tableToken);
        String joinTableName = joinSpec.name();
        Optional<String> alias = joinSpec.alias();

        if (right.table == null) {
            throw new AirtableSqlParseException("O lado direito do LEFT JOIN deve estar qualificado com a tabela de JOIN.");
        }

        AirtableQuery.Join join = new AirtableQuery.Join(
                AirtableQuery.Join.Type.LEFT,
                joinTableName,
                alias,
                left.field,
                right.field
        );

        if (!join.matchesAlias(right.table)) {
            throw new AirtableSqlParseException("A condição do LEFT JOIN deve referenciar a tabela " + joinTableName + ".");
        }

        return join;
    }

    private static FieldReference parseFieldReference(String expression) throws AirtableSqlParseException {
        String trimmed = stripEnclosingParentheses(expression.trim());
        while (trimmed.startsWith("(")) {
            trimmed = trimmed.substring(1).trim();
        }
        while (trimmed.endsWith(")")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        }
        if (trimmed.isEmpty()) {
            throw new AirtableSqlParseException("Campo não pode ser vazio.");
        }
        int dotIndex = trimmed.indexOf('.');
        if (dotIndex < 0) {
            return new FieldReference(null, normalizeIdentifier(trimmed));
        }

        String table = normalizeIdentifier(trimmed.substring(0, dotIndex).trim());
        String field = normalizeIdentifier(trimmed.substring(dotIndex + 1).trim());
        if (table.isEmpty() || field.isEmpty()) {
            throw new AirtableSqlParseException("Campo " + expression + " é inválido.");
        }
        return new FieldReference(table, field);
    }

    private static String resolveBaseField(String token, TableContext context, String clause) throws AirtableSqlParseException {
        return resolveBaseFieldReference(token, context, clause).field;
    }

    private static FieldReference resolveBaseFieldReference(String token, TableContext context, String clause) throws AirtableSqlParseException {
        FieldReference reference = parseFieldReference(token);
        if (reference.table != null && !context.isBaseTable(reference.table)) {
            throw new AirtableSqlParseException(clause + " só suporta campos da tabela base.");
        }
        return reference;
    }

    private static String stripEnclosingParentheses(String value) {
        String trimmed = value;
        while (trimmed.length() >= 2 && trimmed.startsWith("(") && trimmed.endsWith(")")) {
            int depth = 0;
            boolean balanced = true;
            for (int i = 0; i < trimmed.length(); i++) {
                char ch = trimmed.charAt(i);
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth--;
                    if (depth < 0) {
                        balanced = false;
                        break;
                    }
                    if (depth == 0 && i < trimmed.length() - 1) {
                        balanced = false;
                        break;
                    }
                }
            }
            if (balanced && depth == 0) {
                trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
            } else {
                break;
            }
        }
        return trimmed;
    }

    private static int indexOfKeyword(String source, String keyword) {
        return source.toLowerCase(Locale.ENGLISH).indexOf(keyword.toLowerCase(Locale.ENGLISH));
    }

    private static int indexOfJoinKeyword(String source) {
        String lower = source.toLowerCase(Locale.ENGLISH);
        int joinIndex = lower.indexOf(" left join ");
        if (joinIndex >= 0) {
            return joinIndex;
        }
        return lower.indexOf(" left joint ");
    }

    private static int skipKeyword(String sql, int keywordIndex, String keyword) {
        int start = keywordIndex + keyword.length();
        while (start < sql.length() && Character.isWhitespace(sql.charAt(start))) {
            start++;
        }
        return start;
    }

    private static int firstPositive(int... indices) {
        return Arrays.stream(indices)
                .filter(idx -> idx >= 0)
                .min()
                .orElse(-1);
    }

    private static boolean isQuoted(String literal) {
        return literal.length() >= 2 &&
                ((literal.startsWith("'") && literal.endsWith("'")) ||
                        (literal.startsWith("\"") && literal.endsWith("\"")));
    }

    private static String unquote(String literal) {
        return literal.substring(1, literal.length() - 1);
    }

    private static String escapeFormulaValue(String value) {
        return value.replace("'", "\\'");
    }

    private static boolean isNumeric(String literal) {
        try {
            Double.parseDouble(literal);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private static final class TableContext {
        private final TableSpec baseTable;
        private final Optional<AirtableQuery.Join> join;

        private TableContext(TableSpec baseTable, Optional<AirtableQuery.Join> join) {
            this.baseTable = baseTable;
            this.join = join;
        }

        TableSpec baseTable() {
            return baseTable;
        }

        Optional<AirtableQuery.Join> join() {
            return join;
        }

        boolean isBaseTable(String candidate) {
            return baseTable.matches(candidate);
        }
    }

    private static final class TableSpec {
        private final String name;
        private final Optional<String> alias;

        private TableSpec(String name, Optional<String> alias) {
            this.name = name;
            this.alias = alias;
        }

        String name() {
            return name;
        }

        Optional<String> alias() {
            return alias;
        }

        boolean matches(String candidate) {
            if (candidate == null) {
                return false;
            }
            if (name.equalsIgnoreCase(candidate)) {
                return true;
            }
            return alias.filter(a -> a.equalsIgnoreCase(candidate)).isPresent();
        }
    }

    private static final class FieldReference {
        private final String table;
        private final String field;

        private FieldReference(String table, String field) {
            this.table = table;
            this.field = field;
        }

        boolean isRecordId() {
            return "id".equalsIgnoreCase(field);
        }
    }

    private static TableSpec parseTableSpec(String token) throws AirtableSqlParseException {
        String trimmed = token.trim();
        if (trimmed.isEmpty()) {
            return new TableSpec("", Optional.empty());
        }

        Matcher matcher = TABLE_TOKEN.matcher(trimmed);
        if (!matcher.matches()) {
            throw new AirtableSqlParseException("Sintaxe de tabela inválida: " + token);
        }

        String tableName = normalizeIdentifier(matcher.group("table"));
        String aliasGroup = matcher.group("alias");
        Optional<String> alias = aliasGroup != null
                ? Optional.of(normalizeIdentifier(aliasGroup))
                : Optional.empty();

        if (alias.isPresent() && alias.get().isEmpty()) {
            throw new AirtableSqlParseException("Alias não pode ser vazio: " + token);
        }

        return new TableSpec(tableName, alias);
    }

    private static String normalizeIdentifier(String token) {
        String value = token.trim();
        if ((value.startsWith("`") && value.endsWith("`")) ||
                (value.startsWith("\"") && value.endsWith("\"")) ||
                (value.startsWith("'") && value.endsWith("'"))) {
            value = value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static SplitExpression splitExpressionAndAlias(String expression) {
        SplitExpression result = new SplitExpression();
        result.expression = expression;
        result.alias = null;

        int length = expression.length();
        boolean inSingle = false;
        boolean inDouble = false;
        boolean inBacktick = false;

        for (int i = 0; i < length; i++) {
            char ch = expression.charAt(i);
            if (ch == '`' && !inSingle && !inDouble) {
                inBacktick = !inBacktick;
                continue;
            }
            if (ch == '\'' && !inDouble && !inBacktick) {
                inSingle = !inSingle;
                continue;
            }
            if (ch == '"' && !inSingle && !inBacktick) {
                inDouble = !inDouble;
                continue;
            }
            if (Character.isWhitespace(ch) && !inSingle && !inDouble && !inBacktick) {
                String before = expression.substring(0, i).trim();
                String after = expression.substring(i).trim();
                if (!before.isEmpty() && !after.isEmpty()) {
                    result.expression = before;
                    result.alias = after;
                }
                break;
            }
        }

        return result;
    }

    private static final class SplitExpression {
        private String expression;
        private String alias;
    }
}
