package danielmelobrasil.airtable.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a translated Airtable query derived from SQL.
 */
final class AirtableQuery {

    private final String tableName;
    private final List<SelectedField> selectedFields;
    private final Optional<String> filterFormula;
    private final Optional<Integer> maxRecords;
    private final List<Sort> sorts;
    private final Optional<Join> join;

    AirtableQuery(
            String tableName,
            List<SelectedField> selectedFields,
            Optional<String> filterFormula,
            Optional<Integer> maxRecords,
            List<Sort> sorts
    ) {
        this(tableName, selectedFields, filterFormula, maxRecords, sorts, Optional.empty());
    }

    AirtableQuery(
            String tableName,
            List<SelectedField> selectedFields,
            Optional<String> filterFormula,
            Optional<Integer> maxRecords,
            List<Sort> sorts,
            Optional<Join> join
    ) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.selectedFields = Collections.unmodifiableList(new ArrayList<>(selectedFields));
        this.filterFormula = Objects.requireNonNull(filterFormula, "filterFormula");
        this.maxRecords = Objects.requireNonNull(maxRecords, "maxRecords");
        this.sorts = Collections.unmodifiableList(new ArrayList<>(sorts));
        this.join = Objects.requireNonNull(join, "join");
    }

    String getTableName() {
        return tableName;
    }

    List<SelectedField> getSelectedFields() {
        return selectedFields;
    }

    List<String> getColumnLabels() {
        List<String> labels = new ArrayList<>(selectedFields.size());
        for (SelectedField field : selectedFields) {
            labels.add(field.getLabel());
        }
        return labels;
    }

    Optional<String> getFilterFormula() {
        return filterFormula;
    }

    Optional<Integer> getMaxRecords() {
        return maxRecords;
    }

    List<Sort> getSorts() {
        return sorts;
    }

    Optional<Join> getJoin() {
        return join;
    }

    List<String> getRequiredFieldsForBaseTable() {
        List<String> fields = new ArrayList<>();
        for (SelectedField field : selectedFields) {
            if (field.getOrigin() == SelectedField.Origin.BASE) {
                if (!isRecordIdField(field.getField())) {
                    fields.add(field.getField());
                }
            }
        }
        join.ifPresent(j -> {
            if (!isRecordIdField(j.getLeftField()) && !fields.contains(j.getLeftField())) {
                fields.add(j.getLeftField());
            }
        });
        return fields;
    }

    List<String> getRequiredFieldsForJoinTable() {
        if (!join.isPresent()) {
            return Collections.emptyList();
        }
        List<String> fields = new ArrayList<>();
        for (SelectedField field : selectedFields) {
            if (field.getOrigin() == SelectedField.Origin.JOIN) {
                fields.add(field.getField());
            }
        }
        Join joinValue = join.get();
        if (!isRecordIdField(joinValue.getRightField()) && !fields.contains(joinValue.getRightField())) {
            fields.add(joinValue.getRightField());
        }
        return fields;
    }

    static final class SelectedField {
        enum Origin {
            BASE, JOIN
        }

        private final Origin origin;
        private final String field;
        private final String label;

        SelectedField(Origin origin, String field, String label) {
            this.origin = Objects.requireNonNull(origin, "origin");
            this.field = Objects.requireNonNull(field, "field");
            this.label = Objects.requireNonNull(label, "label");
        }

        Origin getOrigin() {
            return origin;
        }

        String getField() {
            return field;
        }

        String getLabel() {
            return label;
        }
    }

    private static boolean isRecordIdField(String field) {
        return "id".equalsIgnoreCase(field) || "record_id".equalsIgnoreCase(field);
    }

    static final class Sort {
        enum Direction {
            ASC, DESC
        }

        private final String field;
        private final Direction direction;

        Sort(String field, Direction direction) {
            this.field = Objects.requireNonNull(field, "field");
            this.direction = Objects.requireNonNull(direction, "direction");
        }

        String getField() {
            return field;
        }

        Direction getDirection() {
            return direction;
        }
    }

    static final class Join {
        enum Type {
            LEFT
        }

        private final Type type;
        private final String tableName;
        private final Optional<String> alias;
        private final String leftField;
        private final String rightField;

        Join(Type type, String tableName, Optional<String> alias, String leftField, String rightField) {
            this.type = Objects.requireNonNull(type, "type");
            this.tableName = Objects.requireNonNull(tableName, "tableName");
            this.alias = Objects.requireNonNull(alias, "alias");
            this.leftField = Objects.requireNonNull(leftField, "leftField");
            this.rightField = Objects.requireNonNull(rightField, "rightField");
        }

        Type getType() {
            return type;
        }

        String getTableName() {
            return tableName;
        }

        Optional<String> getAlias() {
            return alias;
        }

        String getLeftField() {
            return leftField;
        }

        String getRightField() {
            return rightField;
        }

        boolean matchesAlias(String candidate) {
            if (candidate == null) {
                return false;
            }
            if (tableName.equalsIgnoreCase(candidate)) {
                return true;
            }
            return alias.filter(a -> a.equalsIgnoreCase(candidate)).isPresent();
        }
    }
}
