package io.confluent.ksql.planner;

import java.util.ArrayList;
import java.util.List;

public class Schema {

    final List<SchemaField> schemaFields;
    final List<String> fieldNames;

    public Schema(List<SchemaField> schemaField, List<String> fieldNames) {
        if(schemaField.size() != fieldNames.size()) {
            throw new PlanException("Wrong schema format.");
        }
        this.schemaFields = schemaField;
        this.fieldNames = fieldNames;

    }

    public List<SchemaField> getSchemaFields() {
        return schemaFields;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public SchemaField getFieldByName(String fieldName) {
        for(SchemaField field: schemaFields) {
            if(field.getFieldName().equalsIgnoreCase(fieldName)) {
                return field;
            }
        }
        return null;
    }

    public int getFieldIndexByName(String fieldName) {
        for(int i = 0; i < schemaFields.size(); i++) {
            SchemaField field = schemaFields.get(i);
            if(field.getFieldName().equalsIgnoreCase(fieldName)) {
                return i;
            }
        }
        return -1;
    }
    public Schema duplicate() {
        List<SchemaField> newSchemaFields = new ArrayList<>();
        List<String> newFieldNames = new ArrayList<>();
        for(int i = 0; i < schemaFields.size(); i++) {
            newSchemaFields.add(this.schemaFields.get(i).duplicate());
            newFieldNames.add(new String(this.fieldNames.get(i)));
        }
        return new Schema(newSchemaFields, newFieldNames);
    }
}
