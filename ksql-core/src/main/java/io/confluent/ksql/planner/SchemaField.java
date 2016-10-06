package io.confluent.ksql.planner;

import io.confluent.ksql.planner.types.Type;

public class SchemaField {

    final String fieldName;
    final Type fieldType;

    public SchemaField(String fieldName, Type fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Type getFieldType() {
        return fieldType;
    }

    public SchemaField duplicate() {
        return new SchemaField(new String(fieldName), fieldType);
    }
}
