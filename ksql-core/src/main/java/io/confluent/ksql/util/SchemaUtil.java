package io.confluent.ksql.util;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class SchemaUtil {

    public static Schema getTypeSchema(Schema.Type type) {
        if (type == Schema.Type.BOOLEAN) {
            return Schema.BOOLEAN_SCHEMA;
        } else if (type == Schema.Type.INT32) {
            return Schema.INT32_SCHEMA;
        } else if (type == Schema.Type.INT64) {
            return Schema.INT64_SCHEMA;
        } else if (type == Schema.Type.FLOAT64) {
            return Schema.FLOAT64_SCHEMA;
        } else if (type == Schema.Type.STRING) {
            return Schema.STRING_SCHEMA;
        }
        throw new KSQLException("Type is not supported: "+type);
    }

    public static Class getJavaType(Schema.Type type) {
        if(type == Schema.Type.BOOLEAN) {
            return Boolean.class;
        } else if(type == Schema.Type.INT32) {
            return Integer.class;
        } else if(type == Schema.Type.INT64) {
            return Long.class;
        } else if(type == Schema.Type.FLOAT64) {
            return Double.class;
        } else if(type == Schema.Type.STRING) {
            return String.class;
        }
        throw new KSQLException("Type is not supported: "+type);
    }

    public static Field getFieldByName(Schema schema, String fieldName) {
        if (schema.fields() != null) {
            for (Field field: schema.fields()) {
                if (field.name().equalsIgnoreCase(fieldName)) {
                    return  field;
                }
            }
        }
        return null;
    }

    public static int getFieldIndexByName(Schema schema, String fieldName) {
        if (schema.fields() != null) {
            for (int i = 0; i < schema.fields().size(); i++) {
                Field field = schema.fields().get(i);
                if (field.name().equalsIgnoreCase(fieldName)) {
                    return  i;
                }
            }
        }
        return -1;
    }


}
