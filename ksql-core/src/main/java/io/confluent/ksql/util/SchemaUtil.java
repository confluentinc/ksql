package io.confluent.ksql.util;


import org.apache.kafka.connect.data.Schema;

public class SchemaUtil {

    public Schema getTypeSchema(Schema.Type type) {
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
}
