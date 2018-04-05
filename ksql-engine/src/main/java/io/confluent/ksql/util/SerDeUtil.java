/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.List;


public class SerDeUtil {


  public static Schema getSchemaFromAvro(String avroSchemaString) {
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = parser.parse(avroSchemaString);

    SchemaBuilder inferredSchema = SchemaBuilder.struct().name(avroSchema.getName());
    for (org.apache.avro.Schema.Field avroField: avroSchema.getFields()) {
      inferredSchema.field(avroField.name(), getKsqlSchemaForAvroSchema(avroField.schema()));
    }

    return inferredSchema.build();
  }

  private static Schema getKsqlSchemaForAvroSchema(org.apache.avro.Schema avroSchema) {
    switch (avroSchema.getType()) {
      case INT:
        return Schema.INT32_SCHEMA;
      case LONG:
        return Schema.INT64_SCHEMA;
      case DOUBLE:
      case FLOAT:
        return Schema.FLOAT64_SCHEMA;
      case BOOLEAN:
        return Schema.BOOLEAN_SCHEMA;
      case STRING:
        return Schema.STRING_SCHEMA;
      case ARRAY:
        return SchemaBuilder.array(getKsqlSchemaForAvroSchema(avroSchema.getElementType()));
      case MAP:
        return SchemaBuilder.map(Schema.STRING_SCHEMA,
                                 getKsqlSchemaForAvroSchema(avroSchema.getValueType()));
      case UNION:
        return handleUnion(avroSchema);
        
      default:
        throw new KsqlException(String.format("KSQL doesn't currently support Avro type: %s",
                                              avroSchema.getFullName()));
    }
  }

  private static Schema handleUnion(org.apache.avro.Schema avroSchema) {
    List<org.apache.avro.Schema> schemaList = avroSchema.getTypes();
    if (schemaList.size() == 1) {
      return getKsqlSchemaForAvroSchema(schemaList.get(0));
    } else if (schemaList.size() == 2) {
      if (schemaList.get(0).getType() == org.apache.avro.Schema.Type.NULL) {
        return getKsqlSchemaForAvroSchema(schemaList.get(1));
      } else if (schemaList.get(1).getType() == org.apache.avro.Schema.Type.NULL) {
        return getKsqlSchemaForAvroSchema(schemaList.get(0));
      }
    }
    throw new KsqlException("Union type cannot have more than two types and "
                                          + "one of them should be null.");
  }

}
