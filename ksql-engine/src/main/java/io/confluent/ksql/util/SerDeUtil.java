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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;



public class SerDeUtil {

  public static Serde<GenericRow> getRowSerDe(final KsqlTopicSerDe topicSerDe, Schema schema,
                                              KsqlConfig ksqlConfig, boolean isInternal) {
    return topicSerDe.getGenericRowSerde(schema, ksqlConfig, isInternal,
                                         KsqlEngine.getSchemaRegistryClient());
  }

  public static Schema getSchemaFromAvro(String avroSchemaString) {
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = parser.parse(avroSchemaString);

    SchemaBuilder inferredSchema = SchemaBuilder.struct().name(avroSchema.getName());
    for (org.apache.avro.Schema.Field avroField: avroSchema.getFields()) {
      inferredSchema.field(avroField.name(), getKSQLSchemaForAvroSchema(avroField.schema()));
    }

    return inferredSchema.build();
  }

  private static Schema getKSQLSchemaForAvroSchema(org.apache.avro.Schema avroSchema) {
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
        return SchemaBuilder.array(getKSQLSchemaForAvroSchema(avroSchema.getElementType()));
      case MAP:
        return SchemaBuilder.map(Schema.STRING_SCHEMA,
                                 getKSQLSchemaForAvroSchema(avroSchema.getValueType()));
      default:
        throw new KsqlException(String.format("Cannot find correct type for avro type: %s",
                                              avroSchema.getFullName()));
    }
  }

}
