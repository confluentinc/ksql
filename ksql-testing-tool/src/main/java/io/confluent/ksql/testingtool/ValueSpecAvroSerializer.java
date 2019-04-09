/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.testingtool;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

public class ValueSpecAvroSerializer implements Serializer<Object> {
  private final SchemaRegistryClient schemaRegistryClient;
  private final KafkaAvroSerializer avroSerializer;

  ValueSpecAvroSerializer(final SchemaRegistryClient schemaRegistryClient) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(final Map<String, ?> properties, final boolean b) {
  }

  @Override
  public byte[] serialize(final String topicName, final Object spec) {
    final String schemaString;
    try {
      schemaString = schemaRegistryClient.getLatestSchemaMetadata(
          topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX).getSchema();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    final Object avroObject = valueSpecToAvro(
        spec,
        new org.apache.avro.Schema.Parser().parse(schemaString));
    return avroSerializer.serialize(topicName, avroObject);
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @SuppressWarnings("unchecked")
  private static Object valueSpecToAvro(final Object spec, final org.apache.avro.Schema schema) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (spec == null) {
      return null;
    }
    switch (schema.getType()) {
      case INT:
        return Integer.valueOf(spec.toString());
      case LONG:
        return Long.valueOf(spec.toString());
      case STRING:
        return spec.toString();
      case DOUBLE:
        return Double.valueOf(spec.toString());
      case FLOAT:
        return Float.valueOf(spec.toString());
      case BOOLEAN:
        return spec;
      case ARRAY:
        return ((List) spec).stream()
            .map(o -> valueSpecToAvro(o, schema.getElementType()))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<Object, Object>) spec).entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> valueSpecToAvro(e.getValue(), schema.getValueType())
            )
        );
      case RECORD:
        final GenericRecord record = new GenericData.Record(schema);
        for (final org.apache.avro.Schema.Field field : schema.getFields()) {
          record.put(
              field.name(),
              valueSpecToAvro(((Map<String, ?>) spec).get(field.name()), field.schema())
          );
        }
        return record;
      case UNION:
        for (final org.apache.avro.Schema memberSchema : schema.getTypes()) {
          if (!memberSchema.getType().equals(org.apache.avro.Schema.Type.NULL)) {
            return valueSpecToAvro(spec, memberSchema);
          }
        }
        throw new RuntimeException("Union must have non-null type: " + schema.getType().getName());

      default:
        throw new RuntimeException(
            "This test does not support the data type yet: " + schema.getType().getName());
    }
  }
}