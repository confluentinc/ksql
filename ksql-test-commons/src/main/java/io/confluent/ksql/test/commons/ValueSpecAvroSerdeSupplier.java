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

package io.confluent.ksql.test.commons;

import com.google.common.collect.Maps;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.util.KsqlConstants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class ValueSpecAvroSerdeSupplier implements SerdeSupplier<Object> {
  @Override
  public Serializer<Object> getSerializer(final SchemaRegistryClient schemaRegistryClient) {
    return new ValueSpecAvroSerializer(schemaRegistryClient);
  }

  @Override
  public Deserializer<Object> getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
    return new ValueSpecAvroDeserializer(schemaRegistryClient);
  }


  private static final class ValueSpecAvroSerializer implements Serializer<Object> {
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
          throw new RuntimeException("Union must have non-null type: "
              + schema.getType().getName());

        default:
          throw new RuntimeException(
              "This test does not support the data type yet: " + schema.getType().getName());
      }
    }
  }

  private static final class ValueSpecAvroDeserializer implements Deserializer<Object> {

    private final SchemaRegistryClient schemaRegistryClient;
    private final KafkaAvroDeserializer avroDeserializer;

    ValueSpecAvroDeserializer(final SchemaRegistryClient schemaRegistryClient) {
      this.schemaRegistryClient = schemaRegistryClient;
      this.avroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> properties, final boolean b) {
    }

    @Override
    public Object deserialize(final String topicName, final byte[] data) {
      final Object avroObject = avroDeserializer.deserialize(topicName, data);
      final String schemaString;
      try {
        schemaString = schemaRegistryClient.getLatestSchemaMetadata(
            topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX).getSchema();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      return new ValueSpec(
          avroToValueSpec(
              avroObject,
              new org.apache.avro.Schema.Parser().parse(schemaString),
              false));
    }

    // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
    @SuppressWarnings("unchecked")
    private static Object avroToValueSpec(final Object avro,
        final org.apache.avro.Schema schema,
        final boolean toUpper) {
      // CHECKSTYLE_RULES.ON: CyclomaticComplexity
      if (avro == null) {
        return null;
      }
      switch (schema.getType()) {
        case INT:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
          return avro;
        case LONG:
          // Ensure that smaller long values match the value spec from the test file.
          // The json deserializer uses Integer for any number less than Integer.MAX_VALUE.
          if (((Long) avro) < Integer.MAX_VALUE && ((Long) avro) > Integer.MIN_VALUE) {
            return ((Long) avro).intValue();
          }
          return avro;
        case ENUM:
        case STRING:
          return avro.toString();
        case ARRAY:
          if (schema.getElementType().getName().equals(AvroData.MAP_ENTRY_TYPE_NAME)
              ||
              Objects.equals(
                  schema.getElementType().getProp(AvroData.CONNECT_INTERNAL_TYPE_NAME),
                  AvroData.MAP_ENTRY_TYPE_NAME)
              ) {
            final org.apache.avro.Schema valueSchema
                = schema.getElementType().getField("value").schema();
            return ((List) avro).stream().collect(
                Collectors.toMap(
                    m -> ((GenericData.Record) m).get("key").toString(),
                    m -> (avroToValueSpec(((GenericData.Record) m).get("value"),
                        valueSchema,
                        toUpper))
                )
            );
          }
          return ((List) avro).stream()
              .map(o -> avroToValueSpec(o, schema.getElementType(), toUpper))
              .collect(Collectors.toList());
        case MAP:
          return ((Map<Object, Object>) avro).entrySet().stream().collect(
              Collectors.toMap(
                  e -> e.getKey().toString(),
                  e -> avroToValueSpec(e.getValue(), schema.getValueType(), toUpper)
              )
          );
        case RECORD:
          final Map<String, Object> recordSpec = new HashMap<>();
          schema.getFields().forEach(
              f -> recordSpec.put(
                  toUpper ? f.name().toUpperCase() : f.name(),
                  avroToValueSpec(
                      ((GenericData.Record) avro).get(f.name()),
                      f.schema(),
                      toUpper)
              )
          );
          return recordSpec;
        case UNION:
          final int pos = GenericData.get().resolveUnion(schema, avro);
          final boolean hasNull = schema.getTypes().stream()
              .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
          final Object resolved = avroToValueSpec(avro, schema.getTypes().get(pos), toUpper);
          if (schema.getTypes().get(pos).getType().equals(org.apache.avro.Schema.Type.NULL)
              || schema.getTypes().size() == 2 && hasNull) {
            return resolved;
          }
          final Map<String, Object> ret = Maps.newHashMap();
          schema.getTypes()
              .forEach(
                  s -> ret.put(s.getName().toUpperCase(), null));
          ret.put(schema.getTypes().get(pos).getName().toUpperCase(), resolved);
          return ret;
        default:
          throw new RuntimeException("Test cannot handle data of type: " + schema.getType());
      }
    }
  }
}