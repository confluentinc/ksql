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

package io.confluent.ksql.test.serde.avro;

import com.google.common.collect.Maps;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.ValueSpec;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlPreconditions;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
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


  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private static final class ValueSpecAvroSerializer implements Serializer<Object> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

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
      if (spec == null) {
        return null;
      }

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
        case BYTES:
          final LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
          if (logicalType instanceof Decimal) {
            return new DecimalConversion()
                .toBytes(new BigDecimal(spec.toString()), schema, logicalType);
          }
          throw new KsqlException("Unexpected data type seen in schema: " + schema);
        case STRING:
          return spec.toString();
        case DOUBLE:
          return Double.valueOf(spec.toString());
        case FLOAT:
          return Float.valueOf(spec.toString());
        case BOOLEAN:
          return spec;
        case ARRAY:
          // An array of map entries will be deserialized as a Map.
          // Check https://github.com/confluentinc/schema-registry/blob/master/avro-converter/src/main/java/io/confluent/connect/avro/AvroData.java
          // for more details
          if (schema.getElementType().getName().equals(AvroData.MAP_ENTRY_TYPE_NAME)
              ||
              Objects.equals(
                  schema.getElementType().getProp(AvroData.CONNECT_INTERNAL_TYPE_NAME),
                  AvroData.MAP_ENTRY_TYPE_NAME)
              ) {
            return ((Map<Object, Object>) spec).entrySet().stream()
                .map(objectObjectEntry ->
                    getAvroRecordForMapEntry(objectObjectEntry, schema.getElementType()))
                .collect(Collectors.toList());
          }
          final List<?> list = ((List<?>) spec).stream()
              .map(o -> valueSpecToAvro(o, schema.getElementType()))
              .collect(Collectors.toList());

          return new GenericData.Array<>(schema, list);
        case MAP:
          final Map<Object, Object> map = new HashMap<>();
          ((Map<Object, Object>) spec)
              .forEach((k, v) -> map.put(k, valueSpecToAvro(v, schema.getValueType())));

          return new GenericMap(schema, map);
        case RECORD:
          return getAvroRecord(spec, schema);
        case UNION:
          return valueSpecUnionToAvro(spec, schema);
        default:
          throw new RuntimeException(
              "This test does not support the data type yet: " + schema.getType().getName());
      }
    }

    @SuppressWarnings("unchecked")
    private static Object valueSpecUnionToAvro(
        final Object spec,
        final org.apache.avro.Schema schema
    ) {
      // If the schema has two types and one is null, serialize the non null
      if (schema.getTypes().size() == 2) {
        if (schema.getTypes().get(0).getType() == org.apache.avro.Schema.Type.NULL) {
          return valueSpecToAvro(spec, schema.getTypes().get(1));
        } else {
          return valueSpecToAvro(spec, schema.getTypes().get(0));
        }
      }
      // if the schema has more than one non null type, avro serializes it as a map
      for (final org.apache.avro.Schema memberSchema : schema.getTypes()) {
        if (!memberSchema.getType().equals(org.apache.avro.Schema.Type.NULL)) {
          final String typeName = memberSchema.getType().getName().toUpperCase();
          final Object val = ((Map<String, ?>) spec).get(typeName);
          if (val != null) {
            return valueSpecToAvro(val, memberSchema);
          }
        }
      }
      throw new RuntimeException("Union must have non-null type: "
          + schema.getType().getName());
    }

    @SuppressWarnings("unchecked")
    private static GenericRecord getAvroRecord(final Object spec, final Schema schema) {
      final GenericRecord record = new GenericData.Record(schema);
      final Map<String, String> caseInsensitiveFieldNames
          = getUppercaseKeyToActualKey((Map) spec);
      for (final org.apache.avro.Schema.Field field : schema.getFields()) {
        record.put(
            field.name(),
            valueSpecToAvro(((Map<String, ?>) spec)
                .get(caseInsensitiveFieldNames.get(field.name().toUpperCase())), field.schema())
        );
      }
      return record;
    }

    @SuppressWarnings("unchecked")
    // A map entry will be serialized as an avro object with two fields, key and value.
    private static GenericRecord getAvroRecordForMapEntry(
        final Map.Entry<?, ?> spec,
        final Schema schema) {
      final GenericRecord record = new GenericData.Record(schema);
      record.put(AvroData.KEY_FIELD,
          valueSpecToAvro(spec.getKey(), schema.getField(AvroData.KEY_FIELD).schema()));
      record.put(AvroData.VALUE_FIELD,
          valueSpecToAvro(spec.getValue(), schema.getField(AvroData.VALUE_FIELD).schema()));
      return record;
    }

    private static class GenericMap
        extends AbstractMap<Object, Object>
        implements GenericContainer {

      private final Schema schema;
      private final Map<Object, Object> map;

      GenericMap(final Schema schema, final Map<Object, Object> map) {
        this.schema = Objects.requireNonNull(schema, "schema");
        this.map = Objects.requireNonNull(map, "map");
      }

      @Override
      public Schema getSchema() {
        return schema;
      }

      @Nonnull
      @Override
      public Set<Entry<Object, Object>> entrySet() {
        return map.entrySet();
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
      if (data == null) {
        return null;
      }

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
        case BYTES:
          final LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
          KsqlPreconditions.checkArgument(logicalType instanceof Decimal,
              "BYTES must be of DECIMAL type");
          KsqlPreconditions.checkArgument(avro instanceof ByteBuffer,
              "BYTES must be ByteBuffer, got " + avro.getClass());
          return new DecimalConversion().fromBytes((ByteBuffer) avro, schema, logicalType);
        case ARRAY:
          // Since Connect serializes maps as an array of MapEntries to support maps with non
          // string keys, if the array element is MapEntry type we should deserialize it as
          // a map!
          // Check https://github.com/confluentinc/schema-registry/blob/master/avro-converter/src/main/java/io/confluent/connect/avro/AvroData.java
          // for more details
          if (schema.getElementType().getName().equals(AvroData.MAP_ENTRY_TYPE_NAME)
              ||
              Objects.equals(
                  schema.getElementType().getProp(AvroData.CONNECT_INTERNAL_TYPE_NAME),
                  AvroData.MAP_ENTRY_TYPE_NAME)
              ) {
            final org.apache.avro.Schema valueSchema
                = schema.getElementType().getField(AvroData.VALUE_FIELD).schema();
            final Map<String, Object> map = new HashMap<>();
            ((List<GenericData.Record>) avro).forEach(e -> map.put(
                e.get(AvroData.KEY_FIELD).toString(),
                avroToValueSpec(e.get(AvroData.VALUE_FIELD), valueSchema, toUpper)
            ));
            return map;
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
          return avroUnionToValueSpec(avro, schema, toUpper);
        default:
          throw new RuntimeException("Test cannot handle data of type: " + schema.getType());
      }
    }

    private static Object avroUnionToValueSpec(
        final Object avroUnion,
        final org.apache.avro.Schema schema,
        final boolean toUpper
    ) {
      final int pos = GenericData.get().resolveUnion(schema, avroUnion);
      final boolean hasNull = schema.getTypes().stream()
          .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
      final Object resolved = avroToValueSpec(avroUnion, schema.getTypes().get(pos), toUpper);
      // If there are two types and one is NULL, just return the resolved object
      if (schema.getTypes().get(pos).getType().equals(org.apache.avro.Schema.Type.NULL)
          || schema.getTypes().size() == 2 && hasNull) {
        return resolved;
      }
      // If there are more than two non NULL types
      final Map<String, Object> ret = Maps.newHashMap();
      schema.getTypes()
          .forEach(
              s -> ret.put(s.getName().toUpperCase(), null));
      ret.put(schema.getTypes().get(pos).getName().toUpperCase(), resolved);
      return ret;
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> getUppercaseKeyToActualKey(final Map<String, ?> record) {
    return record.entrySet().stream().collect(Collectors.toMap(
        entry -> entry.getKey().toUpperCase(),
        Entry::getKey));
  }
} 