/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.serde;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.storage.Converter;

/**
 * A general purpose serde supplier that works with any {@link io.confluent.ksql.serde.Format}
 * that can provide a {@link Converter} and supports Confluent Schema Registry
 * integration.
 */
public abstract class ConnectSerdeSupplier<T extends ParsedSchema>
    implements SerdeSupplier<Object> {

  private final Function<SchemaRegistryClient, Converter> converterFactory;

  protected ConnectSerdeSupplier(
      final Function<SchemaRegistryClient, Converter> converterFactory
  ) {
    this.converterFactory = Objects.requireNonNull(converterFactory, "converterFactory");
  }

  @Override
  public Serializer<Object> getSerializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new SpecSerializer(schemaRegistryClient, isKey);
  }

  @Override
  public Deserializer<Object> getDeserializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new SpecDeserializer(schemaRegistryClient, isKey);
  }

  protected abstract Schema fromParsedSchema(T schema);

  private final class SpecSerializer implements Serializer<Object> {

    private final SchemaRegistryClient srClient;
    private final Converter converter;
    private final boolean isKey;

    SpecSerializer(final SchemaRegistryClient srClient, final boolean isKey) {
      this.srClient = Objects.requireNonNull(srClient, "srClient");
      this.converter = converterFactory.apply(srClient);
      converter.configure(
          ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "foo"),
          isKey
      );
      this.isKey = isKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(final String topic, final Object spec) {
      if (spec == null) {
        return null;
      }

      final T schema;
      try {
        final String subject = KsqlConstants.getSRSubject(topic, isKey);
        final int id = srClient.getLatestSchemaMetadata(subject).getId();
        schema = (T) srClient.getSchemaBySubjectAndId(subject, id);
      } catch (Exception e) {
        throw new KsqlException(e);
      }

      final Schema connectSchema = fromParsedSchema(schema);
      return converter.fromConnectData(
          topic,
          connectSchema,
          specToConnect(spec, connectSchema)
      );
    }

    // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
    private Object specToConnect(final Object spec, final Schema schema) {
      // CHECKSTYLE_RULES.ON: CyclomaticComplexity
      if (spec == null) {
        return null;
      }

      switch (schema.type()) {
        case INT32:
          return Integer.valueOf(spec.toString());
        case INT64:
          final Long longVal = Long.valueOf(spec.toString());
          if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return new java.sql.Timestamp(longVal);
          }
          return longVal;
        case FLOAT32:
          return Float.valueOf(spec.toString());
        case FLOAT64:
          return Double.valueOf(spec.toString());
        case BOOLEAN:
          return Boolean.valueOf(spec.toString());
        case STRING:
          return spec.toString();
        case ARRAY:
          return ((List<?>) spec)
              .stream()
              .map(el -> specToConnect(el, schema.valueSchema()))
              .collect(Collectors.toList());
        case MAP:
          return ((Map<?, ?>) spec)
              .entrySet()
              .stream()
              // cannot use Collectors.toMap due to JDK bug:
              // https://bugs.openjdk.java.net/browse/JDK-8148463
              .collect(
                  HashMap::new,
                  ((map, v) -> map.put(
                      specToConnect(v.getKey(), schema.keySchema()),
                      specToConnect(v.getValue(), schema.valueSchema()))),
                  HashMap::putAll
              );
        case STRUCT:
          final Map<String, String> caseInsensitiveFieldMap = schema.fields()
              .stream()
              .collect(Collectors.toMap(
                  f -> f.name().toUpperCase(),
                  Field::name
              ));

          final Struct struct = new Struct(schema);
          ((Map<?, ?>) spec)
              .forEach((key, value) -> {
                final String realKey = caseInsensitiveFieldMap.get(key.toString().toUpperCase());
                if (realKey != null) {
                  struct.put(realKey, specToConnect(value, schema.field(realKey).schema()));
                }
              });
          return struct;
        case BYTES:
          if (DecimalUtil.isDecimal(schema)) {
            if (spec instanceof BigDecimal) {
              return DecimalUtil.ensureFit((BigDecimal) spec, schema);
            }

            if (spec instanceof String) {
              // Supported for legacy reasons...
              return DecimalUtil.cast(
                  (String) spec,
                  DecimalUtil.precision(schema),
                  DecimalUtil.scale(schema));
            }

            throw new TestFrameworkException("DECIMAL type requires JSON number in test data");
          }
          throw new RuntimeException("Unexpected BYTES type " + schema.name());
        default:
          throw new RuntimeException(
              "This test does not support the data type yet: " + schema.type());
      }
    }
  }

  private class SpecDeserializer implements Deserializer<Object> {

    private final Converter converter;

    SpecDeserializer(final SchemaRegistryClient srClient, final boolean isKey) {
      this.converter = converterFactory.apply(srClient);
      converter.configure(
          ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "foo"),
          isKey
      );
    }

    @Override
    public Object deserialize(final String topic, final byte[] bytes) {
      if (bytes == null) {
        return null;
      }

      final SchemaAndValue schemaAndValue = converter.toConnectData(topic, bytes);
      return connectToSpec(schemaAndValue.value(), schemaAndValue.schema(), false);
    }

    // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
    private Object connectToSpec(
        final Object data,
        final Schema schema,
        final boolean toUpper
    ) {
      // CHECKSTYLE_RULES.ON: CyclomaticComplexity
      if (data == null) {
        return null;
      }

      switch (schema.type()) {
        case INT64:
          if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return Timestamp.fromLogical(schema, (Date) data);
          }
          return data;
        case INT32:
        case FLOAT32:
        case FLOAT64:
        case BOOLEAN:
          return data;
        case STRING:
          return data.toString();
        case ARRAY:
          return ((List<?>) data)
              .stream()
              .map(v -> connectToSpec(v, schema.valueSchema(), toUpper))
              .collect(Collectors.toList());
        case MAP:
          final Map<String, Object> map = new HashMap<>();
          ((Map<?, ?>) data)
              .forEach((k, v) -> map.put(
                  k.toString(),
                  connectToSpec(v, schema.valueSchema(), toUpper)));
          return map;
        case STRUCT:
          final Map<String, Object> recordSpec = new HashMap<>();
          schema.fields()
              .forEach(f -> recordSpec.put(
                  toUpper ? f.name().toUpperCase() : f.name(),
                  connectToSpec(((Struct) data).get(f.name()), f.schema(), toUpper)));
          return recordSpec;
        case BYTES:
          if (DecimalUtil.isDecimal(schema)) {
            if (data instanceof BigDecimal) {
              return data;
            }
          }
          throw new RuntimeException("Unexpected BYTES type " + schema.name());
        default:
          throw new RuntimeException("Test cannot handle data of type: " + schema.type());
      }
    }

  }


}
