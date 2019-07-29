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

package io.confluent.ksql.serde;

import static io.confluent.ksql.logging.processing.ProcessingLoggerUtil.join;
import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.LoggingDeserializer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public final class GenericRowSerDe {

  private static final String DESERIALIZER_LOGGER_NAME = "deserializer";

  private GenericRowSerDe() {
  }

  @SuppressWarnings("unchecked")
  public static Serde<GenericRow> from(
      final KsqlSerdeFactory serdeFactory,
      final PhysicalSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    final ProcessingLogger processingLogger = processingLogContext.getLoggerFactory()
        .getLogger(join(loggerNamePrefix, DESERIALIZER_LOGGER_NAME));

    final Serde<Object> innerSerde = serdeFactory.createSerde(
        schema.valueSchema(),
        ksqlConfig,
        srClientFactory
    );

    final boolean unwrapped = schema.logicalSchema().valueFields().size() == 1
        && schema.serdeOptions().contains(SerdeOption.UNWRAP_SINGLE_VALUES);

    final Serializer<GenericRow> serializer = unwrapped
        ? new UnwrappedGenericRowSerializer(innerSerde.serializer())
        : new GenericRowSerializer(innerSerde.serializer(), schema.logicalSchema());

    final Deserializer<GenericRow> deserializer = unwrapped
        ? new UnwrappedGenericRowDeserializer(innerSerde.deserializer())
        : new GenericRowDeserializer((Deserializer) innerSerde.deserializer());

    final Serde<GenericRow> genericRowSerde = Serdes.serdeFrom(
        serializer,
        new LoggingDeserializer(deserializer, processingLogger)
    );

    genericRowSerde.configure(Collections.emptyMap(), false);

    return genericRowSerde;
  }

  private static class UnwrappedGenericRowSerializer implements Serializer<GenericRow> {

    private final Serializer<Object> inner;

    UnwrappedGenericRowSerializer(final Serializer<Object> inner) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final GenericRow data) {
      if (data == null) {
        return inner.serialize(topic, null);
      }

      if (data.getColumns().size() != 1) {
        throw new SerializationException("Expected single-field value. "
            + "got: " + data.getColumns().size());
      }

      return inner.serialize(topic, data.getColumns().get(0));
    }
  }

  private static class GenericRowSerializer implements Serializer<GenericRow> {

    private final Serializer<Object> inner;
    private final ConnectSchema schema;

    GenericRowSerializer(final Serializer<Object> inner, final LogicalSchema schema) {
      this.inner = requireNonNull(inner, "inner");
      this.schema = requireNonNull(schema, "schema").valueSchema();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final GenericRow data) {
      if (data == null) {
        return inner.serialize(topic, null);
      }

      if (data.getColumns().size() != schema.fields().size()) {
        throw new SerializationException("Field count mismatch."
            + " expected: " + schema.fields().size()
            + ", got: " + data.getColumns().size()
        );
      }

      final Struct struct = new Struct(schema);
      for (int i = 0; i < data.getColumns().size(); i++) {
        struct.put(schema.fields().get(i), data.getColumns().get(i));
      }

      return inner.serialize(topic, struct);
    }
  }

  private static class GenericRowDeserializer implements Deserializer<GenericRow> {

    private final Deserializer<Struct> inner;

    GenericRowDeserializer(final Deserializer<Struct> inner) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public GenericRow deserialize(final String topic, final byte[] data) {
      final Struct struct = inner.deserialize(topic, data);
      if (struct == null) {
        return null;
      }

      final List<Field> fields = struct.schema().fields();
      final List<Object> columns = new ArrayList<>(fields.size());

      for (final Field field : fields) {
        final Object columnVal = struct.get(field);
        columns.add(columnVal);
      }

      return new GenericRow(columns);
    }
  }

  private static class UnwrappedGenericRowDeserializer implements Deserializer<GenericRow> {

    private final Deserializer<Object> inner;

    UnwrappedGenericRowDeserializer(final Deserializer<Object> inner) {
      this.inner = requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public GenericRow deserialize(final String topic, final byte[] data) {
      final Object value = inner.deserialize(topic, data);
      if (value == null) {
        return null;
      }

      final GenericRow row = new GenericRow();
      row.getColumns().add(value);
      return row;
    }
  }
}
