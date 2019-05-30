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

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.schema.ksql.KsqlSchemaWithOptions;
import io.confluent.ksql.schema.persistence.PersistenceSchemas;
import io.confluent.ksql.util.KsqlConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public final class GenericRowSerDe implements Serde<GenericRow> {

  private final Serde<Object> delegate;
  private final KsqlSchemaWithOptions schema;
  private final boolean unwrapped;

  public static Serde<GenericRow> from(
      final KsqlSerdeFactory serdeFactory,
      final KsqlSchemaWithOptions schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    final PersistenceSchemas physicalSchema = schema.getPhysicalSchema();

    final Serde<Object> structSerde = serdeFactory.createSerde(
        physicalSchema.valueSchema(),
        ksqlConfig,
        srClientFactory,
        loggerNamePrefix,
        processingLogContext);

    return new GenericRowSerDe(structSerde, schema);
  }

  private GenericRowSerDe(
      final Serde<Object> delegate,
      final KsqlSchemaWithOptions schema
  ) {
    this.delegate = requireNonNull(delegate, "delegate");
    this.schema = requireNonNull(schema, "schema");
    this.unwrapped = schema.getLogicalSchema().fields().size() == 1
        && schema.getSerdeOptions().contains(SerdeOption.UNWRAP_SINGLE_VALUES);
  }

  @Override
  public Serializer<GenericRow> serializer() {
    return unwrapped
        ? new UnwrappedGenericRowSerializer(delegate.serializer())
        : new GenericRowSerializer(delegate.serializer(), schema.getLogicalSchema());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Deserializer<GenericRow> deserializer() {
    return unwrapped
        ? new UnwrappedGenericRowDeserializer(delegate.deserializer())
        : new GenericRowDeserializer((Deserializer) delegate.deserializer());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GenericRowSerDe that = (GenericRowSerDe) o;
    return Objects.equals(delegate, that.delegate)
        && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate, schema);
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

    GenericRowSerializer(final Serializer<Object> inner, final KsqlSchema schema) {
      this.inner = requireNonNull(inner, "inner");
      this.schema = requireNonNull(schema, "schema").getSchema();
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
