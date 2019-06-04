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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.persistence.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public final class GenericRowSerDe implements Serde<GenericRow> {

  private final Serde<Object> delegate;
  private final Schema schema;

  public static Serde<GenericRow> from(
      final KsqlSerdeFactory serdeFactory,
      final Schema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {

    final Serde<Object> structSerde = serdeFactory.createSerde(
        PersistenceSchema.of((ConnectSchema) schema),
        ksqlConfig,
        srClientFactory,
        loggerNamePrefix,
        processingLogContext);

    return new GenericRowSerDe(structSerde, schema);
  }

  private GenericRowSerDe(
      final Serde<Object> delegate,
      final Schema schema
  ) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  @Override
  public Serializer<GenericRow> serializer() {
    return new GenericRowSerializer(delegate.serializer(), schema);
  }

  @Override
  public Deserializer<GenericRow> deserializer() {
    return new GenericRowDeserializer(delegate.deserializer());
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

  public static class GenericRowSerializer implements Serializer<GenericRow> {

    private final Serializer<Object> inner;
    private final Schema schema;

    public GenericRowSerializer(final Serializer<Object> inner, final Schema schema) {
      this.inner = Objects.requireNonNull(inner, "inner");
      this.schema = Objects.requireNonNull(schema, "schema");
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

      final Struct struct = new Struct(schema);
      for (int i = 0; i < data.getColumns().size(); i++) {
        struct.put(schema.fields().get(i), data.getColumns().get(i));
      }

      return inner.serialize(topic, struct);
    }
  }

  public static class GenericRowDeserializer implements Deserializer<GenericRow> {

    private final Deserializer<Object> inner;

    public GenericRowDeserializer(final Deserializer<Object> inner) {
      this.inner = Objects.requireNonNull(inner, "inner");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public GenericRow deserialize(final String topic, final byte[] data) {
      final Struct struct = (Struct) inner.deserialize(topic, data);
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
}

