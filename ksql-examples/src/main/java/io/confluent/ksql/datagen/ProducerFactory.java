/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.datagen;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactories;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

final class ProducerFactory {

  private static final KsqlSerdeFactories SERDE_FACTORIES = new KsqlSerdeFactories();

  private ProducerFactory() {
  }

  static DataGenProducer getProducer(
      final Format keyFormat,
      final Format valueFormat,
      final Properties props
  ) {
    final KsqlSerdeFactory keyFactory = createSerdeFactory(keyFormat);
    final KsqlSerdeFactory valueFactory = createSerdeFactory(valueFormat);

    final Optional<SchemaRegistryClient> srClient = SchemaRegistryClientFactory
        .getSrClient(keyFormat, valueFormat, props);

    return new DataGenProducer(keyFactory, valueFactory, srClient::get);
  }

  private static KsqlSerdeFactory createSerdeFactory(final Format format) {
    final KsqlSerdeFactory structSerdeFactory = SERDE_FACTORIES.create(format, Optional.empty());

    if (format.supportsUnwrapping()) {
      return structSerdeFactory;
    }

    return new UnwrappedSerdeFactory(structSerdeFactory);
  }

  private static class UnwrappedSerdeFactory implements KsqlSerdeFactory {

    private final KsqlSerdeFactory delegate;

    UnwrappedSerdeFactory(final KsqlSerdeFactory delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Format getFormat() {
      return delegate.getFormat();
    }

    @Override
    public void validate(final PersistenceSchema schema) {
      delegate.validate(schema);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Serde<Object> createSerde(
        final PersistenceSchema schema,
        final KsqlConfig ksqlConfig,
        final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
    ) {
      final Serde<Object> serde = delegate
          .createSerde(schema, ksqlConfig, schemaRegistryClientFactory);

      final Serializer serializer = serde.serializer();
      return Serdes.serdeFrom(
          new WrappedSerializer((Serializer<Struct>)serializer, schema.getConnectSchema()),
          serde.deserializer()
      );
    }
  }

  private static class WrappedSerializer implements Serializer<Object> {

    private final Serializer<Struct> inner;
    private final ConnectSchema schema;
    private final Field field;

    WrappedSerializer(final Serializer<Struct> inner, final ConnectSchema schema) {
      this.inner = requireNonNull(inner, "inner");
      this.schema = requireNonNull(schema, "schema");
      this.field = schema.fields().get(0);

      if (schema.fields().size() != 1) {
        throw new IllegalArgumentException("Expecting only single field");
      }
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final Object data) {
      if (data == null) {
        return inner.serialize(topic, null);
      }

      final Struct struct = new Struct(schema);
      struct.put(field, data);

      return inner.serialize(topic, struct);
    }
  }
}
