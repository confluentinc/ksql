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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;

final class ProducerFactory {

  private static final GenericKeySerDe KEY_SERDE_FACTORY = new GenericKeySerDe();
  private static final GenericRowSerDe VALUE_SERDE_FACTORY = new GenericRowSerDe();

  private ProducerFactory() {
  }

  static DataGenProducer getProducer(
      final Format keyFormat,
      final Format valueFormat,
      final Properties props
  ) {
    final KsqlConfig ksqlConfig = new KsqlConfig(props);

    final Optional<SchemaRegistryClient> srClient = SchemaRegistryClientFactory
        .getSrClient(keyFormat, valueFormat, ksqlConfig);

    final SerializerFactory<Struct> keySerializerFactory =
        keySerializerFactory(keyFormat, ksqlConfig, srClient);

    final SerializerFactory<GenericRow> valueSerializerFactory =
        valueSerializerFactory(valueFormat, ksqlConfig, srClient);

    return new DataGenProducer(
        keySerializerFactory,
        valueSerializerFactory
    );
  }

  private static SerializerFactory<Struct> keySerializerFactory(
      final Format keyFormat,
      final KsqlConfig ksqlConfig,
      final Optional<SchemaRegistryClient> srClient
  ) {
    return new SerializerFactory<Struct>() {
      @Override
      public Format format() {
        return keyFormat;
      }

      @Override
      public Serializer<Struct> create(final PersistenceSchema schema) {
        return KEY_SERDE_FACTORY.create(
            FormatInfo.of(keyFormat),
            schema,
            ksqlConfig,
            srClient::get,
            "",
            NoopProcessingLogContext.INSTANCE
        ).serializer();
      }
    };
  }

  private static SerializerFactory<GenericRow> valueSerializerFactory(
      final Format valueFormat,
      final KsqlConfig ksqlConfig,
      final Optional<SchemaRegistryClient> srClient
  ) {
    return new SerializerFactory<GenericRow>() {
      @Override
      public Format format() {
        return valueFormat;
      }

      @Override
      public Serializer<GenericRow> create(final PersistenceSchema schema) {
        return VALUE_SERDE_FACTORY.create(
            FormatInfo.of(valueFormat),
            schema,
            ksqlConfig,
            srClient::get,
            "",
            NoopProcessingLogContext.INSTANCE
        ).serializer();
      }
    };
  }
}
