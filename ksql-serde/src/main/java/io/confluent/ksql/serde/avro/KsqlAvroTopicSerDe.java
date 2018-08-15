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

package io.confluent.ksql.serde.avro;

import com.google.common.collect.ImmutableMap;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.connect.KsqlConnectDeserializer;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalDeserializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;

import java.util.function.Supplier;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;


public class KsqlAvroTopicSerDe extends KsqlTopicSerDe {

  public KsqlAvroTopicSerDe() {
    super(DataSource.DataSourceSerDe.AVRO);
  }

  private AvroConverter getAvroConverter(
      final SchemaRegistryClient schemaRegistryClient, final KsqlConfig ksqlConfig) {
    final AvroConverter avroConverter = new AvroConverter(schemaRegistryClient);
    avroConverter.configure(
        ImmutableMap.of(
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY),
            AvroDataConfig.CONNECT_META_DATA_CONFIG,
            false
        ),
        false);
    return avroConverter;
  }

  @Override
  public Serde<GenericRow> getGenericRowSerde(
      final Schema schemaMaybeWithSource,
      final KsqlConfig ksqlConfig,
      final boolean isInternal,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory) {
    final Schema schema = isInternal
        ? schemaMaybeWithSource : SchemaUtil.getSchemaWithNoAlias(schemaMaybeWithSource);
    final Serializer<GenericRow> genericRowSerializer = new ThreadLocalSerializer(
        () -> new KsqlConnectSerializer(
            new AvroDataTranslator(schema),
            getAvroConverter(schemaRegistryClientFactory.get(), ksqlConfig)));
    final Deserializer<GenericRow> genericRowDeserializer = new ThreadLocalDeserializer(
        () -> new KsqlConnectDeserializer(
            getAvroConverter(schemaRegistryClientFactory.get(), ksqlConfig),
            new AvroDataTranslator(schema))
    );
    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  @Override
  public Serde<GenericRow> getGenericRowSerde(final Schema schemaMaybeWithSource,
                                              final KsqlConfig ksqlConfig,
                                              final boolean isInternal,
                                              final SchemaRegistryClient schemaRegistryClient) {
    return getGenericRowSerde(
        schemaMaybeWithSource, ksqlConfig, isInternal, () -> schemaRegistryClient);
  }
}