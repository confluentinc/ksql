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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;

public class KsqlAvroTopicSerDe extends KsqlTopicSerDe {

  public KsqlAvroTopicSerDe() {
    super(DataSource.DataSourceSerDe.AVRO);
  }

  @Override
  public Serde<GenericRow> getGenericRowSerde(Schema schema,
                                              KsqlConfig ksqlConfig,
                                              boolean isInternal,
                                              SchemaRegistryClient schemaRegistryClient) {
    final Serializer<GenericRow> genericRowSerializer =
        new KsqlGenericRowAvroSerializer(schema, schemaRegistryClient, ksqlConfig);
    final Deserializer<GenericRow> genericRowDeserializer =
        new KsqlGenericRowAvroDeserializer(schema, schemaRegistryClient, isInternal);
    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }
}