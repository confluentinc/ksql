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

package io.confluent.ksql.serde.protobuf;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KsqlProtobufTopicSerDe extends KsqlTopicSerDe {
  private static final Logger logger = LoggerFactory.getLogger(KsqlProtobufTopicSerDe.class);

  static final String CONFIG_PROTOBUF_CLASS = "config.protobuf.class";
  private final String protobufClass;

  public KsqlProtobufTopicSerDe(final String protobufClass) {
    super(DataSource.DataSourceSerDe.PROTOBUF);

    logger.info("Using protobuf class: {}", protobufClass);
    this.protobufClass = protobufClass;
  }


  @Override
  public Serde<GenericRow> getGenericRowSerde(Schema schema, KsqlConfig ksqlConfig,
                                              boolean isInternal,
                                              SchemaRegistryClient schemaRegistryClient) {
    Map<String, Object> serdeProps = new HashMap<>();
    serdeProps.put("JsonPOJOClass", GenericRow.class);
    serdeProps.put(CONFIG_PROTOBUF_CLASS, protobufClass);

    final Serializer<GenericRow> genericRowSerializer = new KsqlProtobufSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KsqlProtobufDeserializer(schema, false);
    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }
}