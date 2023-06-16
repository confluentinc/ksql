/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.scalablepush.consumer;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Windowed;

public final class KafkaConsumerFactory {

  private KafkaConsumerFactory() { }

  public interface KafkaConsumerFactoryInterface {
    KafkaConsumer<Object, GenericRow> create(
        KsqlTopic ksqlTopic,
        LogicalSchema logicalSchema,
        ServiceContext serviceContext,
        Map<String, Object> consumerProperties,
        KsqlConfig ksqlConfig,
        String consumerGroupId
    );
  }

  public static KafkaConsumer<Object, GenericRow> create(
      final KsqlTopic ksqlTopic,
      final LogicalSchema logicalSchema,
      final ServiceContext serviceContext,
      final Map<String, Object> consumerProperties,
      final KsqlConfig ksqlConfig,
      final String consumerGroupId
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        logicalSchema,
        ksqlTopic.getKeyFormat().getFeatures(),
        ksqlTopic.getValueFormat().getFeatures()
    );
    final KeySerdeFactory keySerdeFactory = new GenericKeySerDe();
    final Deserializer<Object> keyDeserializer;
    if (ksqlTopic.getKeyFormat().getWindowInfo().isPresent()) {
      final Serde<Windowed<GenericKey>> keySerde = keySerdeFactory.create(
          ksqlTopic.getKeyFormat().getFormatInfo(),
          ksqlTopic.getKeyFormat().getWindowInfo().get(),
          physicalSchema.keySchema(),
          ksqlConfig,
          serviceContext.getSchemaRegistryClientFactory(),
          "",
          NoopProcessingLogContext.INSTANCE,
          Optional.empty()
      );
      keyDeserializer = getDeserializer(keySerde.deserializer());
    } else {
      final Serde<GenericKey> keySerde = keySerdeFactory.create(
          ksqlTopic.getKeyFormat().getFormatInfo(),
          physicalSchema.keySchema(),
          ksqlConfig,
          serviceContext.getSchemaRegistryClientFactory(),
          "",
          NoopProcessingLogContext.INSTANCE,
          Optional.empty()
      );
      keyDeserializer = getDeserializer(keySerde.deserializer());
    }

    final ValueSerdeFactory valueSerdeFactory = new GenericRowSerDe();
    final Serde<GenericRow> valueSerde = valueSerdeFactory.create(
        ksqlTopic.getValueFormat().getFormatInfo(),
        physicalSchema.valueSchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    );
    return new KafkaConsumer<>(
        consumerConfig(consumerProperties, ksqlConfig, consumerGroupId),
        keyDeserializer,
        valueSerde.deserializer()
    );
  }

  @SuppressWarnings("unchecked")
  private static Deserializer<Object> getDeserializer(final Deserializer<?> deserializer) {
    return (Deserializer<Object>) deserializer;
  }

  /**
   * Common consumer properties used.
   *
   * @return base set of consumer properties.
   */
  public static Map<String, Object> consumerConfig(
      final Map<String, Object> consumerProperties,
      final KsqlConfig ksqlConfig,
      final String consumerGroupId
  ) {
    final Map<String, Object> config = new HashMap<>(consumerProperties);
    config.putAll(ksqlConfig.getConsumerClientConfigProps());
    config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return config;
  }

}
