/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.server.computation.KafkaConfigStore.KsqlProperties;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.LinkedList;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class KafkaConfigStoreTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(
      ImmutableMap.of(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test-service-id"
      )
  );
  private final String topicName = KsqlInternalTopicUtils.getTopicName(ksqlConfig, "configs");
  private final TopicPartition topicPartition = new TopicPartition(topicName, 0);
  private final List<TopicPartition> topicPartitionAsList
      = Collections.singletonList(topicPartition);

  @Mock
  private Supplier<KafkaConsumer<String, KsqlProperties>> consumerSupplier;
  @Mock
  private Supplier<KafkaProducer<String, KsqlProperties>> producerSupplier;
  @Mock
  private KafkaProducer<String, KsqlProperties> producer;
  @Mock
  private KafkaTopicClient topicClient;

  private KafkaConfigStore configStore;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    when(topicClient.isTopicExists(anyString())).thenReturn(true);
    when(topicClient.addTopicConfig(anyString(), anyMap()))
        .thenReturn(false);
    when(producerSupplier.get()).thenReturn(producer);
  }

  private void initConfigStore() {
    configStore = new KafkaConfigStore(ksqlConfig, topicClient, consumerSupplier, producerSupplier);
  }

  @SuppressWarnings("unchecked")
  private KafkaConsumer<String, KsqlProperties> expectRead(
      final KafkaConfigStore.KsqlProperties... properties) {
    final KafkaConsumer<String, KsqlProperties> consumer = mock(KafkaConsumer.class);
    when(consumer.endOffsets(any()))
        .thenReturn(ImmutableMap.of(topicPartition, (long)properties.length));
    when(consumer.position(any())).thenReturn(0L).thenReturn((long) properties.length);
    final List<ConsumerRecord<String, KafkaConfigStore.KsqlProperties>> records
        = new LinkedList<>();
    for (int i = 0; i < properties.length; i++) {
      records.add(
          new ConsumerRecord<>(
              topicName,
              0,
              (long) i,
              "ksql-standalone-configs",
              properties[i]
          )
      );
    }
    when(consumer.poll(any())).thenReturn(
        new ConsumerRecords<>(
            Collections.singletonMap(
                topicPartition,
                records
            )
        )
    );
    return consumer;
  }

  private void verifyDrainLog(
      final KafkaConsumer<String, KsqlProperties> consumer,
      final InOrder inOrder,
      final int nmsgs) {
    inOrder.verify(consumer).assign(topicPartitionAsList);
    inOrder.verify(consumer).seekToBeginning(topicPartitionAsList);
    for (int i = 0; i < nmsgs; i++) {
      inOrder.verify(consumer).poll(any());
    }
  }

  @Test
  public void shouldWriteConfigIfNoConfigWritten() {
    // Given:
    final KafkaConsumer<String, KsqlProperties> consumerBefore = expectRead();
    final KafkaConsumer<String, KsqlProperties> consumerAfter = expectRead(
        new KafkaConfigStore.KsqlProperties(
            ksqlConfig.getAllConfigPropsWithSecretsObfuscated()));
    when(consumerSupplier.get()).thenReturn(consumerBefore).thenReturn(consumerAfter);

    // When:
    initConfigStore();
    final KsqlConfig resolvedConfig = configStore.getKsqlConfig();

    // Then:
    assertThat(resolvedConfig.values(), equalTo(ksqlConfig.values()));
    assertThat(
        resolvedConfig.getKsqlStreamConfigProps(),
        equalTo(ksqlConfig.getKsqlStreamConfigProps()));
    final InOrder inOrder = Mockito.inOrder(consumerBefore, consumerAfter, producer);
    verifyDrainLog(consumerBefore, inOrder, 0);
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<ProducerRecord<String, KafkaConfigStore.KsqlProperties>> msgCaptor
        = ArgumentCaptor.forClass(ProducerRecord.class);
    inOrder.verify(producer).send(msgCaptor.capture());
    assertThat(
        msgCaptor.getValue().value().getKsqlProperties(),
        equalTo(ksqlConfig.getAllConfigPropsWithSecretsObfuscated()));
    inOrder.verify(producer).flush();
    verifyDrainLog(consumerAfter, inOrder, 1);
  }

  @Test
  public void shouldReadAndMergeExistingConfigIfExists() {
    // Given:
    final KsqlConfig persistedConfig = new KsqlConfig(
        Collections.singletonMap(
            KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "not-the-default"
        )
    );
    final KafkaConsumer<String, KsqlProperties> consumer = expectRead(
        new KafkaConfigStore.KsqlProperties(
            persistedConfig.getAllConfigPropsWithSecretsObfuscated()));
    when(consumerSupplier.get()).thenReturn(consumer);

    // When:
    initConfigStore();
    final KsqlConfig resolvedConfig = configStore.getKsqlConfig();

    // Then:
    assertThat(
        resolvedConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG),
        equalTo("not-the-default"));
    verifyZeroInteractions(producer);
  }

  @Test
  public void shouldDeserializeMissingContentsToNull() {
    // When:
    final Deserializer<KafkaConfigStore.KsqlProperties> deserializer
        = KafkaConfigStore.createDeserializer();
    final KafkaConfigStore.KsqlProperties ksqlProperties
        = deserializer.deserialize(topicName, "{}".getBytes());

    // Then:
    assertThat(ksqlProperties.getKsqlProperties(), equalTo(Collections.emptyMap()));
  }
}
