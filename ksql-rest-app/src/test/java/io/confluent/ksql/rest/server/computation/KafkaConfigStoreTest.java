/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.server.computation.KafkaConfigStore.KsqlProperties;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class KafkaConfigStoreTest {
  private final static String TOPIC_NAME = "topic";

  private final KsqlConfig currentConfig = new KsqlConfig(
      ImmutableMap.of(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "current"));
  private final KsqlConfig savedConfig = new KsqlConfig(
      ImmutableMap.of(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "saved"));
  private final KsqlConfig badConfig = new KsqlConfig(
      ImmutableMap.of(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "bad"));

  private final KsqlProperties properties = new KsqlProperties(
      currentConfig.getAllConfigPropsWithSecretsObfuscated()
  );
  private final KsqlProperties savedProperties = new KsqlProperties(
      savedConfig.getAllConfigPropsWithSecretsObfuscated()
  );
  private final KsqlProperties badProperties = new KsqlProperties(
      badConfig.getAllConfigPropsWithSecretsObfuscated()
  );

  private final TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
  private final List<TopicPartition> topicPartitionAsList
      = Collections.singletonList(topicPartition);
  private final List<ConsumerRecords<String, byte[]>> log = new LinkedList<>();
  private final Serializer<KsqlProperties> serializer = KafkaConfigStore.createSerializer();

  @Mock
  private KafkaConsumer<String, byte[]> consumerBefore;
  @Mock
  private KafkaConsumer<String, byte[]> consumerAfter;
  @Mock
  private Supplier<KafkaConsumer<String, byte[]>> consumerSupplier;
  @Mock
  private Supplier<KafkaProducer<String, KsqlProperties>> producerSupplier;
  @Mock
  private KafkaProducer<String, KsqlProperties> producer;
  @Mock
  private KsqlConfig currentConfigProxy;
  @Mock
  private KsqlConfig mergedConfig;
  private InOrder inOrder;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    when(producerSupplier.get()).thenReturn(producer);
    when(consumerSupplier.get()).thenReturn(consumerBefore).thenReturn(consumerAfter);
    when(currentConfigProxy.getAllConfigPropsWithSecretsObfuscated()).thenReturn(
        currentConfig.getAllConfigPropsWithSecretsObfuscated());
    when(currentConfigProxy.overrideBreakingConfigsWithOriginalValues(any()))
        .thenReturn(mergedConfig);
    inOrder = Mockito.inOrder(consumerBefore, consumerAfter, producer);
  }

  private KsqlConfig getKsqlConfig() {
    return new KafkaConfigStore(
        TOPIC_NAME,
        currentConfigProxy,
        consumerSupplier,
        producerSupplier
    ).getKsqlConfig();
  }

  private long endOffset(final List<ConsumerRecords<String, byte[]>> log) {
    return log.stream().mapToLong(ConsumerRecords::count).sum();
  }

  private long endOffset() {
    return endOffset(this.log);
  }

  private void addPollResult(
      final String key,
      final byte[]... properties) {
    final List<ConsumerRecord<String, byte[]>> records
        = new LinkedList<>();
    final long start = endOffset();
    for (int i = 0; i < properties.length; i++) {
      records.add(
          new ConsumerRecord<>(TOPIC_NAME, 0, start + i, key, properties[i])
      );
    }
    log.add(new ConsumerRecords<>(Collections.singletonMap(topicPartition, records)));
  }

  private void addPollResult(
      final String key,
      final KsqlProperties... properties) {
    addPollResult(
        key,
        Arrays.stream(properties)
            .map(p -> serializer.serialize("", p))
            .collect(Collectors.toList())
            .toArray(new byte[properties.length][]));
  }

  private void addPollResult(final String key1, byte[] value1, final String key2, byte[] value2) {
    final long start = endOffset();
    log.add(new ConsumerRecords<>(Collections.singletonMap(
        topicPartition,
        Collections.singletonList(new ConsumerRecord<>(TOPIC_NAME, 0, start, key1, value1)
    ))));
    log.add(new ConsumerRecords<>(Collections.singletonMap(
        topicPartition,
        Collections.singletonList(new ConsumerRecord<>(TOPIC_NAME, 0, start + 1, key2, value2)
    ))));
  }

  private void expectRead(final KafkaConsumer<String, byte[]> consumer) {
    final long endOff = endOffset();
    when(consumer.endOffsets(any())).thenReturn(ImmutableMap.of(topicPartition, endOff));
    final ListIterator<ConsumerRecords<String, byte[]>> iterator
        = ImmutableList.copyOf(log).listIterator();
    when(consumer.position(topicPartition)).thenAnswer(
        invocation -> endOffset(log.subList(0, iterator.nextIndex()))
    );
    when(consumer.poll(any())).thenAnswer(
        invocation -> iterator.hasNext() ? iterator.next() : ConsumerRecords.empty());
  }

  private void verifyDrainLog(final KafkaConsumer<String, byte[]> consumer, final int nPolls) {
    inOrder.verify(consumer).assign(topicPartitionAsList);
    inOrder.verify(consumer).seekToBeginning(topicPartitionAsList);
    inOrder.verify(consumer, times(nPolls)).poll(any());
    inOrder.verify(consumer).close();
  }

  private void verifyProduce() {
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<ProducerRecord<String, KafkaConfigStore.KsqlProperties>> msgCaptor
        = ArgumentCaptor.forClass(ProducerRecord.class);
    inOrder.verify(producer).send(msgCaptor.capture());
    assertThat(
        msgCaptor.getValue().value().getKsqlProperties(),
        equalTo(currentConfig.getAllConfigPropsWithSecretsObfuscated()));
    inOrder.verify(producer).flush();
  }

  private void verifyMergedConfig(final KsqlConfig mergedConfig) {
    assertThat(mergedConfig, is(this.mergedConfig));
    verify(currentConfigProxy).overrideBreakingConfigsWithOriginalValues(
        savedConfig.getAllConfigPropsWithSecretsObfuscated());
  }

  @Test
  public void shouldIgnoreRecordsWithDifferentKey() {
    // Given:
    addPollResult("foo", "val".getBytes());
    addPollResult(KafkaConfigStore.CONFIG_MSG_KEY, serializer.serialize("", savedProperties));
    expectRead(consumerBefore);

    // When:
    getKsqlConfig();

    // Then:
    verifyDrainLog(consumerBefore, 2);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldIgnoreRecordsWithDifferentKeyWithinPoll() {
    // Given:
    addPollResult(
        "foo", "val".getBytes(),
        KafkaConfigStore.CONFIG_MSG_KEY, serializer.serialize("", savedProperties)
    );
    expectRead(consumerBefore);

    // When:
    getKsqlConfig();

    // Then:
    verifyDrainLog(consumerBefore, 1);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldPollToEndOfTopic() {
    // Given:
    addPollResult("foo", "val".getBytes());
    addPollResult("bar", "baz".getBytes());
    expectRead(consumerBefore);
    addPollResult(KafkaConfigStore.CONFIG_MSG_KEY, savedProperties);
    expectRead(consumerAfter);

    // When:
    getKsqlConfig();

    // Then:
    verifyDrainLog(consumerBefore, 2);
    verifyProduce();
  }

  @Test
  public void shouldWriteConfigIfNoConfigWritten() {
    // Given:
    expectRead(consumerBefore);
    addPollResult(KafkaConfigStore.CONFIG_MSG_KEY, properties);
    expectRead(consumerAfter);

    // When:
    getKsqlConfig();

    // Then:
    verifyDrainLog(consumerBefore, 0);
    verifyProduce();
  }


  @Test
  public void shouldUseFirstPolledConfig() {
    // Given:
    addPollResult(KafkaConfigStore.CONFIG_MSG_KEY, savedProperties, badProperties);
    expectRead(consumerBefore);

    // When:
    final KsqlConfig mergedConfig = getKsqlConfig();

    // Then:
    verifyMergedConfig(mergedConfig);
  }

  @Test
  public void shouldNotWriteConfigIfExists() {
    // Given:
    addPollResult(KafkaConfigStore.CONFIG_MSG_KEY, savedProperties);
    expectRead(consumerBefore);

    // When:
    getKsqlConfig();

    // Then:
    verifyNoMoreInteractions(producer);
  }

  @Test
  public void shouldReadConfigAfterWrite() {
    // Given:
    expectRead(consumerBefore);
    addPollResult(KafkaConfigStore.CONFIG_MSG_KEY, savedProperties, properties);
    expectRead(consumerAfter);

    // When:
    final KsqlConfig mergedConfig = getKsqlConfig();

    // Then:
    verifyDrainLog(consumerBefore, 0);
    verifyProduce();
    verifyDrainLog(consumerAfter, 1);
    verifyMergedConfig(mergedConfig);
  }

  @Test
  public void shouldMergeExistingConfigIfExists() {
    // Given:
    addPollResult(KafkaConfigStore.CONFIG_MSG_KEY, savedProperties);
    expectRead(consumerBefore);

    // When:
    final KsqlConfig mergedConfig = getKsqlConfig();

    // Then:
    verifyMergedConfig(mergedConfig);
  }

  @Test
  public void shouldDeserializeEmptyContentsToNull() {
    // When:
    final Deserializer<KafkaConfigStore.KsqlProperties> deserializer
        = KafkaConfigStore.createDeserializer();
    final KafkaConfigStore.KsqlProperties ksqlProperties
        = deserializer.deserialize(TOPIC_NAME, "{}".getBytes(StandardCharsets.UTF_8));

    // Then:
    assertThat(ksqlProperties.getKsqlProperties(), equalTo(Collections.emptyMap()));
  }
}
