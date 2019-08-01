/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.connect;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectConfigServiceTest {

  @Mock
  private KafkaConsumer<String, byte[]> consumer;
  @Mock
  private ConnectPollingService pollingService;
  @Mock
  private Connector connector;

  private ConnectConfigService configService;

  @Test(timeout = 30_000L)
  public void shouldCreateConnectorFromConfig() throws InterruptedException {
    // Given:
    final Map<String, String> config = ImmutableMap.of();
    givenRecord(config);

    final CountDownLatch awaitIteration = new CountDownLatch(1);
    doAnswer(invocationOnMock -> {
      awaitIteration.countDown();
      return null;
    }).when(pollingService).runOneIteration();
    setupConfigService();

    // When:
    configService.startAsync().awaitRunning();

    // Then:
    awaitIteration.await();
    verify(pollingService).addConnector(connector);
    configService.stopAsync().awaitTerminated();
  }

  @Test(timeout = 30_000L)
  public void shouldWakeupConsumerBeforeShuttingDown() {
    // Given:
    final CountDownLatch awaitWakeup = new CountDownLatch(1);
    when(consumer.poll(any())).thenAnswer(invocationOnMock -> {
      awaitWakeup.await();
      throw new WakeupException();
    });
    doAnswer(invocation -> {
      awaitWakeup.countDown();
      return null;
    }).when(consumer).wakeup();
    setupConfigService();
    configService.startAsync().awaitRunning();

    // When:
    configService.stopAsync().awaitTerminated();

    // Then:
    final InOrder inOrder = inOrder(consumer);
    inOrder.verify(consumer).poll(any());
    inOrder.verify(consumer).wakeup();
    inOrder.verify(consumer).close();
    inOrder.verifyNoMoreInteractions();
  }

  private void givenRecord(final Map<String, String> properties) {
    final JsonConverter converter = new JsonConverter();
    converter.configure(ImmutableMap.of(
        "converter.type", "value",
        JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false));

    final byte[] mapAsBytes = converter
        .fromConnectData("topic", null, ImmutableMap.of("properties", properties));
    when(consumer.poll(any()))
        .thenReturn(new ConsumerRecords<>(
            ImmutableMap.of(
                new TopicPartition("topic", 0),
                ImmutableList.of(new ConsumerRecord<>("topic", 0, 0L, "connector", mapAsBytes)))))
        .thenReturn(new ConsumerRecords<>(
            ImmutableMap.of()));
  }

  private void setupConfigService() {
    configService = new ConnectConfigService(
        new KsqlConfig(ImmutableMap.of()),
        pollingService,
        props -> Optional.of(connector),
        props -> consumer);
  }

}