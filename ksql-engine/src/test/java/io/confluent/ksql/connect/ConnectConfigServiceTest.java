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
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
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
  private ConnectClient connectClient;
  @Mock
  private Connector connector;

  private ConnectConfigService configService;

  @Test(timeout = 30_000L)
  public void shouldCreateConnectorFromConfig() throws InterruptedException {
    // Given:
    final Map<String, String> config = ImmutableMap.of();
    givenConnector("connector", config);

    final CountDownLatch awaitIteration = new CountDownLatch(1);
    doAnswer(invocationOnMock -> {
      awaitIteration.countDown();
      return null;
    }).when(pollingService).addConnector(connector);
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
    setupConfigService();
    givenConnector("ignored", ImmutableMap.of());
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

  private void givenConnector(final String name, final Map<String, String> properties) {
    final CountDownLatch awaitWakeup = new CountDownLatch(1);
    when(consumer.poll(any()))
        .thenReturn(new ConsumerRecords<>(
            ImmutableMap.of(
                new TopicPartition("topic", 0),
                ImmutableList.of(new ConsumerRecord<>("topic", 0, 0L, "connector", new byte[]{})))))
        .thenAnswer(invocationOnMock -> {
          awaitWakeup.await(30, TimeUnit.SECONDS);
          throw new WakeupException();
        });

    doAnswer(invocation -> {
      awaitWakeup.countDown();
      return null;
    }).when(consumer).wakeup();

    when(connectClient.describe(name)).thenReturn(ConnectResponse.of(new ConnectorInfo(
        name,
        properties,
        ImmutableList.of(),
        ConnectorType.SOURCE
    )));
    when(connectClient.connectors()).thenReturn(ConnectResponse.of(ImmutableList.of(name)));
  }

  private void setupConfigService() {
    configService = new ConnectConfigService(
        new KsqlConfig(ImmutableMap.of()),
        connectClient,
        pollingService,
        info -> Optional.of(connector),
        props -> consumer);
  }

}