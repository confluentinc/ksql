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
import io.confluent.ksql.util.KsqlServerException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.internal.verification.NoMoreInteractions;
import org.mockito.internal.verification.Times;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;

@RunWith(MockitoJUnitRunner.class)
public class ConnectConfigServiceTest {

  @Rule
  public final Timeout timeout = Timeout.seconds(30);

  @Mock
  private KafkaConsumer<String, byte[]> consumer;
  @Mock
  private ConnectPollingService pollingService;
  @Mock
  private ConnectClient connectClient;
  @Mock
  private Connector connector;
  @Mock
  private Function<ConnectorInfo, Optional<Connector>> connectorFactory;

  private ConnectConfigService configService;

  @Before
  public void setUp() {
    when(connectorFactory.apply(any())).thenReturn(Optional.of(connector));
  }

  @Test
  public void shouldCreateConnectorFromConfig() throws InterruptedException {
    // Given:
    givenConnectors("connector");
    givenNoMoreRecords(
        givenConnectorRecord(
            when(consumer.poll(any())))
    );

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

  @Test
  public void shouldWakeupConsumerBeforeShuttingDown() throws InterruptedException {
    // Given:
    final CountDownLatch noMoreLatch = new CountDownLatch(1);
    setupConfigService();
    givenNoMoreRecords(when(consumer.poll(any())), noMoreLatch);

    // When:
    configService.startAsync().awaitRunning();
    noMoreLatch.await();
    configService.stopAsync().awaitTerminated();

    // Then:
    final InOrder inOrder = inOrder(consumer);
    inOrder.verify(consumer).poll(any());
    inOrder.verify(consumer).wakeup();
    inOrder.verify(consumer).close();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldNotDescribeSameConnectorTwice() throws InterruptedException {
    // Given:
    givenConnectors("connector");

    final CountDownLatch noMoreLatch = new CountDownLatch(1);
    givenNoMoreRecords(
        givenConnectorRecord(
            givenConnectorRecord(
                when(consumer.poll(any())))
        ),
        noMoreLatch
    );
    setupConfigService();

    // When:
    configService.startAsync().awaitRunning();
    noMoreLatch.await();

    // Then:
    final InOrder inOrder = inOrder(consumer, connectClient);
    inOrder.verify(consumer).poll(any());
    inOrder.verify(connectClient).connectors();
    inOrder.verify(connectClient).describe("connector");
    inOrder.verify(consumer).poll(any());
    inOrder.verify(connectClient).connectors();
    inOrder.verify(consumer).poll(any());
    // note no more calls to describe
    inOrder.verifyNoMoreInteractions();

    configService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldIgnoreUnsupportedConnectors() throws InterruptedException {
    // Given:
    final CountDownLatch noMoreLatch = new CountDownLatch(1);
    givenNoMoreRecords(
        givenConnectorRecord(
            when(consumer.poll(any()))),
        noMoreLatch
    );
    givenConnectors("foo");
    when(connectorFactory.apply(any())).thenReturn(Optional.empty());
    setupConfigService();

    // When:
    configService.startAsync().awaitRunning();
    noMoreLatch.await();

    // Then:
    verify(pollingService, new NoMoreInteractions()).addConnector(any());
    configService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldIgnoreConnectClientFailureToList() throws InterruptedException {
    // Given:
    final CountDownLatch noMoreLatch = new CountDownLatch(1);
    givenNoMoreRecords(
        givenConnectorRecord(
            when(consumer.poll(any()))),
        noMoreLatch
    );
    when(connectClient.connectors()).thenThrow(new KsqlServerException("fail!"));
    setupConfigService();

    // When:
    configService.startAsync().awaitRunning();
    noMoreLatch.await();

    // Then:
    verify(connectClient, new Times(1)).connectors();
    verify(connectClient, new NoMoreInteractions()).describe(any());
    // poll again even though error was thrown
    verify(consumer, new Times(2)).poll(any());
    configService.stopAsync().awaitTerminated();
  }

  @Test
  public void shouldIgnoreConnectClientFailureToDescribe() throws InterruptedException {
    // Given:
    givenConnectors("connector", "connector2");
    when(connectClient.describe("connector")).thenThrow(new KsqlServerException("fail!"));

    givenNoMoreRecords(
        givenConnectorRecord(
            when(consumer.poll(any())))
    );

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
    verify(connectorFactory, new Times(1)).apply(any());
    verify(pollingService).addConnector(connector);
    configService.stopAsync().awaitTerminated();
  }

  private void givenConnectors(final String... names){
    for (final String name : names) {
      when(connectClient.describe(name)).thenReturn(ConnectResponse.of(new ConnectorInfo(
          name,
          ImmutableMap.of(),
          ImmutableList.of(),
          ConnectorType.SOURCE
      ), HttpStatus.SC_CREATED));
    }
    when(connectClient.connectors()).thenReturn(ConnectResponse.of(ImmutableList.copyOf(names),
        HttpStatus.SC_OK));
  }

  private OngoingStubbing<?> givenConnectorRecord(OngoingStubbing<?> stubbing) {
    return stubbing
        .thenAnswer(inv -> new ConsumerRecords<>(
            ImmutableMap.of(
                new TopicPartition("topic", 0),
                ImmutableList.of(new ConsumerRecord<>("topic", 0, 0L, "connector", new byte[]{})))));
  }

  private void givenNoMoreRecords(final OngoingStubbing<?> stubbing) {
    givenNoMoreRecords(stubbing, new CountDownLatch(1));
  }

  private void givenNoMoreRecords(final OngoingStubbing<?> stubbing, final CountDownLatch noMoreLatch) {
    final CountDownLatch awaitWakeup = new CountDownLatch(1);
    stubbing.thenAnswer(invocationOnMock -> {
      noMoreLatch.countDown();
      awaitWakeup.await();
      throw new WakeupException();
    });

    doAnswer(invocation -> {
      awaitWakeup.countDown();
      return null;
    }).when(consumer).wakeup();
  }

  private void setupConfigService() {
    configService = new ConnectConfigService(
        new KsqlConfig(ImmutableMap.of()),
        connectClient,
        pollingService,
        connectorFactory,
        props -> consumer);
  }

}