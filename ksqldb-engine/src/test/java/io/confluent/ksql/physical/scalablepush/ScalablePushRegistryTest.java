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

package io.confluent.ksql.physical.scalablepush;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.physical.scalablepush.consumer.CatchupCoordinator;
import io.confluent.ksql.physical.scalablepush.consumer.KafkaConsumerFactory.KafkaConsumerFactoryInterface;
import io.confluent.ksql.physical.scalablepush.consumer.LatestConsumer;
import io.confluent.ksql.physical.scalablepush.consumer.LatestConsumer.LatestConsumerFactory;
import io.confluent.ksql.physical.scalablepush.consumer.NoopCatchupCoordinator;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@RunWith(MockitoJUnitRunner.class)
public class ScalablePushRegistryTest {
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k1"), SqlTypes.INTEGER)
      .keyColumn(ColumnName.of("k2"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("v2"), SqlTypes.INTEGER)
      .build();

  private static final long TIMESTAMP = 123;
  private static final String TOPIC = "topic";
  private static final String SOURCE_APP_ID = "source_app_id";

  @Mock
  private PushLocator locator;
  @Mock
  private ProcessingQueue processingQueue;
  @Mock
  private ProcessingQueue processingQueue2;
  @Mock
  private KsqlTopic ksqlTopic;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KafkaConsumerFactoryInterface kafkaConsumerFactory;
  @Mock
  private LatestConsumerFactory latestConsumerFactory;
  @Mock
  private KafkaConsumer<Object, GenericRow> kafkaConsumer;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private ExecutorService executorService;

  private ExecutorService realExecutorService;
  private AtomicReference<Runnable> startLatestRunnable = new AtomicReference<>(null);
  private TestLatestConsumer latestConsumer;
  private TestLatestConsumer latestConsumer2;
  private List<TestLatestConsumer> latestConsumers = new ArrayList<>();

  @Before
  public void setUp() {
    when(ksqlTopic.getKafkaTopicName()).thenReturn(TOPIC);
    when(kafkaConsumerFactory.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(kafkaConsumer);

    latestConsumer = new TestLatestConsumer(TOPIC, false, SCHEMA, kafkaConsumer,
        new NoopCatchupCoordinator(), assignment -> { },  ksqlConfig, Clock.systemUTC());
    latestConsumer2 = new TestLatestConsumer(TOPIC, false, SCHEMA, kafkaConsumer,
        new NoopCatchupCoordinator(), assignment -> { },  ksqlConfig, Clock.systemUTC());
    when(latestConsumerFactory.create(any(), anyBoolean(), any(), any(), any(), any(),
        any(), any())).thenReturn(latestConsumer, latestConsumer2);
    when(ksqlTopic.getKeyFormat()).thenReturn(keyFormat);
    when(keyFormat.isWindowed()).thenReturn(false);
    realExecutorService = Executors.newSingleThreadExecutor();
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      startLatestRunnable.set(runnable);
      realExecutorService.submit(runnable);
      return null;
    }).when(executorService).submit(any(Runnable.class));
    when(processingQueue.getQueryId()).thenReturn(new QueryId("q1"));
    when(processingQueue2.getQueryId()).thenReturn(new QueryId("q2"));
  }

  @After
  public void tearDown() {
    realExecutorService.shutdownNow();
  }

  @Test
  public void shouldRegisterAndStartLatest() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false, ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, executorService);

    // When:
    registry.register(processingQueue, false);
    assertThat(registry.isLatestRunning(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(1));

    // Then:
    registry.unregister(processingQueue);
    assertThat(registry.latestNumRegistered(), is(0));
    assertThatEventually(registry::isLatestRunning, is(false));
  }

  @Test
  public void shouldEnforceNewNodeContinuity() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(locator, SCHEMA, true, true,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, executorService);

    // When:
    registry.register(processingQueue, false);
    assertThat(registry.isLatestRunning(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(1));
    final Exception e = assertThrows(KsqlException.class,
        () -> registry.register(processingQueue, true));

    // Then:
    assertThat(e.getMessage(), containsString("New node missed data"));
  }

  @Test
  public void shouldCatchException_onRun() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, executorService);
    latestConsumer.setErrorOnRun(true);
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();

    // When:
    registry.register(processingQueue, false);

    // Then:
    assertThatEventually(isErrorQueue::get, is(true));
  }

  @Test
  public void shouldCatchException_onCreationFailure_kafkaConsumer() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, executorService);
    doThrow(new RuntimeException("Error!"))
        .when(kafkaConsumerFactory).create(any(), any(), any(), any(), any(), any());
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> registry.register(processingQueue, false)
    );

    // Then:
    assertThat(e.getMessage(), is("Error!"));
    assertThat(isErrorQueue.get(), is(true));
    assertThat(registry.isLatestRunning(), is(false));
  }

  @Test
  public void shouldCatchException_onCreationFailure_latestConsumer() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, executorService);
    doThrow(new RuntimeException("Error!"))
        .when(latestConsumerFactory).create(
            any(), anyBoolean(), any(), any(), any(), any(), any(), any());
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> registry.register(processingQueue, false)
    );

    // Then:
    assertThat(e.getMessage(), is("Error!"));
    assertThat(isErrorQueue.get(), is(true));
    assertThat(registry.isLatestRunning(), is(false));
  }

  @Test
  public void shouldStopRunningAfterStartingIfRegistryClosed() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, executorService);
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      startLatestRunnable.set(runnable);
      return null;
    }).when(executorService).submit(any(Runnable.class));

    // When:
    registry.register(processingQueue, false);
    // Close the registry before the lastest has had a chance to run
    registry.close();
    startLatestRunnable.get().run();

    // Then:
    assertThat(registry.latestNumRegistered(), is(0));
    assertThat(registry.isLatestRunning(), is(false));
  }

  @Test
  public void shouldCloseLatestAfterStartingIfRequestUnregistered() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, executorService);
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      startLatestRunnable.set(runnable);
      return null;
    }).when(executorService).submit(any(Runnable.class));

    // When:
    registry.register(processingQueue, false);
    registry.unregister(processingQueue);
    startLatestRunnable.get().run();

    // Then:
    assertThat(registry.latestNumRegistered(), is(0));
    assertThat(registry.isLatestRunning(), is(false));
  }


  @Test
  public void shouldRegisterAfterAlreadyStarted() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, executorService);
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      startLatestRunnable.set(runnable);
      return null;
    }).when(executorService).submit(any(Runnable.class));

    // When:
    registry.register(processingQueue, false);
    // This should register a second queue, but we haven't actually started running the latest
    // yet.
    registry.register(processingQueue2, false);
    realExecutorService.submit(startLatestRunnable.get());

    // Then:
    assertThatEventually(registry::latestNumRegistered, is(2));
    assertThat(registry.isLatestRunning(), is(true));
  }

  @Test
  public void shouldRegisterAfterBeginningClosing() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, executorService);

    // When:
    latestConsumer.setForceRun(true);
    registry.register(processingQueue, false);
    assertThatEventually(registry::latestNumRegistered, is(1));
    // This should make the latest close and shutdown
    registry.unregister(processingQueue);
    // Meanwhile another queue is registered
    registry.register(processingQueue2, false);
    // Only now does the consumer stop running, forcing it to kick off another consumer
    latestConsumer.setForceRun(false);

    // Then:
    verify(latestConsumerFactory, times(2)).create(any(), anyBoolean(), any(), any(), any(), any(),
        any(), any());
    assertThatEventually(registry::latestNumRegistered, is(1));
    assertThat(registry.isLatestRunning(), is(true));
    registry.unregister(processingQueue2);
    assertThatEventually(registry::latestNumRegistered, is(0));
    assertThatEventually(registry::isLatestRunning, is(false));
    assertThat(latestConsumer.isClosed(), is(true));
    assertThat(latestConsumer2.isClosed(), is(true));
  }

  @Test
  public void shouldCreate() {
    // When:
    final Optional<ScalablePushRegistry> registry =
        ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, "http://localhost:8088"),
            false, ImmutableMap.of(), SOURCE_APP_ID, ksqlTopic, serviceContext, ksqlConfig);

    // Then:
    assertThat(registry.isPresent(), is(true));
  }

  @Test
  public void shouldCreate_badApplicationServer() {
    // When
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, 123), false,
            ImmutableMap.of(), SOURCE_APP_ID, ksqlTopic, serviceContext, ksqlConfig)
    );

    // Then
    assertThat(e.getMessage(), containsString("not String"));
  }

  @Test
  public void shouldCreate_badUrlApplicationServer() {
    // When
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, "abc"), false,
            ImmutableMap.of(), SOURCE_APP_ID, ksqlTopic, serviceContext, ksqlConfig)
    );

    // Then
    assertThat(e.getMessage(), containsString("malformed"));
  }

  @Test
  public void shouldCreate_noApplicationServer() {
    // When
    final Optional<ScalablePushRegistry> registry =
        ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of(), false, ImmutableMap.of(), SOURCE_APP_ID,
            ksqlTopic, serviceContext, ksqlConfig);

    // Then
    assertThat(registry.isPresent(), is(false));
  }

  public static class TestLatestConsumer extends LatestConsumer {

    private final AtomicBoolean forceRun = new AtomicBoolean(false);
    private final AtomicBoolean errorOnRun = new AtomicBoolean(false);

    public TestLatestConsumer(String topicName, boolean windowed,
        LogicalSchema logicalSchema,
        KafkaConsumer<Object, GenericRow> consumer,
        CatchupCoordinator catchupCoordinator,
        Consumer<Collection<TopicPartition>> catchupAssignmentUpdater,
        KsqlConfig ksqlConfig, Clock clock) {
      super(topicName, windowed, logicalSchema, consumer, catchupCoordinator,
          catchupAssignmentUpdater,
          ksqlConfig, clock);
    }

    public void setForceRun(boolean forceRun) {
      this.forceRun.set(forceRun);
    }

    public void setErrorOnRun(boolean errorOnRun) {
      this.errorOnRun.set(errorOnRun);
    }

    public void run() {
      if (errorOnRun.get()) {
        throw new RuntimeException("Error!");
      }
      while (!isClosed() || forceRun.get()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    public long getNumRowsReceived() {
      return 1L;
    }
  }
}
