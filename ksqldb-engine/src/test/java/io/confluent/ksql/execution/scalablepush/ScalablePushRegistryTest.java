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

package io.confluent.ksql.execution.scalablepush;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry.CatchupMetadata;
import io.confluent.ksql.execution.scalablepush.consumer.CatchupConsumer;
import io.confluent.ksql.execution.scalablepush.consumer.CatchupConsumer.CatchupConsumerFactory;
import io.confluent.ksql.execution.scalablepush.consumer.CatchupCoordinator;
import io.confluent.ksql.execution.scalablepush.consumer.KafkaConsumerFactory.KafkaConsumerFactoryInterface;
import io.confluent.ksql.execution.scalablepush.consumer.LatestConsumer;
import io.confluent.ksql.execution.scalablepush.consumer.LatestConsumer.LatestConsumerFactory;
import io.confluent.ksql.execution.scalablepush.locator.PushLocator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PushOffsetRange;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
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
  private static final String CATCHUP_CONSUMER_GROUP = "catchup_consumer_group";

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
  private CatchupConsumerFactory catchupConsumerFactory;
  @Mock
  private KafkaConsumer<Object, GenericRow> kafkaConsumer;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private ExecutorService executorService;
  @Mock
  private ScheduledExecutorService catchupService;
  @Mock
  private PushOffsetRange pushOffsetRange;

  private ExecutorService realExecutorService;
  private AtomicReference<Runnable> startLatestRunnable = new AtomicReference<>(null);
  private TestLatestConsumer latestConsumer;
  private TestLatestConsumer latestConsumer2;
  private TestCatchupConsumer catchupConsumer;
  private TestCatchupCoordinator catchupCoordinator;
  private ScalablePushRegistry registry;

  @Before
  public void setUp() {
    when(ksqlTopic.getKafkaTopicName()).thenReturn(TOPIC);
    when(kafkaConsumerFactory.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(kafkaConsumer);

    catchupCoordinator = new TestCatchupCoordinator();
    latestConsumer = new TestLatestConsumer(TOPIC, false, SCHEMA, kafkaConsumer,
        catchupCoordinator, assignment -> { },  ksqlConfig, Clock.systemUTC());
    latestConsumer2 = new TestLatestConsumer(TOPIC, false, SCHEMA, kafkaConsumer,
        catchupCoordinator, assignment -> { },  ksqlConfig, Clock.systemUTC());
    catchupConsumer = new TestCatchupConsumer(TOPIC, false, SCHEMA, kafkaConsumer,
        () -> latestConsumer,
        catchupCoordinator, pushOffsetRange, Clock.systemUTC(), pq -> { });

    when(latestConsumerFactory.create(any(), anyBoolean(), any(), any(), any(), any(),
        any(), any())).thenReturn(latestConsumer, latestConsumer2);
    when(catchupConsumerFactory.create(any(), anyBoolean(), any(), any(), any(), any(),
        any(), any(), anyLong(), any())).thenReturn(catchupConsumer);
    when(ksqlTopic.getKeyFormat()).thenReturn(keyFormat);
    when(keyFormat.isWindowed()).thenReturn(false);
    realExecutorService = Executors.newFixedThreadPool(2);
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      startLatestRunnable.set(runnable);
      realExecutorService.submit(runnable);
      return null;
    }).when(executorService).submit(any(Runnable.class));
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      realExecutorService.submit(runnable);
      return null;
    }).when(catchupService).submit(any(Runnable.class));
    when(processingQueue.getQueryId()).thenReturn(new QueryId("q1"));
    when(processingQueue2.getQueryId()).thenReturn(new QueryId("q2"));
    registry = new ScalablePushRegistry(
        locator, SCHEMA, false, ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, catchupConsumerFactory, executorService,
        catchupService);
    when(ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS)).thenReturn(10);
  }

  @After
  public void tearDown() throws InterruptedException {
    realExecutorService.shutdown();
    if (!realExecutorService.awaitTermination(100, TimeUnit.MILLISECONDS)) {
      realExecutorService.shutdownNow();
    }
  }

  @Test
  public void shouldRegisterAndStartLatest() {
    // When:
    registry.register(processingQueue, Optional.empty());
    assertThat(registry.isLatestRunning(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(1));

    // Then:
    registry.unregister(processingQueue);
    assertThat(registry.latestNumRegistered(), is(0));
    assertThatEventually(registry::isLatestRunning, is(false));
  }

  @Test
  public void shouldCatchException_onRun() {
    // Given:
    latestConsumer.setErrorOnRun(true);
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();

    // When:
    registry.register(processingQueue, Optional.empty());

    // Then:
    assertThatEventually(isErrorQueue::get, is(true));
    assertThatEventually(registry::isLatestRunning, is(false));
    assertThatEventually(registry::latestNumRegistered, is(0));
  }

  @Test
  public void shouldCatchException_onCreationFailure_kafkaConsumer() {
    // Given:
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
        () -> registry.register(processingQueue, Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), is("Error!"));
    assertThat(isErrorQueue.get(), is(true));
    assertThat(registry.isLatestRunning(), is(false));
  }

  @Test
  public void shouldCatchException_onCreationFailure_latestConsumer() {
    // Given:
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
        () -> registry.register(processingQueue, Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), is("Error!"));
    assertThat(isErrorQueue.get(), is(true));
    assertThat(registry.isLatestRunning(), is(false));
  }

  @Test
  public void shouldStopRunningAfterStartingIfRegistryClosed() {
    // Given:
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      startLatestRunnable.set(runnable);
      return null;
    }).when(executorService).submit(any(Runnable.class));

    // When:
    registry.register(processingQueue, Optional.empty());
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
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      startLatestRunnable.set(runnable);
      return null;
    }).when(executorService).submit(any(Runnable.class));

    // When:
    registry.register(processingQueue, Optional.empty());
    registry.unregister(processingQueue);
    startLatestRunnable.get().run();

    // Then:
    assertThat(registry.latestNumRegistered(), is(0));
    assertThat(registry.isLatestRunning(), is(false));
  }


  @Test
  public void shouldRegisterAfterAlreadyStarted() {
    // Given:
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      startLatestRunnable.set(runnable);
      return null;
    }).when(executorService).submit(any(Runnable.class));

    // When:
    registry.register(processingQueue, Optional.empty());
    // This should register a second queue, but we haven't actually started running the latest
    // yet.
    registry.register(processingQueue2, Optional.empty());
    realExecutorService.submit(startLatestRunnable.get());

    // Then:
    assertThatEventually(registry::latestNumRegistered, is(2));
    assertThat(registry.isLatestRunning(), is(true));
  }

  @Test
  public void shouldRegisterAfterBeginningClosing() {
    // When:
    latestConsumer.setForceRun(true);
    registry.register(processingQueue, Optional.empty());
    assertThatEventually(registry::latestNumRegistered, is(1));
    // This should make the latest close and shutdown
    registry.unregister(processingQueue);
    // Meanwhile another queue is registered
    registry.register(processingQueue2, Optional.empty());
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
            ImmutableMap.of(), SOURCE_APP_ID, ksqlTopic, serviceContext, ksqlConfig);

    // Then:
    assertThat(registry.isPresent(), is(true));
  }

  @Test
  public void shouldCreate_badApplicationServer() {
    // When
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, 123),
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
            ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, "abc"),
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
            ImmutableMap.of(), ImmutableMap.of(), SOURCE_APP_ID,
            ksqlTopic, serviceContext, ksqlConfig);

    // Then
    assertThat(registry.isPresent(), is(false));
  }

  @Test
  public void shouldRegisterAndStartCatchup() {
    // When:
    registry.register(processingQueue,
        Optional.of(new CatchupMetadata(pushOffsetRange, CATCHUP_CONSUMER_GROUP)));
    assertThat(registry.isLatestRunning(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(0));
    assertThatEventually(registry::catchupNumRegistered, is(1));

    // Then:
    registry.unregister(processingQueue);
    assertThat(registry.isLatestRunning(), is(false));
    assertThat(registry.latestNumRegistered(), is(0));
    assertThat(registry.catchupNumRegistered(), is(0));
  }

  @Test
  public void shouldRegisterAndStartCatchup_andSwitchOver() {
    // Given:
    when(catchupConsumerFactory.create(any(), anyBoolean(), any(), any(), any(), any(),
        any(), any(), anyLong(), any())).thenAnswer(a -> {
      Consumer<ProcessingQueue> caughtUpCallback = a.getArgument(9);
      catchupConsumer = new TestCatchupConsumer(TOPIC, false, SCHEMA, kafkaConsumer,
          () -> latestConsumer,
          catchupCoordinator, pushOffsetRange, Clock.systemUTC(), caughtUpCallback);
      return catchupConsumer;
    });

    // When:
    registry.register(processingQueue,
        Optional.of(new CatchupMetadata(pushOffsetRange, CATCHUP_CONSUMER_GROUP)));
    assertThat(registry.isLatestRunning(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(0));
    assertThatEventually(registry::catchupNumRegistered, is(1));

    // Then:
    catchupCoordinator.setCaughtUp();
    assertThat(registry.isLatestRunning(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(1));
    assertThatEventually(registry::catchupNumRegistered, is(0));
    registry.unregister(processingQueue);
    assertThatEventually(registry::isLatestRunning, is(false));
  }

  @Test
  public void shouldCatchExceptionCatchup_onRun() {
    // Given:
    catchupConsumer.setErrorOnRun(true);
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();

    // When:
    registry.register(processingQueue,
        Optional.of(new CatchupMetadata(pushOffsetRange, CATCHUP_CONSUMER_GROUP)));

    // Then:
    assertThatEventually(isErrorQueue::get, is(true));
    assertThatEventually(registry::isLatestRunning, is(false));
    assertThatEventually(registry::latestNumRegistered, is(0));
    assertThatEventually(registry::catchupNumRegistered, is(0));
  }

  @Test
  public void shouldCatchExceptionCatchup_onCreationFailure_kafkaConsumer() {
    // Given:
    doReturn(kafkaConsumer)
        .when(kafkaConsumerFactory).create(any(), any(), any(), any(), any(), contains("latest"));
    doThrow(new RuntimeException("Error!"))
        .when(kafkaConsumerFactory).create(any(), any(), any(), any(), any(), contains("catchup"));
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> registry.register(processingQueue,
            Optional.of(new CatchupMetadata(pushOffsetRange, CATCHUP_CONSUMER_GROUP)))
    );

    // Then:
    assertThat(e.getMessage(), is("Error!"));
    assertThat(isErrorQueue.get(), is(true));
    assertThat(registry.isLatestRunning(), is(false));
    assertThat(registry.anyCatchupsRunning(), is(false));
  }

  @Test
  public void shouldCatchExceptionCatchup_onCreationFailure_catchupConsumer() {
    // Given:
    doThrow(new RuntimeException("Error!"))
        .when(catchupConsumerFactory).create(
        any(), anyBoolean(), any(), any(), any(), any(), any(), any(), anyLong(), any());
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> registry.register(processingQueue,
            Optional.of(new CatchupMetadata(pushOffsetRange, CATCHUP_CONSUMER_GROUP)))
    );

    // Then:
    assertThat(e.getMessage(), is("Error!"));
    assertThat(isErrorQueue.get(), is(true));
    assertThat(registry.isLatestRunning(), is(false));
    assertThat(registry.anyCatchupsRunning(), is(false));
  }

  @Test
  public void shouldStopRunningAfterStartingIfRegistryClosedCatchup() {
    // When:
    registry.register(processingQueue,
        Optional.of(new CatchupMetadata(pushOffsetRange, CATCHUP_CONSUMER_GROUP)));
    // Close the registry before the lastest has had a chance to run
    registry.close();

    // Then:
    assertThat(registry.latestNumRegistered(), is(0));
    assertThat(registry.isLatestRunning(), is(false));
    assertThat(registry.anyCatchupsRunning(), is(false));
  }

  @Test
  public void shouldThrowErrorOnTooManyCatchups() {
    // Given:
    when(ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS)).thenReturn(1);

    // When:
    registry.register(processingQueue,
        Optional.of(new CatchupMetadata(pushOffsetRange, CATCHUP_CONSUMER_GROUP)));
    assertThat(registry.isLatestRunning(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(0));
    assertThatEventually(registry::catchupNumRegistered, is(1));
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> registry.register(processingQueue2,
            Optional.of(new CatchupMetadata(pushOffsetRange, CATCHUP_CONSUMER_GROUP)))
    );
    registry.unregister(processingQueue);

    // Then:
    assertThat(e.getMessage(), containsString("Too many catchups registered"));
    verify(processingQueue, never()).onError();
    verify(processingQueue2).onError();
    assertThat(registry.isLatestRunning(), is(false));
    assertThat(registry.latestNumRegistered(), is(0));
    assertThat(registry.catchupNumRegistered(), is(0));
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

  public static class TestCatchupConsumer extends CatchupConsumer {
    private final AtomicBoolean forceRun = new AtomicBoolean(false);
    private final AtomicBoolean errorOnRun = new AtomicBoolean(false);

    public TestCatchupConsumer(String topicName, boolean windowed,
        LogicalSchema logicalSchema,
        KafkaConsumer<Object, GenericRow> consumer,
        Supplier<LatestConsumer> latestConsumerSupplier,
        CatchupCoordinator catchupCoordinator,
        PushOffsetRange pushOffsetRange, Clock clock,
        Consumer<ProcessingQueue> caughtUpCallback) {
      super(topicName, windowed, logicalSchema, consumer, latestConsumerSupplier,
          catchupCoordinator,
          pushOffsetRange, clock, 0, caughtUpCallback);
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
          checkCaughtUp();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    public long getNumRowsReceived() {
      return 1L;
    }
  }

  public static class TestCatchupCoordinator implements CatchupCoordinator {
    private final AtomicBoolean caughtUp = new AtomicBoolean(false);

    @Override
    public void checkShouldWaitForCatchup() {
    }

    @Override
    public boolean checkShouldCatchUp(AtomicBoolean signalledLatest,
        Function<Boolean, Boolean> isCaughtUp, Runnable switchOver) {
      if (caughtUp.get()) {
        switchOver.run();
        return true;
      }
      return false;
    }

    @Override
    public void catchupIsClosing(AtomicBoolean signalledLatest) {
    }

    public void setCaughtUp() {
      caughtUp.set(true);
    }
  }
}
