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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.physical.scalablepush.consumer.ConsumerMetadata;
import io.confluent.ksql.physical.scalablepush.consumer.ConsumerMetadata.ConsumerMetadataFactory;
import io.confluent.ksql.physical.scalablepush.consumer.KafkaConsumerFactory.KafkaConsumerFactoryInterface;
import io.confluent.ksql.physical.scalablepush.consumer.LatestConsumer;
import io.confluent.ksql.physical.scalablepush.consumer.LatestConsumer.LatestConsumerFactory;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
  private static final ConsumerMetadata METADATA = new ConsumerMetadata(2);

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
  private ConsumerMetadataFactory consumerMetadataFactory;
  @Mock
  private LatestConsumer latestConsumer;
  @Mock
  private KafkaConsumer<Object, GenericRow> kafkaConsumer;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private ExecutorService executorService;

  private ExecutorService realExecutorService;
  private AtomicReference<Runnable> startLatestRunnable = new AtomicReference<>(null);
  private AtomicBoolean latestClosed = new AtomicBoolean(false);

  @Before
  public void setUp() {
    when(ksqlTopic.getKafkaTopicName()).thenReturn(TOPIC);
    when(kafkaConsumerFactory.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(kafkaConsumer);

    doAnswer(a -> {
      while (!latestClosed.get()) {
        Thread.sleep(10);
      }
      return null;
    }).when(latestConsumer).run();
    doAnswer(a -> {
      latestClosed.set(true);
      return null;
    }).when(latestConsumer).close();
    when(latestConsumer.isClosed()).thenAnswer(a -> {
      return latestClosed.get();
    });
    AtomicInteger num = new AtomicInteger(0);
    doAnswer(a -> {
      num.incrementAndGet();
      return null;
    }).when(latestConsumer).register(any());
    doAnswer(a -> {
      num.decrementAndGet();
      return null;
    }).when(latestConsumer).unregister(any());
    doAnswer(a -> {
      return num.get();
    }).when(latestConsumer).numRegistered();
    when(latestConsumerFactory.create(anyInt(), any(), anyBoolean(), any(), any(), any(), any(),
        any(), any())).thenAnswer(a -> {
          latestClosed.set(false);
          return latestConsumer;
    });
    when(consumerMetadataFactory.create(any(), any()))
        .thenReturn(METADATA);
    when(ksqlTopic.getKeyFormat()).thenReturn(keyFormat);
    when(keyFormat.isWindowed()).thenReturn(false);
    realExecutorService = Executors.newSingleThreadExecutor();
    doAnswer(a -> {
      final Runnable runnable = a.getArgument(0);
      startLatestRunnable.set(runnable);
      realExecutorService.submit(runnable);
      return null;
    }).when(executorService).submit(any(Runnable.class));
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
        kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory, executorService);

    // When:
    registry.register(processingQueue, false);
    assertThat(registry.isLatestStarted(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(1));

    // Then:
    registry.unregister(processingQueue);
    assertThat(registry.latestNumRegistered(), is(0));
    assertThatEventually(registry::isLatestStarted, is(false));
  }

  @Test
  public void shouldEnforceNewNodeContinuity() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(locator, SCHEMA, true, true,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory, executorService);
    when(latestConsumer.getNumRowsReceived()).thenReturn(1L);

    // When:
    registry.register(processingQueue, false);
    assertThat(registry.isLatestStarted(), is(true));
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
        kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory, executorService);
    doThrow(new RuntimeException("Error!")).when(latestConsumer).run();
    AtomicBoolean isErrorConsumer = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorConsumer.set(true);
      return null;
    }).when(latestConsumer).onError();

    // When:
    registry.register(processingQueue, false);

    // Then:
    assertThatEventually(isErrorConsumer::get, is(true));
  }

  @Test
  public void shouldCatchException_onCreationFailure_kafkaConsumer() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory, executorService);
    doThrow(new RuntimeException("Error!"))
        .when(kafkaConsumerFactory).create(any(), any(), any(), any(), any(), any());
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();

    // When:
    registry.register(processingQueue, false);

    // Then:
    assertThatEventually(isErrorQueue::get, is(true));
    assertThat(registry.isLatestStarted(), is(false));
  }

  @Test
  public void shouldCatchException_onCreationFailure_latestConsumer() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory, executorService);
    doThrow(new RuntimeException("Error!"))
        .when(latestConsumerFactory).create(
            anyInt(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();

    // When:
    registry.register(processingQueue, false);

    // Then:
    assertThatEventually(isErrorQueue::get, is(true));
    assertThat(registry.isLatestStarted(), is(false));
  }

  @Test
  public void shouldCloseLatestAfterStartingIfRegistryClosed() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory, executorService);
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
    assertThat(registry.isLatestStarted(), is(false));
  }

  @Test
  public void shouldCloseLatestAfterStartingIfRequestUnregistered() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory, executorService);
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
    assertThat(registry.isLatestStarted(), is(false));
  }


  @Test
  public void shouldRegisterAfterAlreadyStarted() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory, executorService);
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
    assertThat(registry.isLatestStarted(), is(true));
  }

  @Test
  public void shouldRegisterAfterBeginningClosing() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, SOURCE_APP_ID,
        kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory, executorService);
    AtomicBoolean forceRun = new AtomicBoolean(false);
    doAnswer(a -> {
      while (!latestClosed.get() || forceRun.get()) {
        Thread.sleep(10);
      }
      return null;
    }).when(latestConsumer).run();

    // When:
    forceRun.set(true);
    registry.register(processingQueue, false);
    assertThatEventually(registry::latestNumRegistered, is(1));
    // This should make the latest close and shutdown
    registry.unregister(processingQueue);
    // Meanwhile another queue is registered
    registry.register(processingQueue2, false);
    // Only now does the consumer stop running, forcing it to kick off another consumer
    forceRun.set(false);

    // Then:
    assertThatEventually(registry::latestNumRegistered, is(1));
    assertThat(registry.isLatestStarted(), is(true));
    registry.unregister(processingQueue2);
    assertThatEventually(registry::latestNumRegistered, is(0));
    assertThatEventually(registry::isLatestStarted, is(false));
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
}
