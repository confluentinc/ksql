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
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Window;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
  private static final ConsumerMetadata METADATA = new ConsumerMetadata(2);

  @Mock
  private PushLocator locator;
  @Mock
  private ProcessingQueue processingQueue;

  private Window window;
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

  @Before
  public void setUp() {
    when(ksqlTopic.getKafkaTopicName()).thenReturn(TOPIC);
    when(kafkaConsumerFactory.create(any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(kafkaConsumer);

    AtomicBoolean closed = new AtomicBoolean(false);
    doAnswer(a -> {
      while (!closed.get()) {
        Thread.sleep(10);
      }
      return null;
    }).when(latestConsumer).run();
    doAnswer(a -> {
      System.out.println("Closing latestConsumer");
      closed.set(true);
      return null;
    }).when(latestConsumer).close();
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
        any(), any())).thenReturn(latestConsumer);
    when(consumerMetadataFactory.create(any(), any()))
        .thenReturn(METADATA);
    when(ksqlTopic.getKeyFormat()).thenReturn(keyFormat);
    when(keyFormat.isWindowed()).thenReturn(false);
  }

  @Test
  public void shouldRegisterAndStartLatest() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false, ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory);

    // When:
    registry.register(processingQueue, false);
    assertThat(registry.isLatestStarted(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(1));

    // Then:
    registry.unregister(processingQueue);
    assertThat(registry.latestNumRegistered(), is(0));
  }

  @Test
  public void shouldEnforceNewNodeContinuity() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(locator, SCHEMA, true, true,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory);
    when(latestConsumer.getNumRowsReceived()).thenReturn(1L);

    // When:
    registry.register(processingQueue, false);
    assertThat(registry.isLatestStarted(), is(true));
    assertThatEventually(registry::latestNumRegistered, is(1));
    final Exception e = assertThrows(IllegalStateException.class,
        () -> registry.register(processingQueue, true));

    // Then:
    assertThat(e.getMessage(), containsString("New node missed data"));
  }

  @Test
  public void shouldCatchException_onRegister() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory);
    doThrow(new RuntimeException("Error!")).when(latestConsumer).register(any());
    AtomicBoolean isErrorQueue = new AtomicBoolean(false);
    AtomicBoolean isErrorConsumer = new AtomicBoolean(false);
    doAnswer(a -> {
      isErrorQueue.set(true);
      return null;
    }).when(processingQueue).onError();
    doAnswer(a -> {
      isErrorConsumer.set(true);
      return null;
    }).when(latestConsumer).onError();

    // When:
    registry.register(processingQueue, false);

    // Then:
    assertThatEventually(isErrorQueue::get, is(true));
    assertThatEventually(isErrorConsumer::get, is(true));
  }

  @Test
  public void shouldCatchException_onRun() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(
        locator, SCHEMA, false, false,
        ImmutableMap.of(), ksqlTopic, serviceContext,
        ksqlConfig, kafkaConsumerFactory, latestConsumerFactory, consumerMetadataFactory);
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
  public void shouldCreate() {
    // When:
    final Optional<ScalablePushRegistry> registry =
        ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, "http://localhost:8088"),
            false, ImmutableMap.of(), ksqlTopic, serviceContext, ksqlConfig);

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
            ImmutableMap.of(), ksqlTopic, serviceContext, ksqlConfig)
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
            ImmutableMap.of(), ksqlTopic, serviceContext, ksqlConfig)
    );

    // Then
    assertThat(e.getMessage(), containsString("malformed"));
  }

  @Test
  public void shouldCreate_noApplicationServer() {
    // When
    final Optional<ScalablePushRegistry> registry =
        ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of(), false, ImmutableMap.of(), ksqlTopic, serviceContext, ksqlConfig);

    // Then
    assertThat(registry.isPresent(), is(false));
  }
}
