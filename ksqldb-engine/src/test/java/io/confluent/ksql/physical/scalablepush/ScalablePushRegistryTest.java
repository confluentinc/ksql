package io.confluent.ksql.physical.scalablepush;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScalablePushRegistryTest {

  private static final List<?> KEY = ImmutableList.of(1, "foo");
  private static final List<?> VALUE = ImmutableList.of(4.9, 10);
  private static LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k1"), SqlTypes.INTEGER)
      .keyColumn(ColumnName.of("k2"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("v2"), SqlTypes.INTEGER)
      .build();
  private static final long TIMESTAMP = 123;

  @Mock
  private KStream<?, GenericRow> stream;
  @Mock
  private PushLocator locator;
  @Mock
  private ProcessingQueue processingQueue;
  @Mock
  private ProcessorContext processorContext;
  @Mock
  private GenericKey genericKey;
  @Mock
  private GenericRow genericRow;
  @Mock
  private Windowed<GenericKey> windowed;
  @Mock
  private Window window;

  @Before
  public void setUp() {
    when(processingQueue.getQueryId()).thenReturn(new QueryId("abc"));
  }

  @Test
  public void shouldRegisterAndGetQueueOffer_nonWindowed() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(locator, SCHEMA, false);
    when(genericKey.values()).thenAnswer(a -> KEY);
    when(genericRow.values()).thenAnswer(a -> VALUE);
    when(processorContext.timestamp()).thenReturn(TIMESTAMP);

    // When:
    registry.register(processingQueue);
    assertThat(registry.numRegistered(), is(1));

    // Then:
    final Processor<Object, GenericRow> processor = registry.get();
    processor.init(processorContext);
    processor.process(genericKey, genericRow);
    verify(processingQueue).offer(
        Row.of(SCHEMA, GenericKey.fromList(KEY), GenericRow.fromList(VALUE), TIMESTAMP));
    registry.unregister(processingQueue);
    assertThat(registry.numRegistered(), is(0));
  }

  @Test
  public void shouldRegisterAndGetQueueOffer_windowed() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(locator, SCHEMA, true);
    when(genericKey.values()).thenAnswer(a -> KEY);
    when(genericRow.values()).thenAnswer(a -> VALUE);
    when(processorContext.timestamp()).thenReturn(TIMESTAMP);
    when(windowed.window()).thenReturn(window);
    when(windowed.key()).thenReturn(genericKey);

    // When:
    registry.register(processingQueue);
    assertThat(registry.numRegistered(), is(1));

    // Then:
    final Processor<Object, GenericRow> processor = registry.get();
    processor.init(processorContext);
    processor.process(windowed, genericRow);
    verify(processingQueue).offer(
        WindowedRow.of(SCHEMA, new Windowed<>(GenericKey.fromList(KEY), window),
            GenericRow.fromList(VALUE), TIMESTAMP));
    registry.unregister(processingQueue);
    assertThat(registry.numRegistered(), is(0));
  }

  @Test
  public void shouldCatchException() {
    // Given:
    ScalablePushRegistry registry = new ScalablePushRegistry(locator, SCHEMA, false);
    when(genericKey.values()).thenAnswer(a -> KEY);
    when(genericRow.values()).thenAnswer(a -> VALUE);
    when(processorContext.timestamp()).thenReturn(TIMESTAMP);
    when(processingQueue.offer(any())).thenThrow(new RuntimeException("Error!"));

    // When:
    registry.register(processingQueue);

    // Then:
    final Processor<Object, GenericRow> processor = registry.get();
    processor.init(processorContext);
    processor.process(genericKey, genericRow);
  }

  @Test
  public void shouldCreate() {
    // When:
    final Optional<ScalablePushRegistry> registry =
        ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, "http://localhost:8088"));

    // Then:
    assertThat(registry.isPresent(), is(true));
  }

  @Test
  public void shouldCreate_badApplicationServer() {
    // When
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, 123))
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
            ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, "abc"))
    );

    // Then
    assertThat(e.getMessage(), containsString("malformed"));
  }

  @Test
  public void shouldCreate_noApplicationServer() {
    // When
    final Optional<ScalablePushRegistry> registry =
        ScalablePushRegistry.create(SCHEMA, Collections::emptyList, false,
            ImmutableMap.of());

    // Then
    assertThat(registry.isPresent(), is(false));
  }
}
