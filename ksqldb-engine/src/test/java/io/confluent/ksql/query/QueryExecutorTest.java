package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterialization;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializationFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryExecutorTest {

  private static final String STATEMENT_TEXT = "KSQL STATEMENT";
  private static final QueryId QUERY_ID = new QueryId("queryid");
  private static final QueryId QUERY_ID_2 = new QueryId("queryid-2");
  private static final Set<SourceName> SOURCES
      = ImmutableSet.of(SourceName.of("foo"), SourceName.of("bar"));
  private static final SourceName SINK_NAME = SourceName.of("baz");
  private static final String STORE_NAME = "store";
  private static final String SUMMARY = "summary";
  private static final Map<String, Object> OVERRIDES = Collections.emptyMap();

  private static final LogicalSchema SINK_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("col0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("col1"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema TRANSIENT_SINK_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("col0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("col1"), SqlTypes.STRING)
      .build();

  private static final KeyFormat KEY_FORMAT = KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.JSON.name()));
  private static final PhysicalSchema SINK_PHYSICAL_SCHEMA = PhysicalSchema.from(
      SINK_SCHEMA,
      SerdeOptions.of()
  );
  private static final OptionalInt LIMIT = OptionalInt.of(123);
  private static final String SERVICE_ID = "service-";
  private static final String PERSISTENT_PREFIX = "persistent-";

  @Mock
  private ExecutionStep physicalPlan;
  @Mock
  private KStream<Struct, GenericRow> kstream;
  @Mock
  private DataSource sink;
  @Mock
  private KsqlTopic ksqlTopic;
  @Mock
  private MaterializationInfo.Builder materializationBuilder;
  @Mock
  private MaterializationInfo materializationInfo;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private Consumer<QueryMetadata> closeCallback;
  @Mock
  private KafkaStreamsBuilder kafkaStreamsBuilder;
  @Mock
  private StreamsBuilder streamsBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private Topology topology;
  @Mock
  private TopologyDescription topoDesc;
  @Mock
  private KsMaterializationFactory ksMaterializationFactory;
  @Mock
  private KsMaterialization ksMaterialization;
  @Mock
  private KsqlMaterializationFactory ksqlMaterializationFactory;
  @Mock
  private Materialization materialization;
  @Mock
  private LogicalSchema aggregationSchema;
  @Mock
  private KTableHolder<Struct> tableHolder;
  @Mock
  private KStreamHolder<Struct> streamHolder;
  @Captor
  private ArgumentCaptor<Map<String, Object>> propertyCaptor;

  private QueryExecutor queryBuilder;
  private final Stacker stacker = new Stacker();

  @Before
  public void setup() {
    when(sink.getSchema()).thenReturn(SINK_SCHEMA);
    when(sink.getSerdeOptions()).thenReturn(SerdeOptions.of());
    when(sink.getKsqlTopic()).thenReturn(ksqlTopic);
    when(sink.getName()).thenReturn(SINK_NAME);
    when(sink.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(ksqlTopic.getKeyFormat()).thenReturn(KEY_FORMAT);
    when(kafkaStreamsBuilder.build(any(), any())).thenReturn(kafkaStreams);
    when(tableHolder.getMaterializationBuilder()).thenReturn(Optional.of(materializationBuilder));
    when(materializationBuilder.build()).thenReturn(materializationInfo);
    when(materializationInfo.getStateStoreSchema()).thenReturn(aggregationSchema);
    when(materializationInfo.stateStoreName()).thenReturn(STORE_NAME);
    when(ksMaterializationFactory.create(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(ksMaterialization));
    when(ksqlMaterializationFactory.create(any(), any(), any(), any())).thenReturn(materialization);
    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(any())).thenReturn(processingLogger);
    when(ksqlConfig.getKsqlStreamConfigProps(anyString())).thenReturn(Collections.emptyMap());
    when(ksqlConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG))
        .thenReturn(PERSISTENT_PREFIX);
    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn(SERVICE_ID);
    when(physicalPlan.build(any())).thenReturn(tableHolder);
    when(topology.describe()).thenReturn(topoDesc);
    when(topoDesc.subtopologies()).thenReturn(ImmutableSet.of());
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(streamsBuilder.build(any())).thenReturn(topology);
    queryBuilder = new QueryExecutor(
        ksqlConfig,
        OVERRIDES,
        processingLogContext,
        serviceContext,
        functionRegistry,
        closeCallback,
        kafkaStreamsBuilder,
        streamsBuilder,
        new MaterializationProviderBuilderFactory(
            ksqlConfig,
            serviceContext,
            ksMaterializationFactory,
            ksqlMaterializationFactory
        )
    );
  }

  @Test
  public void shouldBuildTransientQueryCorrectly() {
    // Given:
    givenTransientQuery();

    // When:
    final TransientQueryMetadata queryMetadata = queryBuilder.buildTransientQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        SOURCES,
        physicalPlan,
        SUMMARY,
        TRANSIENT_SINK_SCHEMA,
        LIMIT
    );

    // Then:
    assertThat(queryMetadata.getStatementString(), equalTo(STATEMENT_TEXT));
    assertThat(queryMetadata.getSourceNames(), equalTo(SOURCES));
    assertThat(queryMetadata.getExecutionPlan(), equalTo(SUMMARY));
    assertThat(queryMetadata.getTopology(), is(topology));
    assertThat(queryMetadata.getOverriddenProperties(), equalTo(OVERRIDES));
    verify(kafkaStreamsBuilder).build(any(), propertyCaptor.capture());
    assertThat(queryMetadata.getStreamsProperties(), equalTo(propertyCaptor.getValue()));
  }

  @Test
  public void shouldBuildPersistentQueryCorrectly() {
    // When:
    final PersistentQueryMetadata queryMetadata = queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );

    // Then:
    assertThat(queryMetadata.getStatementString(), equalTo(STATEMENT_TEXT));
    assertThat(queryMetadata.getQueryId(), equalTo(QUERY_ID));
    assertThat(queryMetadata.getSinkName(), equalTo(SINK_NAME));
    assertThat(queryMetadata.getPhysicalSchema(), equalTo(SINK_PHYSICAL_SCHEMA));
    assertThat(queryMetadata.getResultTopic(), is(ksqlTopic));
    assertThat(queryMetadata.getSourceNames(), equalTo(SOURCES));
    assertThat(queryMetadata.getDataSourceType(), equalTo(DataSourceType.KSTREAM));
    assertThat(queryMetadata.getExecutionPlan(), equalTo(SUMMARY));
    assertThat(queryMetadata.getTopology(), is(topology));
    assertThat(queryMetadata.getOverriddenProperties(), equalTo(OVERRIDES));
    assertThat(queryMetadata.getStreamsProperties(), equalTo(capturedStreamsProperties()));
  }

  @Test
  public void shouldBuildPersistentQueryWithCorrectStreamsApp() {
    // When:
    final PersistentQueryMetadata queryMetadata = queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );
    queryMetadata.start();

    // Then:
    verify(kafkaStreams).start();
  }

  @Test
  public void shouldStartPersistentQueryWithCorrectMaterializationProvider() {
    // Given:
    final PersistentQueryMetadata queryMetadata = queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );
    queryMetadata.start();

    // When:
    final Optional<Materialization> result = queryMetadata.getMaterialization(QUERY_ID, stacker);

    // Then:
    assertThat(result.get(), is(materialization));
  }

  @Test
  public void shouldCreateKSMaterializationCorrectly() {
    // When:
    queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );

    // Then:
    final Map<String, Object> properties = capturedStreamsProperties();
    verify(ksMaterializationFactory).create(
        eq(STORE_NAME),
        same(kafkaStreams),
        same(aggregationSchema),
        any(),
        eq(Optional.empty()),
        eq(properties),
        eq(ksqlConfig),
        any()
    );
  }

  @Test
  public void shouldMaterializeCorrectlyOnStart() {
    // Given:
    final PersistentQueryMetadata queryMetadata = queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );
    queryMetadata.start();

    // When:
    queryMetadata.getMaterialization(QUERY_ID_2, stacker);

    // Then:
    verify(ksqlMaterializationFactory).create(
        ksMaterialization,
        materializationInfo,
        QUERY_ID_2,
        stacker
    );
  }

  @Test
  public void shouldNotIncludeMaterializationProviderIfNoMaterialization() {
    // Given:
    when(tableHolder.getMaterializationBuilder()).thenReturn(Optional.empty());

    final PersistentQueryMetadata queryMetadata = queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );

    // When:
    final Optional<Materialization> result = queryMetadata.getMaterialization(QUERY_ID, stacker);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldCreateExpectedServiceId() {
    // When:
    queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );

    // Then:
    assertThat(
        capturedStreamsProperties().get(StreamsConfig.APPLICATION_ID_CONFIG),
        equalTo("_confluent-ksql-service-persistent-queryid")
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldAddMetricsInterceptorsToStreamsConfig() {
    // When:
    queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );

    // Then:
    final Map<String, Object> streamsProperties = capturedStreamsProperties();
    final List<String> ciList = (List<String>) streamsProperties.get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
    );
    assertThat(ciList, contains(ConsumerCollector.class.getName()));

    final List<String> piList = (List<String>) streamsProperties.get(
        StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
    );
    assertThat(piList, contains(ProducerCollector.class.getName()));
  }

  private void shouldUseProvidedOptimizationConfig(final Object value) {
    // Given:
    final Map<String, Object> properties =
        Collections.singletonMap(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, value);
    when(ksqlConfig.getKsqlStreamConfigProps(anyString())).thenReturn(properties);

    // When:
    final PersistentQueryMetadata queryMetadata = queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );
    queryMetadata.start();

    // Then:
    final Map<String, Object> captured = capturedStreamsProperties();
    assertThat(captured.get(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG), equalTo(value));
  }

  @Test
  public void shouldUseOptimizationConfigProvidedWhenOn() {
    shouldUseProvidedOptimizationConfig(StreamsConfig.OPTIMIZE);
  }

  @Test
  public void shouldUseOptimizationConfigProvidedWhenOff() {
    shouldUseProvidedOptimizationConfig(StreamsConfig.NO_OPTIMIZATION);
  }

  @SuppressWarnings("unchecked")
  private void assertPropertiesContainDummyInterceptors() {
    final Map<String, Object> streamsProperties = capturedStreamsProperties();
    final List<String> cInterceptors = (List<String>) streamsProperties.get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    assertThat(
        cInterceptors,
        contains(
            DummyConsumerInterceptor.class.getName(),
            ConsumerCollector.class.getName()
        )
    );
    final List<String> pInterceptors = (List<String>) streamsProperties.get(
        StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG));
    assertThat(
        pInterceptors,
        contains(
            DummyProducerInterceptor.class.getName(),
            ProducerCollector.class.getName()
        )
    );
  }

  @Test
  public void shouldAddMetricsInterceptorsToExistingList() {
    // Given:
    when(ksqlConfig.getKsqlStreamConfigProps(anyString())).thenReturn(ImmutableMap.of(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        ImmutableList.of(DummyConsumerInterceptor.class.getName()),
        StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        ImmutableList.of(DummyProducerInterceptor.class.getName())
    ));

    // When:
    queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );

    // Then:
    assertPropertiesContainDummyInterceptors();
  }

  @Test
  public void shouldAddMetricsInterceptorsToExistingString() {
    // When:
    when(ksqlConfig.getKsqlStreamConfigProps(anyString())).thenReturn(ImmutableMap.of(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        DummyConsumerInterceptor.class.getName(),
        StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        DummyProducerInterceptor.class.getName()
    ));

    // When:
    queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );

    // Then:
    assertPropertiesContainDummyInterceptors();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldAddMetricsInterceptorsToExistingStringList() {
    // When:
    when(ksqlConfig.getKsqlStreamConfigProps(anyString())).thenReturn(ImmutableMap.of(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        DummyConsumerInterceptor.class.getName()
            + ","
            + DummyConsumerInterceptor2.class.getName()
    ));

    // When:
    queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );

    // Then:
    final Map<String, Object> streamsProperties = capturedStreamsProperties();
    final List<String> cInterceptors = (List<String>) streamsProperties.get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    assertThat(
        cInterceptors,
        contains(
            DummyConsumerInterceptor.class.getName(),
            DummyConsumerInterceptor2.class.getName(),
            ConsumerCollector.class.getName()
        )
    );
  }

  @Test
  public void shouldConfigureProducerErrorHandler() {
    final ProcessingLogger logger = mock(ProcessingLogger.class);
    when(processingLoggerFactory.getLogger(QUERY_ID.toString())).thenReturn(logger);

    // When:
    queryBuilder.buildPersistentQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        sink,
        SOURCES,
        physicalPlan,
        SUMMARY
    );

    // Then:
    final Map<String, Object> streamsProps = capturedStreamsProperties();
    assertThat(
        streamsProps.get(ProductionExceptionHandlerUtil.KSQL_PRODUCTION_ERROR_LOGGER),
        is(logger));
  }

  public static class DummyConsumerInterceptor implements ConsumerInterceptor {

    public ConsumerRecords onConsume(final ConsumerRecords consumerRecords) {
      return consumerRecords;
    }

    public void close() {
    }

    public void onCommit(final Map map) {
    }

    public void configure(final Map<String, ?> map) {
    }
  }

  public static class DummyProducerInterceptor implements ProducerInterceptor {

    public void onAcknowledgement(final RecordMetadata rm, final Exception e) {
    }

    public ProducerRecord onSend(final ProducerRecord producerRecords) {
      return producerRecords;
    }

    public void close() {
    }

    public void configure(final Map<String, ?> map) {
    }
  }

  public static class DummyConsumerInterceptor2 implements ConsumerInterceptor {

    public ConsumerRecords onConsume(final ConsumerRecords consumerRecords) {
      return consumerRecords;
    }

    public void close() {
    }

    public void onCommit(final Map map) {
    }

    public void configure(final Map<String, ?> map) {
    }
  }

  private Map<String, Object> capturedStreamsProperties() {
    verify(kafkaStreamsBuilder).build(any(), propertyCaptor.capture());
    return propertyCaptor.getValue();
  }

  private void givenTransientQuery() {
    when(physicalPlan.build(any())).thenReturn(streamHolder);
    when(streamHolder.getStream()).thenReturn(kstream);
  }
}