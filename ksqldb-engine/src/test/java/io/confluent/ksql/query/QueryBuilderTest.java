package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.RuntimeAssignor;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
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
import io.confluent.ksql.metrics.MetricCollectors;
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
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SharedKafkaStreamsRuntime;
import io.confluent.ksql.util.TransientQueryMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.internals.namedtopology.AddNamedTopologyResult;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class QueryBuilderTest {

  private static final String STATEMENT_TEXT = "KSQL STATEMENT";
  private static final QueryId QUERY_ID = new QueryId("queryid");
  private static final QueryId QUERY_ID_2 = new QueryId("queryid-2");
  private static final Set<DataSource> SOURCES
      = ImmutableSet.of(givenSource("foo"), givenSource("bar"));
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

  private static final KeyFormat KEY_FORMAT = KeyFormat
      .nonWindowed(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of());

  private static final ValueFormat VALUE_FORMAT = ValueFormat
      .of(FormatInfo.of(FormatFactory.AVRO.name()), SerdeFeatures.of());

  private static final PhysicalSchema SINK_PHYSICAL_SCHEMA = PhysicalSchema.from(
      SINK_SCHEMA,
      SerdeFeatures.of(),
      SerdeFeatures.of()
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
  private KafkaStreamsBuilder kafkaStreamsBuilder;
  @Mock
  private StreamsBuilder streamsBuilder;
  @Mock
  private NamedTopologyBuilder namedTopologyBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper;
  @Mock
  private Topology topology;
  @Mock
  private NamedTopology namedTopology;
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
  @Mock
  private SessionConfig config;
  @Mock
  private QueryMetadata.Listener queryListener;
  @Captor
  private ArgumentCaptor<Map<String, Object>> propertyCaptor;
  @Mock
  private AddNamedTopologyResult addNamedTopologyResult;
  @Mock
  private KafkaFuture<Void> future;
  private RuntimeAssignor runtimeAssignor;

  private QueryBuilder queryBuilder;
  private final Stacker stacker = new Stacker();
  private List<SharedKafkaStreamsRuntime> sharedKafkaStreamsRuntimes;

  @Before
  public void setup() {
    when(sink.getSchema()).thenReturn(SINK_SCHEMA);
    when(sink.getKsqlTopic()).thenReturn(ksqlTopic);
    when(sink.getName()).thenReturn(SINK_NAME);
    when(sink.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(ksqlTopic.getKeyFormat()).thenReturn(KEY_FORMAT);
    when(ksqlTopic.getValueFormat()).thenReturn(VALUE_FORMAT);
    when(kafkaStreamsBuilder.build(any(), any())).thenReturn(kafkaStreams);
    when(kafkaStreamsBuilder.buildNamedTopologyWrapper(any())).thenReturn(kafkaStreamsNamedTopologyWrapper);
    when(kafkaStreamsNamedTopologyWrapper.newNamedTopologyBuilder(any(), any())).thenReturn(namedTopologyBuilder);
    when(kafkaStreamsNamedTopologyWrapper.addNamedTopology(any())).thenReturn(addNamedTopologyResult);
    when(tableHolder.getMaterializationBuilder()).thenReturn(Optional.of(materializationBuilder));
    when(materializationBuilder.build()).thenReturn(materializationInfo);
    when(materializationInfo.getStateStoreSchema()).thenReturn(aggregationSchema);
    when(materializationInfo.stateStoreName()).thenReturn(STORE_NAME);
    when(ksMaterializationFactory.create(any(), any(), any(), any(), any(), any(), any(), any(),
        any(), any()))
        .thenReturn(Optional.of(ksMaterialization));
    when(ksqlMaterializationFactory.create(any(), any(), any(), any())).thenReturn(materialization);
    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(any(), anyMap())).thenReturn(processingLogger);
    when(ksqlConfig.getKsqlStreamConfigProps(anyString())).thenReturn(Collections.emptyMap());
    when(ksqlConfig.getString(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS)).thenReturn("");
    when(ksqlConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG))
        .thenReturn(PERSISTENT_PREFIX);
    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn(SERVICE_ID);
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(false);
    when(physicalPlan.build(any())).thenReturn(tableHolder);
    when(streamsBuilder.build(any())).thenReturn(topology);
    when(namedTopologyBuilder.build()).thenReturn(namedTopology);
    when(config.getConfig(true)).thenReturn(ksqlConfig);
    when(config.getOverrides()).thenReturn(OVERRIDES);
    sharedKafkaStreamsRuntimes = new ArrayList<>();

    queryBuilder = new QueryBuilder(
        config,
        processingLogContext,
        serviceContext,
        functionRegistry,
        kafkaStreamsBuilder,
        new MaterializationProviderBuilderFactory(
            ksqlConfig,
            serviceContext,
            ksMaterializationFactory,
            ksqlMaterializationFactory
        ),
        sharedKafkaStreamsRuntimes,
        true);

    runtimeAssignor = new RuntimeAssignor(ksqlConfig);
  }

  @Test
  public void shouldBuildTransientQueryCorrectly() {
    // Given:
    givenTransientQuery();

    // When:
    final TransientQueryMetadata queryMetadata = queryBuilder.buildTransientQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        SOURCES.stream().map(DataSource::getName).collect(Collectors.toSet()),
        physicalPlan,
        SUMMARY,
        TRANSIENT_SINK_SCHEMA,
        LIMIT,
        Optional.empty(),
        false,
        queryListener,
        streamsBuilder,
        Optional.empty(),
        new MetricCollectors()
    );
    queryMetadata.initialize();

    // Then:
    assertThat(queryMetadata.getStatementString(), equalTo(STATEMENT_TEXT));
    assertThat(queryMetadata.getSourceNames(), equalTo(SOURCES.stream()
        .map(DataSource::getName).collect(Collectors.toSet())));
    assertThat(queryMetadata.getExecutionPlan(), equalTo(SUMMARY));
    assertThat(queryMetadata.getTopology(), is(topology));
    assertThat(queryMetadata.getOverriddenProperties(), equalTo(OVERRIDES));
    verify(kafkaStreamsBuilder).build(any(), propertyCaptor.capture());
    assertThat(queryMetadata.getStreamsProperties(), equalTo(propertyCaptor.getValue()));
    assertThat(queryMetadata.getStreamsProperties().get(InternalConfig.TOPIC_PREFIX_ALTERNATIVE), nullValue());
  }

  @Test
  public void shouldBuildCreateAsPersistentQueryCorrectly() {
    // Given:
    final ProcessingLogger uncaughtProcessingLogger = mock(ProcessingLogger.class);
    when(processingLoggerFactory.getLogger(
        QueryLoggerUtil.queryLoggerName(QUERY_ID, new QueryContext.Stacker()
            .push("ksql.logger.thread.exception.uncaught").getQueryContext()),
        Collections.singletonMap("query-id", QUERY_ID.toString()))
    ).thenReturn(uncaughtProcessingLogger);

    // When:
    final PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    );
    queryMetadata.initialize();

    // Then:
    assertThat(queryMetadata.getStatementString(), equalTo(STATEMENT_TEXT));
    assertThat(queryMetadata.getQueryId(), equalTo(QUERY_ID));
    assertThat(queryMetadata.getSinkName().get(), equalTo(SINK_NAME));
    assertThat(queryMetadata.getPhysicalSchema(), equalTo(SINK_PHYSICAL_SCHEMA));
    assertThat(queryMetadata.getResultTopic(), is(Optional.of(ksqlTopic)));
      assertThat(queryMetadata.getSourceNames(), equalTo(SOURCES.stream()
          .map(DataSource::getName).collect(Collectors.toSet())));
    assertThat(queryMetadata.getDataSourceType().get(), equalTo(DataSourceType.KSTREAM));
    assertThat(queryMetadata.getExecutionPlan(), equalTo(SUMMARY));
    assertThat(queryMetadata.getTopology(), is(topology));
    assertThat(queryMetadata.getOverriddenProperties(), equalTo(OVERRIDES));
    assertThat(queryMetadata.getStreamsProperties(), equalTo(capturedStreamsProperties()));
    assertThat(queryMetadata.getProcessingLogger(), equalTo(uncaughtProcessingLogger));
    assertThat(queryMetadata.getPersistentQueryType(),
        equalTo(KsqlConstants.PersistentQueryType.CREATE_AS));
    // queries in dedicated runtimes must not include alternative topic prefix
    assertThat(
        queryMetadata.getStreamsProperties().get(InternalConfig.TOPIC_PREFIX_ALTERNATIVE),
        is(nullValue())
    );
  }

  @Test
  public void shouldBuildSharedCreateAsPersistentQueryCorrectly() {
    // Given:
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);
    final ProcessingLogger uncaughtProcessingLogger = mock(ProcessingLogger.class);
    when(processingLoggerFactory.getLogger(
        QueryLoggerUtil.queryLoggerName(QUERY_ID, new QueryContext.Stacker()
            .push("ksql.logger.thread.exception.uncaught").getQueryContext()),
        Collections.singletonMap("query-id", QUERY_ID.toString()))
    ).thenReturn(uncaughtProcessingLogger);

    // When:
    final PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    );
    queryMetadata.initialize();

    // Then:
    assertThat(queryMetadata.getStatementString(), equalTo(STATEMENT_TEXT));
    assertThat(queryMetadata.getQueryId(), equalTo(QUERY_ID));
    assertThat(queryMetadata.getSinkName().get(), equalTo(SINK_NAME));
    assertThat(queryMetadata.getPhysicalSchema(), equalTo(SINK_PHYSICAL_SCHEMA));
    assertThat(queryMetadata.getResultTopic(), is(Optional.of(ksqlTopic)));
    assertThat(queryMetadata.getSourceNames(), equalTo(SOURCES.stream()
        .map(DataSource::getName).collect(Collectors.toSet())));
    assertThat(queryMetadata.getDataSourceType().get(), equalTo(DataSourceType.KSTREAM));
    assertThat(queryMetadata.getExecutionPlan(), equalTo(SUMMARY));
    assertThat(queryMetadata.getTopology(), is(namedTopology));
    assertThat(queryMetadata.getOverriddenProperties(), equalTo(OVERRIDES));
    assertThat(queryMetadata.getProcessingLogger(), equalTo(uncaughtProcessingLogger));
    assertThat(queryMetadata.getPersistentQueryType(),
        equalTo(KsqlConstants.PersistentQueryType.CREATE_AS));
    // queries in dedicated runtimes must not include alternative topic prefix
    assertThat(
        queryMetadata.getStreamsProperties().get(InternalConfig.TOPIC_PREFIX_ALTERNATIVE),
        is("_confluent-ksql-service-query")
    );
  }

  @Test
  public void shouldBuildTransientQueryWithSharedRutimesCorrectly() {
    // Given:
    givenTransientQuery();
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);

    // When:
    final TransientQueryMetadata queryMetadata = queryBuilder.buildTransientQuery(
        STATEMENT_TEXT,
        QUERY_ID,
        SOURCES.stream().map(DataSource::getName).collect(Collectors.toSet()),
        physicalPlan,
        SUMMARY,
        TRANSIENT_SINK_SCHEMA,
        LIMIT,
        Optional.empty(),
        false,
        queryListener,
        streamsBuilder,
        Optional.empty(),
        new MetricCollectors()
    );
    queryMetadata.initialize();

    // Then:
    assertThat(queryMetadata.getStatementString(), equalTo(STATEMENT_TEXT));
    assertThat(queryMetadata.getSourceNames(), equalTo(SOURCES.stream()
        .map(DataSource::getName).collect(Collectors.toSet())));
    assertThat(queryMetadata.getExecutionPlan(), equalTo(SUMMARY));
    assertThat(queryMetadata.getTopology(), is(topology));
    assertThat(queryMetadata.getOverriddenProperties(), equalTo(OVERRIDES));
    verify(kafkaStreamsBuilder).build(any(), propertyCaptor.capture());
    assertThat(queryMetadata.getStreamsProperties(), equalTo(propertyCaptor.getValue()));
    assertThat(queryMetadata.getStreamsProperties().get(InternalConfig.TOPIC_PREFIX_ALTERNATIVE), nullValue());
  }

  @Test
  public void shouldBuildDedicatedCreateAsPersistentQueryWithSharedRuntimeCorrectly() {
    // Given:
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);
    final ProcessingLogger uncaughtProcessingLogger = mock(ProcessingLogger.class);
    when(processingLoggerFactory.getLogger(
        QueryLoggerUtil.queryLoggerName(QUERY_ID, new QueryContext.Stacker()
            .push("ksql.logger.thread.exception.uncaught").getQueryContext()),
        Collections.singletonMap("query-id", QUERY_ID.toString()))
    ).thenReturn(uncaughtProcessingLogger);

    // When:
    final PersistentQueryMetadata queryMetadata =  queryBuilder.buildPersistentQueryInDedicatedRuntime(
        ksqlConfig,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        STATEMENT_TEXT,
        QUERY_ID,
        Optional.of(sink),
        SOURCES,
        physicalPlan,
        SUMMARY,
        queryListener,
        ArrayList::new,
        streamsBuilder,
        new MetricCollectors()
    );
    queryMetadata.initialize();

    // Then:
    assertThat(queryMetadata.getStatementString(), equalTo(STATEMENT_TEXT));
    assertThat(queryMetadata.getQueryId(), equalTo(QUERY_ID));
    assertThat(queryMetadata.getSinkName().get(), equalTo(SINK_NAME));
    assertThat(queryMetadata.getPhysicalSchema(), equalTo(SINK_PHYSICAL_SCHEMA));
    assertThat(queryMetadata.getResultTopic(), is(Optional.of(ksqlTopic)));
    assertThat(queryMetadata.getSourceNames(), equalTo(SOURCES.stream()
        .map(DataSource::getName).collect(Collectors.toSet())));
    assertThat(queryMetadata.getDataSourceType().get(), equalTo(DataSourceType.KSTREAM));
    assertThat(queryMetadata.getExecutionPlan(), equalTo(SUMMARY));
    assertThat(queryMetadata.getTopology(), is(topology));
    assertThat(queryMetadata.getOverriddenProperties(), equalTo(OVERRIDES));
    assertThat(queryMetadata.getProcessingLogger(), equalTo(uncaughtProcessingLogger));
    assertThat(queryMetadata.getPersistentQueryType(),
        equalTo(KsqlConstants.PersistentQueryType.CREATE_AS));
    // queries in dedicated runtimes must not include alternative topic prefix
    assertThat(
        queryMetadata.getStreamsProperties().get(InternalConfig.TOPIC_PREFIX_ALTERNATIVE),
        is(nullValue())
    );
  }

  @Test
  public void shouldBuildInsertPersistentQueryCorrectly() {
    // Given:
    final ProcessingLogger uncaughtProcessingLogger = mock(ProcessingLogger.class);
    when(processingLoggerFactory.getLogger(
        QueryLoggerUtil.queryLoggerName(QUERY_ID, new QueryContext.Stacker()
            .push("ksql.logger.thread.exception.uncaught").getQueryContext()),
        Collections.singletonMap("query-id", QUERY_ID.toString()))
    ).thenReturn(uncaughtProcessingLogger);

    // When:
    final PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.INSERT,
        QUERY_ID);
    queryMetadata.initialize();

    // Then:
    assertThat(queryMetadata.getStatementString(), equalTo(STATEMENT_TEXT));
    assertThat(queryMetadata.getQueryId(), equalTo(QUERY_ID));
    assertThat(queryMetadata.getSinkName().get(), equalTo(SINK_NAME));
    assertThat(queryMetadata.getPhysicalSchema(), equalTo(SINK_PHYSICAL_SCHEMA));
    assertThat(queryMetadata.getResultTopic(), is(Optional.of(ksqlTopic)));
      assertThat(queryMetadata.getSourceNames(), equalTo(SOURCES.stream()
          .map(DataSource::getName).collect(Collectors.toSet())));
    assertThat(queryMetadata.getDataSourceType().get(), equalTo(DataSourceType.KSTREAM));
    assertThat(queryMetadata.getExecutionPlan(), equalTo(SUMMARY));
    assertThat(queryMetadata.getTopology(), is(topology));
    assertThat(queryMetadata.getOverriddenProperties(), equalTo(OVERRIDES));
    assertThat(queryMetadata.getStreamsProperties(), equalTo(capturedStreamsProperties()));
    assertThat(queryMetadata.getProcessingLogger(), equalTo(uncaughtProcessingLogger));
    assertThat(queryMetadata.getPersistentQueryType(),
        equalTo(KsqlConstants.PersistentQueryType.INSERT));
    assertThat(queryMetadata.getStreamsProperties().get(InternalConfig.TOPIC_PREFIX_ALTERNATIVE), nullValue());
  }

  @Test
  public void shouldBuildPersistentQueryWithCorrectStreamsApp() {
    // When:
    final PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    );
    queryMetadata.initialize();
    queryMetadata.start();

    // Then:
    verify(kafkaStreams).start();
  }

  @Test
  public void shouldStartCreateSourceQueryWithMaterializationProvider() {
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);

    // Given:
    final DataSource source = givenSource("foo");
    when(source.getSchema()).thenReturn(SINK_SCHEMA);
    when(source.getKsqlTopic()).thenReturn(ksqlTopic);
    final PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        ImmutableSet.of(source),
        KsqlConstants.PersistentQueryType.CREATE_SOURCE,
        QUERY_ID,
        Optional.empty()
    );
    queryMetadata.initialize();
    queryMetadata.register();
    queryMetadata.start();

    // When:
    final Optional<Materialization> result = queryMetadata.getMaterialization(QUERY_ID, stacker);

    // Then:
    assertThat(result.get(), is(materialization));
    assertThat(queryMetadata.getStreamsProperties().get(InternalConfig.TOPIC_PREFIX_ALTERNATIVE),
        is("_confluent-ksql-service-query"));
  }

  @Test
  public void shouldStartPersistentQueryWithCorrectMaterializationProvider() {
    // Given:
    final PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    );
    queryMetadata.initialize();
    queryMetadata.start();

    // When:
    final Optional<Materialization> result = queryMetadata.getMaterialization(QUERY_ID, stacker);

    // Then:
    assertThat(result.get(), is(materialization));
  }

  @Test
  public void shouldCreateKSMaterializationCorrectly() {
    // When:
    buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    ).initialize();

    // Then:
    final Map<String, Object> properties = capturedStreamsProperties();
    verify(ksMaterializationFactory).create(
        eq(STORE_NAME),
        same(kafkaStreams),
        same(topology),
        same(aggregationSchema),
        any(),
        eq(Optional.empty()),
        eq(properties),
        eq(ksqlConfig),
        any(),
        any()
    );
  }

  @Test
  public void shouldMaterializeCorrectlyOnStart() {
    // Given:
    final PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    );
    queryMetadata.initialize();
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

    final PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    );
    queryMetadata.initialize();

    // When:
    final Optional<Materialization> result = queryMetadata.getMaterialization(QUERY_ID, stacker);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldCreateExpectedServiceId() {
    // When:
    buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    ).initialize();

    // Then:
    assertThat(
        capturedStreamsProperties().get(StreamsConfig.APPLICATION_ID_CONFIG),
        equalTo("_confluent-ksql-service-" + PERSISTENT_PREFIX + "queryid")
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldAddMetricsInterceptorsToStreamsConfig() {
    // When:
    buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    ).initialize();

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
    final PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    );
    queryMetadata.initialize();
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
    buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    ).initialize();

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
    buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    ).initialize();

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
    buildPersistentQuery(SOURCES, KsqlConstants.PersistentQueryType.CREATE_AS, QUERY_ID).initialize();

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
  public void shouldMakePersistentQueriesWithSameSources() {
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);

    // When:
    buildPersistentQuery(SOURCES, KsqlConstants.PersistentQueryType.CREATE_AS, QUERY_ID);
    buildPersistentQuery(SOURCES, KsqlConstants.PersistentQueryType.CREATE_AS, QUERY_ID_2);

    assertThat("chose same source", sharedKafkaStreamsRuntimes.size() > 1);
  }

  @Test
  public void shouldMakePersistentQueriesWithDifferentSources() {
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);

    // When:
    PersistentQueryMetadata queryMetadata = buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID);

    PersistentQueryMetadata queryMetadata2 = buildPersistentQuery(
            ImmutableSet.of(givenSource("food"), givenSource("bard")),
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID);
    assertThat("did not chose the same runtime", queryMetadata.getKafkaStreams().equals(queryMetadata2.getKafkaStreams()));

  }

  @Test
  public void shouldConfigureProducerErrorHandler() {
    final ProcessingLogger logger = mock(ProcessingLogger.class);
    when(processingLoggerFactory.getLogger(QUERY_ID.toString(), Collections.singletonMap("query-id", QUERY_ID.toString()))).thenReturn(logger);

    // When:
    buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    ).initialize();

    // Then:
    final Map<String, Object> streamsProps = capturedStreamsProperties();
    assertThat(
        streamsProps.get(ProductionExceptionHandlerUtil.KSQL_PRODUCTION_ERROR_LOGGER),
        is(logger));
  }

  @Test
  public void shouldConfigureCustomMetricsTags() {
    // Given:
    when(ksqlConfig.getString(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS)).thenReturn("custom-tag:custom-tag-value");

    // When:
    buildPersistentQuery(
        SOURCES,
        KsqlConstants.PersistentQueryType.CREATE_AS,
        QUERY_ID
    ).initialize();

    // Then:
    final Map<String, Object> streamsProps = capturedStreamsProperties();
    assertThat(
        streamsProps.get(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS),
        is("custom-tag:custom-tag-value"));
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

  private PersistentQueryMetadata buildPersistentQuery(final Set<DataSource> sources,
                                                       final KsqlConstants.PersistentQueryType persistentQueryType,
                                                       final QueryId queryId) {
    return buildPersistentQuery(sources, persistentQueryType, queryId, Optional.of(sink));
  }

  private PersistentQueryMetadata buildPersistentQuery(final Set<DataSource> sources,
                                                       final KsqlConstants.PersistentQueryType persistentQueryType,
                                                       final QueryId queryId,
                                                       final Optional<DataSource> sink) {
    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)) {
      return queryBuilder.buildPersistentQueryInSharedRuntime(
          ksqlConfig,
          persistentQueryType,
          STATEMENT_TEXT,
          queryId,
          sink,
          sources,
          physicalPlan,
          SUMMARY,
          queryListener,
          ArrayList::new,
          runtimeAssignor.getRuntimeAndMaybeAddRuntime(queryId,
              sources.stream().map(s -> s.getName().toString()).collect(Collectors.toSet()),
              config.getConfig(true)),
          new MetricCollectors()
      );
    } else {
      return queryBuilder.buildPersistentQueryInDedicatedRuntime(
          ksqlConfig,
          persistentQueryType,
          STATEMENT_TEXT,
          queryId,
          sink,
          SOURCES,
          physicalPlan,
          SUMMARY,
          queryListener,
          ArrayList::new,
          streamsBuilder,
          new MetricCollectors()
      );
    }
  }

  private static DataSource givenSource(final String name) {
    final DataSource source = Mockito.mock(DataSource.class);
    when(source.getName()).thenReturn(SourceName.of(name));
    return source;
  }
}