/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.logging.processing.MeteredProcessingLoggerFactory;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.FieldInfo.FieldType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryDescriptionFactoryTest {

  private static final Map<KsqlHostInfoEntity, KsqlQueryStatus> STATUS_MAP =
      Collections.singletonMap(new KsqlHostInfoEntity("host", 8080), KsqlQueryStatus.RUNNING);
  private static final LogicalSchema TRANSIENT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema PERSISTENT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
      .build();

  private static final Map<String, Object> STREAMS_PROPS = Collections.singletonMap("k1", "v1");
  private static final Map<String, Object> PROP_OVERRIDES = Collections.singletonMap("k2", "v2");
  private static final String APPLICATION_ID = "app id";
  private static final QueryId QUERY_ID = new QueryId("queryId");
  private static final ImmutableSet<SourceName> SOURCE_NAMES = ImmutableSet.of(SourceName.of("s1"), SourceName.of("s2"));
  private static final String SQL_TEXT = "test statement";
  private static final String TOPOLOGY_TEXT = "Topology Text";
  private static final Long closeTimeout = KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT;

  @Mock
  private KafkaStreamsBuilder kafkaStreamsBuilder;
  @Mock
  private KafkaStreams queryStreams;
  @Mock
  private Topology topology;
  @Mock(name = TOPOLOGY_TEXT)
  private TopologyDescription topologyDescription;
  @Mock
  private BlockingRowQueue queryQueue;
  @Mock
  private KsqlTopic sinkTopic;
  @Mock
  private ExecutionStep<?> physicalPlan;
  @Mock
  private DataSource sinkDataSource;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private QueryMetadata.Listener listener;
  @Mock
  private MeteredProcessingLoggerFactory processingLoggerFactory;
  
  private QueryMetadata transientQuery;
  private PersistentQueryMetadata persistentQuery;
  private QueryDescription transientQueryDescription;
  private QueryDescription persistentQueryDescription;

  @Before
  public void setUp() {
    when(topology.describe()).thenReturn(topologyDescription);
    when(kafkaStreamsBuilder.build(any(), any())).thenReturn(queryStreams);
    when(queryStreams.metadataForLocalThreads()).thenReturn(Collections.emptySet());

    when(sinkTopic.getKeyFormat()).thenReturn(
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()));
    when(sinkDataSource.getKsqlTopic()).thenReturn(sinkTopic);
    when(sinkDataSource.getName()).thenReturn(SourceName.of("sink name"));

    transientQuery = new TransientQueryMetadata(
        SQL_TEXT,
        TRANSIENT_SCHEMA,
        SOURCE_NAMES,
        "execution plan",
        queryQueue,
        QUERY_ID,
        APPLICATION_ID,
        topology,
        kafkaStreamsBuilder,
        STREAMS_PROPS,
        PROP_OVERRIDES,
        closeTimeout,
        10,
        ResultType.STREAM,
        0L,
        0L,
        listener,
        processingLoggerFactory
    );
    transientQuery.initialize();

    transientQueryDescription = QueryDescriptionFactory.forQueryMetadata(transientQuery, Collections.emptyMap());

    persistentQuery = new PersistentQueryMetadataImpl(
        KsqlConstants.PersistentQueryType.CREATE_AS,
        SQL_TEXT,
        PhysicalSchema.from(PERSISTENT_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of()),
        SOURCE_NAMES,
        Optional.of(sinkDataSource),
        "execution plan",
        QUERY_ID,
        Optional.empty(),
        APPLICATION_ID,
        topology,
        kafkaStreamsBuilder,
        new QuerySchemas(),
        STREAMS_PROPS,
        PROP_OVERRIDES,
        closeTimeout,
        QueryErrorClassifier.DEFAULT_CLASSIFIER,
        physicalPlan,
        10,
        processingLogger,
        0L,
        0L,
        listener,
        Optional.empty(),
        processingLoggerFactory
    );
    persistentQuery.initialize();

    persistentQueryDescription = QueryDescriptionFactory.forQueryMetadata(persistentQuery, STATUS_MAP);
  }

  @Test
  public void shouldHaveQueryIdForTransientQuery() {
    assertThat(transientQueryDescription.getId().toString(), is(QUERY_ID.toString()));
  }

  @Test
  public void shouldHaveQueryIdForPersistentQuery() {
    assertThat(persistentQueryDescription.getId().toString(), is(QUERY_ID.toString()));
  }

  @Test
  public void shouldExposeExecutionPlan() {
    assertThat(transientQueryDescription.getExecutionPlan(), is("execution plan"));
    assertThat(persistentQueryDescription.getExecutionPlan(), is("execution plan"));
  }

  @Test
  public void shouldExposeSources() {
    assertThat(transientQueryDescription.getSources(), is(SOURCE_NAMES.stream().map(SourceName::text).collect(Collectors.toSet())));
    assertThat(persistentQueryDescription.getSources(), is(SOURCE_NAMES.stream().map(SourceName::text).collect( Collectors.toSet())));
  }

  @Test
  public void shouldExposeStatementText() {
    assertThat(transientQueryDescription.getStatementText(), is(SQL_TEXT));
    assertThat(persistentQueryDescription.getStatementText(), is(SQL_TEXT));
  }

  @Test
  public void shouldExposeTopology() {
    assertThat(transientQueryDescription.getTopology(), is(TOPOLOGY_TEXT));
    assertThat(persistentQueryDescription.getTopology(), is(TOPOLOGY_TEXT));
  }

  @Test
  public void shouldExposeOverriddenProperties() {
    assertThat(transientQueryDescription.getOverriddenProperties(), is(PROP_OVERRIDES));
    assertThat(persistentQueryDescription.getOverriddenProperties(), is(PROP_OVERRIDES));
  }

  @Test
  public void shouldExposeValueFieldsForTransientQueries() {
    assertThat(transientQueryDescription.getFields(), contains(
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null), Optional.empty()),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null), Optional.empty())));
  }

  @Test
  public void shouldExposeAllFieldsForPersistentQueries() {
    assertThat(persistentQueryDescription.getFields(), contains(
        new FieldInfo("k0", new SchemaInfo(SqlBaseType.STRING, null, null), Optional.of(FieldType.KEY)),
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null), Optional.empty()),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null), Optional.empty())));
  }

  @Test
  public void shouldReportPersistentQueriesStatus() {
    assertThat(persistentQueryDescription.getState(), is(Optional.of("RUNNING")));

    final Map<KsqlHostInfoEntity, KsqlQueryStatus> updatedStatusMap = new HashMap<>(STATUS_MAP);
    updatedStatusMap.put(new KsqlHostInfoEntity("anotherhost", 8080), KsqlQueryStatus.ERROR);
    final QueryDescription updatedPersistentQueryDescription =
        QueryDescriptionFactory.forQueryMetadata(persistentQuery, updatedStatusMap);
    assertThat(updatedPersistentQueryDescription.getState(), is(Optional.of("ERROR")));
  }

  @Test
  public void shouldNotReportTransientQueriesStatus() {
    assertThat(transientQueryDescription.getState(), is(Optional.empty()));
  }

  @Test
  public void shouldHavePersistentQueryTypePersistentQueryDescription() {
    assertThat(persistentQueryDescription.getQueryType(), is(KsqlQueryType.PERSISTENT));
  }

  @Test
  public void shouldHavePushQueryTypeTransientQueryDescription() {
    assertThat(transientQueryDescription.getQueryType(), is(KsqlQueryType.PUSH));
  }

  @Test
  public void shouldHandleRowTimeInValueSchemaForTransientQuery() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
        .build();

    transientQuery = new TransientQueryMetadata(
        SQL_TEXT,
        schema,
        SOURCE_NAMES,
        "execution plan",
        queryQueue,
        QUERY_ID,
        "app id",
        topology,
        kafkaStreamsBuilder,
        STREAMS_PROPS,
        PROP_OVERRIDES,
        closeTimeout,
        10,
        ResultType.STREAM,
        0L,
        0L,
        listener,
        processingLoggerFactory
    );
    transientQuery.initialize();

    // When:
    transientQueryDescription = QueryDescriptionFactory.forQueryMetadata(transientQuery, Collections.emptyMap());

    // Then:
    assertThat(transientQueryDescription.getFields(), contains(
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null), Optional.empty()),
        new FieldInfo("ROWTIME", new SchemaInfo(SqlBaseType.BIGINT, null, null), Optional.empty()),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null), Optional.empty())));
  }

  @Test
  public void shouldHandleRowKeyInValueSchemaForTransientQuery() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
        .build();

    transientQuery = new TransientQueryMetadata(
        SQL_TEXT,
        schema,
        SOURCE_NAMES,
        "execution plan",
        queryQueue,
        QUERY_ID,
        "app id",
        topology,
        kafkaStreamsBuilder,
        STREAMS_PROPS,
        PROP_OVERRIDES,
        closeTimeout,
        10,
        ResultType.STREAM,
        0L,
        0L,
        listener,
        processingLoggerFactory
    );
    transientQuery.initialize();

    // When:
    transientQueryDescription = QueryDescriptionFactory.forQueryMetadata(transientQuery, Collections.emptyMap());

    // Then:
    assertThat(transientQueryDescription.getFields(), contains(
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null), Optional.empty()),
        new FieldInfo("ROWKEY", new SchemaInfo(SqlBaseType.STRING, null, null), Optional.empty()),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null), Optional.empty())));
  }
}
