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
import static org.hamcrest.Matchers.isEmptyString;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QuerySchemas;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryDescriptionFactoryTest {

  private static final LogicalSchema TRANSIENT_SCHEMA = LogicalSchema.builder()
      .noImplicitColumns()
      .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema PERSISTENT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
      .build();

  private static final Map<String, Object> STREAMS_PROPS = Collections.singletonMap("k1", "v1");
  private static final Map<String, Object> PROP_OVERRIDES = Collections.singletonMap("k2", "v2");
  private static final QueryId QUERY_ID = new QueryId("query_id");
  private static final ImmutableSet<SourceName> SOURCE_NAMES = ImmutableSet.of(SourceName.of("s1"), SourceName.of("s2"));
  private static final String SQL_TEXT = "test statement";
  private static final String TOPOLOGY_TEXT = "Topology Text";
  private static final Long closeTimeout = KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT;

  @Mock
  private Consumer<QueryMetadata> queryCloseCallback;
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
  private QueryMetadata transientQuery;
  private QueryDescription transientQueryDescription;
  private QueryDescription persistentQueryDescription;

  @Before
  public void setUp() {
    when(topology.describe()).thenReturn(topologyDescription);
    when(queryStreams.state()).thenReturn(State.RUNNING);

    when(sinkTopic.getKeyFormat()).thenReturn(KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())));

    transientQuery = new TransientQueryMetadata(
        SQL_TEXT,
        queryStreams,
        TRANSIENT_SCHEMA,
        SOURCE_NAMES,
        "execution plan",
        queryQueue,
        "app id",
        topology,
        STREAMS_PROPS,
        PROP_OVERRIDES,
        queryCloseCallback,
        closeTimeout);

    transientQueryDescription = QueryDescriptionFactory.forQueryMetadata(transientQuery);

    final PersistentQueryMetadata persistentQuery = new PersistentQueryMetadata(
        SQL_TEXT,
        queryStreams,
        PhysicalSchema.from(PERSISTENT_SCHEMA, SerdeOption.none()),
        SOURCE_NAMES,
        SourceName.of("sink Name"),
        "execution plan",
        QUERY_ID,
        DataSourceType.KSTREAM,
        Optional.empty(),
        "app id",
        sinkTopic,
        topology,
        QuerySchemas.of(new LinkedHashMap<>()),
        STREAMS_PROPS,
        PROP_OVERRIDES,
        queryCloseCallback,
        closeTimeout);

    persistentQueryDescription = QueryDescriptionFactory.forQueryMetadata(persistentQuery);
  }

  @Test
  public void shouldHaveEmptyQueryIdFromTransientQuery() {
    assertThat(transientQueryDescription.getId().getId(), is(isEmptyString()));
  }

  @Test
  public void shouldHaveQueryIdForPersistentQuery() {
    assertThat(persistentQueryDescription.getId().getId(), is(QUERY_ID.getId()));
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
  public void shouldExposeOverridenProperties() {
    assertThat(transientQueryDescription.getOverriddenProperties(), is(PROP_OVERRIDES));
    assertThat(persistentQueryDescription.getOverriddenProperties(), is(PROP_OVERRIDES));
  }

  @Test
  public void shouldExposeValueFieldsForTransientQueries() {
    assertThat(transientQueryDescription.getFields(), contains(
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null)),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null))));
  }

  @Test
  public void shouldExposeAllFieldsForPersistentQueries() {
    assertThat(persistentQueryDescription.getFields(), contains(
        new FieldInfo("ROWTIME", new SchemaInfo(SqlBaseType.BIGINT, null, null)),
        new FieldInfo("ROWKEY", new SchemaInfo(SqlBaseType.STRING, null, null)),
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null)),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null))));
  }

  @Test
  public void shouldReportPersistentQueriesStatus() {
    assertThat(persistentQueryDescription.getState(), is(Optional.of("RUNNING")));
  }

  @Test
  public void shouldNotReportTransientQueriesStatus() {
    assertThat(transientQueryDescription.getState(), is(Optional.empty()));
  }

  @Test
  public void shouldHandleRowTimeInValueSchemaForTransientQuery() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .noImplicitColumns()
        .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
        .build();

    transientQuery = new TransientQueryMetadata(
        SQL_TEXT,
        queryStreams,
        schema,
        SOURCE_NAMES,
        "execution plan",
        queryQueue,
        "app id",
        topology,
        STREAMS_PROPS,
        PROP_OVERRIDES,
        queryCloseCallback,
        closeTimeout);

    // When:
    transientQueryDescription = QueryDescriptionFactory.forQueryMetadata(transientQuery);

    // Then:
    assertThat(transientQueryDescription.getFields(), contains(
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null)),
        new FieldInfo("ROWTIME", new SchemaInfo(SqlBaseType.BIGINT, null, null)),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null))));
  }

  @Test
  public void shouldHandleRowKeyInValueSchemaForTransientQuery() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .noImplicitColumns()
        .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
        .build();

    transientQuery = new TransientQueryMetadata(
        SQL_TEXT,
        queryStreams,
        schema,
        SOURCE_NAMES,
        "execution plan",
        queryQueue,
        "app id",
        topology,
        STREAMS_PROPS,
        PROP_OVERRIDES,
        queryCloseCallback,
        closeTimeout);

    // When:
    transientQueryDescription = QueryDescriptionFactory.forQueryMetadata(transientQuery);

    // Then:
    assertThat(transientQueryDescription.getFields(), contains(
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null)),
        new FieldInfo("ROWKEY", new SchemaInfo(SqlBaseType.STRING, null, null)),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null))));
  }
}
