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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.physical.LimitHandler;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QuerySchemas;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryDescriptionFactoryTest {

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .valueColumn("field1", SqlTypes.INTEGER)
      .valueColumn("field2", SqlTypes.STRING)
      .build();

  private static final Map<String, Object> STREAMS_PROPS = Collections.singletonMap("k1", "v1");
  private static final Map<String, Object> PROP_OVERRIDES = Collections.singletonMap("k2", "v2");
  private static final QueryId QUERY_ID = new QueryId("query_id");
  private static final ImmutableSet<String> SOURCE_NAMES = ImmutableSet.of("s1, s2");
  private static final String SQL_TEXT = "test statement";
  private static final String TOPOLOGY_TEXT = "Topology Text";

  @Mock
  private Consumer<QueryMetadata> queryCloseCallback;
  @Mock
  private KafkaStreams queryStreams;
  @Mock
  private Topology topology;
  @Mock(name = TOPOLOGY_TEXT)
  private TopologyDescription topologyDescription;
  @Mock
  private Consumer<LimitHandler> limitHandler;
  @Mock
  private KsqlTopic sinkTopic;
  private QueryMetadata transientQuery;
  private QueryDescription transientQueryDescription;
  private QueryDescription persistentQueryDescription;

  @Before
  public void setUp() {
    when(topology.describe()).thenReturn(topologyDescription);

    transientQuery = new TransientQueryMetadata(
        SQL_TEXT,
        queryStreams,
        SOME_SCHEMA,
        SOURCE_NAMES,
        limitHandler,
        "execution plan",
        new LinkedBlockingQueue<>(),
        DataSourceType.KSTREAM,
        "app id",
        topology,
        STREAMS_PROPS,
        PROP_OVERRIDES,
        queryCloseCallback);

    transientQueryDescription = QueryDescriptionFactory.forQueryMetadata(transientQuery);

    final PersistentQueryMetadata persistentQuery = new PersistentQueryMetadata(
        SQL_TEXT,
        queryStreams,
        PhysicalSchema.from(SOME_SCHEMA, SerdeOption.none()),
        SOURCE_NAMES,
        "sink Name",
        "execution plan",
        QUERY_ID,
        DataSourceType.KSTREAM,
        "app id",
        sinkTopic,
        topology,
        QuerySchemas.of(new LinkedHashMap<>()),
        STREAMS_PROPS,
        PROP_OVERRIDES,
        queryCloseCallback);

    persistentQueryDescription = QueryDescriptionFactory.forQueryMetadata(persistentQuery);
  }

  @Test
  public void shouldHaveEmptyQueryIdFromTransientQuery() {
    Assert.assertThat(transientQueryDescription.getId().getId(), is(isEmptyString()));
  }

  @Test
  public void shouldHaveQueryIdForPersistentQuery() {
    Assert.assertThat(persistentQueryDescription.getId().getId(), is(QUERY_ID.getId()));
  }

  @Test
  public void shouldExposeExecutionPlan() {
    Assert.assertThat(transientQueryDescription.getExecutionPlan(), is("execution plan"));
    Assert.assertThat(persistentQueryDescription.getExecutionPlan(), is("execution plan"));
  }

  @Test
  public void shouldExposeSources() {
    Assert.assertThat(transientQueryDescription.getSources(), is(SOURCE_NAMES));
    Assert.assertThat(persistentQueryDescription.getSources(), is(SOURCE_NAMES));
  }

  @Test
  public void shouldExposeStatementText() {
    Assert.assertThat(transientQueryDescription.getStatementText(), is(SQL_TEXT));
    Assert.assertThat(persistentQueryDescription.getStatementText(), is(SQL_TEXT));
  }

  @Test
  public void shouldExposeTopology() {
    Assert.assertThat(transientQueryDescription.getTopology(), is(TOPOLOGY_TEXT));
    Assert.assertThat(persistentQueryDescription.getTopology(), is(TOPOLOGY_TEXT));
  }

  @Test
  public void shouldExposeOverridenProperties() {
    Assert.assertThat(transientQueryDescription.getOverriddenProperties(), is(PROP_OVERRIDES));
    Assert.assertThat(persistentQueryDescription.getOverriddenProperties(), is(PROP_OVERRIDES));
  }

  @Test
  public void shouldExposeValueFieldsForTransientQueries() {
    Assert.assertThat(transientQueryDescription.getFields(), contains(
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null)),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null))));
  }

  @Test
  public void shouldExposeAllFieldsForPersistentQueries() {
    Assert.assertThat(persistentQueryDescription.getFields(), contains(
        new FieldInfo("ROWTIME", new SchemaInfo(SqlBaseType.BIGINT, null, null)),
        new FieldInfo("ROWKEY", new SchemaInfo(SqlBaseType.STRING, null, null)),
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null)),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null))));
  }

  @Test
  public void shouldHandleRowTimeInValueSchemaForTransientQuery() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("field1", SqlTypes.INTEGER)
        .valueColumn("ROWTIME", SqlTypes.BIGINT)
        .valueColumn("field2", SqlTypes.STRING)
        .build();

    transientQuery = new TransientQueryMetadata(
        SQL_TEXT,
        queryStreams,
        schema,
        SOURCE_NAMES,
        limitHandler,
        "execution plan",
        new LinkedBlockingQueue<>(),
        DataSourceType.KSTREAM,
        "app id",
        topology,
        STREAMS_PROPS,
        PROP_OVERRIDES,
        queryCloseCallback);

    // When:
    transientQueryDescription = QueryDescriptionFactory.forQueryMetadata(transientQuery);

    // Then:
    Assert.assertThat(transientQueryDescription.getFields(), contains(
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null)),
        new FieldInfo("ROWTIME", new SchemaInfo(SqlBaseType.BIGINT, null, null)),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null))));
  }

  @Test
  public void shouldHandleRowKeyInValueSchemaForTransientQuery() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn("field1", SqlTypes.INTEGER)
        .valueColumn("ROWKEY", SqlTypes.STRING)
        .valueColumn("field2", SqlTypes.STRING)
        .build();

    transientQuery = new TransientQueryMetadata(
        SQL_TEXT,
        queryStreams,
        schema,
        SOURCE_NAMES,
        limitHandler,
        "execution plan",
        new LinkedBlockingQueue<>(),
        DataSourceType.KSTREAM,
        "app id",
        topology,
        STREAMS_PROPS,
        PROP_OVERRIDES,
        queryCloseCallback);

    // When:
    transientQueryDescription = QueryDescriptionFactory.forQueryMetadata(transientQuery);

    // Then:
    Assert.assertThat(transientQueryDescription.getFields(), contains(
        new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null)),
        new FieldInfo("ROWKEY", new SchemaInfo(SqlBaseType.STRING, null, null)),
        new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null))));
  }
}
