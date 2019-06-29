/*
 * Copyright 2018 Confluent Inc.
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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.LimitHandler;
import io.confluent.ksql.physical.QuerySchemas;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryDescriptionTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.of(
      SchemaBuilder.struct()
          .field("field1", Schema.OPTIONAL_INT32_SCHEMA)
          .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
          .build());

  private static final List<FieldInfo> EXPECTED_FIELDS = Arrays.asList(
      new FieldInfo("ROWTIME", new SchemaInfo(SqlBaseType.BIGINT, null, null)),
      new FieldInfo("ROWKEY", new SchemaInfo(SqlBaseType.STRING, null, null)),
      new FieldInfo("field1", new SchemaInfo(SqlBaseType.INTEGER, null, null)),
      new FieldInfo("field2", new SchemaInfo(SqlBaseType.STRING, null, null)));

  private static final String STATEMENT = "statement";
  private static final Map<String, Object> STREAMS_PROPS = Collections.singletonMap("k1", "v1");
  private static final Map<String, Object> PROP_OVERRIDES = Collections.singletonMap("k2", "v2");

  @Mock
  private Consumer<QueryMetadata> queryCloseCallback;
  @Mock
  private KafkaStreams queryStreams;
  @Mock
  private Topology topology;
  @Mock
  private TopologyDescription topologyDescription;
  @Mock
  private Consumer<LimitHandler> limitHandler;

  @Before
  public void setUp() {
    when(topology.describe()).thenReturn(topologyDescription);
  }

  @Test
  public void shouldSetFieldsCorrectlyForQueryMetadata() {
    // Given:
    final QueryMetadata queryMetadata = new QueuedQueryMetadata(
        "test statement",
        queryStreams,
        SCHEMA,
        ImmutableSet.of("s1, s2"),
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
    final QueryDescription queryDescription = QueryDescription.forQueryMetadata(queryMetadata);

    // Then:
    assertThat(queryDescription.getId().getId(), equalTo(""));
    assertThat(queryDescription.getExecutionPlan(), equalTo("execution plan"));
    assertThat(queryDescription.getSources(), equalTo(ImmutableSet.of("s1, s2")));
    assertThat(queryDescription.getStatementText(), equalTo("test statement"));
    assertThat(queryDescription.getTopology(), equalTo(topologyDescription.toString()));
    assertThat(queryDescription.getFields(), equalTo(EXPECTED_FIELDS));
    assertThat(queryDescription.getOverriddenProperties(), is(PROP_OVERRIDES));
  }

  @Test
  public void shouldSetFieldsCorrectlyForPersistentQueryMetadata() {
    // Given:
    final KsqlTopic sinkTopic = new KsqlTopic("fake_sink", "fake_sink", new KsqlJsonSerdeFactory(), true);
    final KsqlStream<?> fakeSink = new KsqlStream<>(
        STATEMENT,
        "fake_sink",
        SCHEMA,
        SerdeOption.none(),
        KeyField.of(SCHEMA.valueFields().get(0).name(), SCHEMA.valueFields().get(0)),
        new MetadataTimestampExtractionPolicy(),
        sinkTopic,
        Serdes::String
    );

    final PersistentQueryMetadata queryMetadata = new PersistentQueryMetadata(
        "test statement",
        queryStreams,
        PhysicalSchema.from(SCHEMA, SerdeOption.none()),
        Collections.emptySet(),
        fakeSink.getName(),
        "execution plan",
        new QueryId("query_id"),
        DataSourceType.KSTREAM,
        "app id",
        sinkTopic,
        topology,
        QuerySchemas.of(new LinkedHashMap<>()),
        STREAMS_PROPS,
        PROP_OVERRIDES,
        queryCloseCallback);

    // When:
    final QueryDescription queryDescription = QueryDescription.forQueryMetadata(queryMetadata);

    // Then:
    assertThat(queryDescription.getId().getId(), equalTo("query_id"));
    assertThat(queryDescription.getSinks(), equalTo(Collections.singleton("fake_sink")));
    assertThat(queryDescription.getFields(), equalTo(EXPECTED_FIELDS));
    assertThat(queryDescription.getOverriddenProperties(), is(PROP_OVERRIDES));
  }
}
