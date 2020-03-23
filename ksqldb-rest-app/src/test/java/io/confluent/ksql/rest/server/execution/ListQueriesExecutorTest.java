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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KafkaStreamsStateCount;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescriptionFactory;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.PersistentQueryMetadata;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListQueriesExecutorTest {

  private static final KsqlHostInfoEntity LOCAL_HOST = new KsqlHostInfoEntity("some host", 555);
  private static final KafkaStreams.State QUERY_STATE = KafkaStreams.State.RUNNING;
  private static final Map<KsqlHostInfoEntity, String> KSQL_HOST_INFO_MAP = Collections.singletonMap(LOCAL_HOST, QUERY_STATE.toString());
  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Mock
  SessionProperties sessionProperties;

  KafkaStreamsStateCount kafkaStreamsStateCount;

  @Before
  public void setup() {
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    when(sessionProperties.getKsqlHostInfo()).thenReturn(LOCAL_HOST.toKsqlHost());
    kafkaStreamsStateCount = new KafkaStreamsStateCount();
  }

  @Test
  public void shouldListQueriesEmpty() {
    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        engine.configure("SHOW QUERIES;"),
            mock(SessionProperties.class),
        engine.getEngine(),
        engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueries(), is(empty()));
  }

  @Test
  public void shouldListQueriesBasic() {
    // Given
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id");

    final KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getPersistentQueries()).thenReturn(ImmutableList.of(metadata));
    kafkaStreamsStateCount.updateStateCount(QUERY_STATE, 1);

    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        engine,
        this.engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueries(), containsInAnyOrder(
        new RunningQuery(
            metadata.getStatementString(),
            ImmutableSet.of(metadata.getSinkName().text()),
            ImmutableSet.of(metadata.getResultTopic().getKafkaTopicName()),
            metadata.getQueryId(),
            kafkaStreamsStateCount
        )));
  }

  @Test
  public void shouldListQueriesExtended() {
    // Given
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES EXTENDED;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id");

    final KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getPersistentQueries()).thenReturn(ImmutableList.of(metadata));

    // When
    final QueryDescriptionList queries = (QueryDescriptionList) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        engine,
        this.engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueryDescriptions(), containsInAnyOrder(
        QueryDescriptionFactory.forQueryMetadata(metadata, KSQL_HOST_INFO_MAP)));
  }

  @SuppressWarnings("SameParameterValue")
  public static PersistentQueryMetadata givenPersistentQuery(final String id) {
    final PersistentQueryMetadata metadata = mock(PersistentQueryMetadata.class);
    when(metadata.getStatementString()).thenReturn("sql");
    when(metadata.getQueryId()).thenReturn(new QueryId(id));
    when(metadata.getSinkName()).thenReturn(SourceName.of(id));
    when(metadata.getLogicalSchema()).thenReturn(TemporaryEngine.SCHEMA);
    when(metadata.getState()).thenReturn(QUERY_STATE.toString());
    when(metadata.getTopologyDescription()).thenReturn("topology");
    when(metadata.getExecutionPlan()).thenReturn("plan");

    final KsqlTopic sinkTopic = mock(KsqlTopic.class);
    when(sinkTopic.getKeyFormat()).thenReturn(KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())));
    when(sinkTopic.getKafkaTopicName()).thenReturn(id);
    when(metadata.getResultTopic()).thenReturn(sinkTopic);

    return metadata;
  }
}
