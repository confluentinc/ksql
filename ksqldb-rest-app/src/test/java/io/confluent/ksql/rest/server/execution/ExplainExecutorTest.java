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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionFactory;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExplainExecutorTest {

  private static final KsqlQueryStatus STATE = KsqlQueryStatus.RUNNING;
  private static final KsqlHostInfo LOCAL_HOST = new KsqlHostInfo("host", 8080);
  @Rule
  public final TemporaryEngine engine = new TemporaryEngine();
  @Mock
  private SessionProperties sessionProperties;


  @Before
  public void setup() {
    when(sessionProperties.getKsqlHostInfo()).thenReturn(LOCAL_HOST);
  }

  @Test
  public void shouldExplainQueryId() {
    // Given:
    final ConfiguredStatement<?> explain = engine.configure("EXPLAIN id;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id");

    final KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getPersistentQuery(metadata.getQueryId())).thenReturn(Optional.of(metadata));

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        sessionProperties,
        engine,
        this.engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(
        query.getQueryDescription(),
        equalTo(QueryDescriptionFactory.forQueryMetadata(
            metadata, Collections.singletonMap(new KsqlHostInfoEntity(LOCAL_HOST), STATE))));
  }

  @Test
  public void shouldExplainPersistentStatement() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "Y");
    final String statementText = "CREATE STREAM X AS SELECT * FROM Y;";
    final ConfiguredStatement<?> explain = engine.configure("EXPLAIN " + statementText);

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        sessionProperties,
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
    assertThat("No side effects should happen", engine.getEngine().getPersistentQueries(), is(empty()));
    assertThat(query.getQueryDescription().getKsqlHostQueryStatus(), equalTo(Collections.emptyMap()));
  }

  @Test
  public void shouldExplainStatement() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "Y");
    final String statementText = "SELECT * FROM Y EMIT CHANGES;";
    final ConfiguredStatement<?> explain = engine.configure("EXPLAIN " + statementText);

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        sessionProperties,
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
    assertThat(query.getQueryDescription().getKsqlHostQueryStatus(), equalTo(Collections.emptyMap()));
  }

  @Test
  public void shouldExplainStatementWithStructFieldDereference() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "Y");
    final String statementText = "SELECT address->street FROM Y EMIT CHANGES;";
    final ConfiguredStatement<?> explain = engine.configure("EXPLAIN " + statementText);

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        sessionProperties,
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
  }

  @Test
  public void shouldFailOnNonQueryExplain() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CustomExecutors.EXPLAIN.execute(
            engine.configure("Explain SHOW TOPICS;"),
            sessionProperties,
            engine.getEngine(),
            engine.getServiceContext()
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The provided statement does not run a ksql query"));
  }

  @SuppressWarnings("SameParameterValue")
  public static PersistentQueryMetadata givenPersistentQuery(final String id) {
    final PersistentQueryMetadata metadata = mock(PersistentQueryMetadata.class);
    when(metadata.getQueryApplicationId()).thenReturn("consumer-group-id");
    when(metadata.getQueryId()).thenReturn(new QueryId(id));
    when(metadata.getSinkName()).thenReturn(Optional.of(SourceName.of(id)));
    when(metadata.getLogicalSchema()).thenReturn(TemporaryEngine.SCHEMA);
    when(metadata.getState()).thenReturn(KafkaStreams.State.valueOf(STATE.toString()));
    when(metadata.getTopologyDescription()).thenReturn("topology");
    when(metadata.getExecutionPlan()).thenReturn("plan");
    when(metadata.getStatementString()).thenReturn("sql");
    when(metadata.getQueryType()).thenReturn(KsqlConstants.KsqlQueryType.PERSISTENT);

    final KsqlTopic sinkTopic = mock(KsqlTopic.class);
    when(sinkTopic.getKeyFormat()).thenReturn(
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()));
    when(metadata.getResultTopic()).thenReturn(Optional.of(sinkTopic));

    return metadata;
  }
}
