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

import static com.google.common.collect.ImmutableMap.of;
import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static io.confluent.ksql.rest.server.execution.CustomExecutors.EXPLAIN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionFactory;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExplainExecutorTest {

  @Rule
  public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldExplainQueryId() {
    // Given:
    final ConfiguredStatement<?> explain = engine.configure("EXPLAIN id;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id");
    when(metadata.getState()).thenReturn("Running");

    KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getPersistentQuery(metadata.getQueryId())).thenReturn(Optional.of(metadata));

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        ImmutableMap.of(),
        engine,
        this.engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription(), equalTo(QueryDescriptionFactory.forQueryMetadata(metadata)));
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
        ImmutableMap.of(),
        engine.getEngine(),
        engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
    assertThat("No side effects should happen", engine.getEngine().getPersistentQueries(), is(empty()));
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
        ImmutableMap.of(),
        engine.getEngine(),
        engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
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
        ImmutableMap.of(),
        engine.getEngine(),
        engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
  }

  @Test
  public void shouldFailOnNonQueryExplain() {
    // Expect:
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> EXPLAIN.execute(
            engine.configure("Explain SHOW TOPICS;"),
            of(),
            engine.getEngine(),
            engine.getServiceContext()
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("The provided statement does not run a ksql query"));
  }

  @SuppressWarnings("SameParameterValue")
  public static PersistentQueryMetadata givenPersistentQuery(final String id) {
    final PersistentQueryMetadata metadata = mock(PersistentQueryMetadata.class);
    when(metadata.getQueryId()).thenReturn(new QueryId(id));
    when(metadata.getSinkName()).thenReturn(SourceName.of(id));
    when(metadata.getLogicalSchema()).thenReturn(TemporaryEngine.SCHEMA);

    return metadata;
  }
}
