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
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.PersistentQueryMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListQueriesExecutorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldListQueriesEmpty() {
    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        engine.configure("SHOW QUERIES;"),
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

    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        engine,
        this.engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueries(), containsInAnyOrder(
        new RunningQuery(
            metadata.getStatementString(),
            ImmutableSet.of(metadata.getSinkName()),
            new EntityQueryId(metadata.getQueryId()))));
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
        engine,
        this.engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueryDescriptions(), containsInAnyOrder(
        QueryDescription.forQueryMetadata(metadata)));
  }

  @SuppressWarnings("SameParameterValue")
  public static PersistentQueryMetadata givenPersistentQuery(final String id) {
    final PersistentQueryMetadata metadata = mock(PersistentQueryMetadata.class);
    when(metadata.getQueryId()).thenReturn(new QueryId(id));
    when(metadata.getSinkName()).thenReturn(id);
    when(metadata.getLogicalSchema()).thenReturn(TemporaryEngine.SCHEMA);

    return metadata;
  }
}
