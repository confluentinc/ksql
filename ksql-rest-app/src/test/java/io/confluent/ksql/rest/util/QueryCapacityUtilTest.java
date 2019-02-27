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

package io.confluent.ksql.rest.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryCapacityUtilTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KsqlConfig ksqlConfig;

  @Test
  public void shouldReportCapacityExceededIfOverLimit() {
    // Given:
    givenActivePersistentQueries(4);
    givenQueryLimit(2);

    // Then:
    assertThat(QueryCapacityUtil.exceedsPersistentQueryCapacity(ksqlEngine, ksqlConfig, 0),
        equalTo(true));
  }

  @Test
  public void shouldReportCapacityExceededIfTooManyQueriesAdded() {
    // Given:
    givenActivePersistentQueries(2);
    givenQueryLimit(4);

    // Then:
    assertThat(QueryCapacityUtil.exceedsPersistentQueryCapacity(ksqlEngine, ksqlConfig, 3),
        equalTo(true));
  }

  @Test
  public void shouldNotReportCapacityExceededIfReached() {
    // Given:
    givenActivePersistentQueries(2);
    givenQueryLimit(4);

    // Then:
    assertThat(QueryCapacityUtil.exceedsPersistentQueryCapacity(ksqlEngine, ksqlConfig, 2),
        equalTo(false));
  }

  @Test
  public void shouldNotReportCapacityExceededIfNotReached() {
    // Given:
    givenActivePersistentQueries(2);
    givenQueryLimit(4);

    // Then:
    assertThat(QueryCapacityUtil.exceedsPersistentQueryCapacity(ksqlEngine, ksqlConfig, 1),
        equalTo(false));
  }

  @Test
  public void shouldThrowWhenAsked() {
    // Given:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Not executing statement(s) 'my statement' as it would cause the number "
            + "of active, persistent queries to exceed the configured limit. "
            + "Use the TERMINATE command to terminate existing queries, "
            + "or increase the 'ksql.query.persistent.active.limit' setting "
            + "via the 'ksql-server.properties' file. "
            + "Current persistent query count: 3. Configured limit: 2.");

    final String statementStr = "my statement";
    givenActivePersistentQueries(3);
    givenQueryLimit(2);

    // When:
    QueryCapacityUtil.throwTooManyActivePersistentQueriesException(
        ksqlEngine, ksqlConfig, statementStr);
  }

  private void givenActivePersistentQueries(final int numQueries) {
    when(ksqlEngine.numberOfPersistentQueries())
        .thenReturn(numQueries);
  }

  private void givenQueryLimit(final int queryLimit) {
    when(ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG))
        .thenReturn(queryLimit);
  }
}
