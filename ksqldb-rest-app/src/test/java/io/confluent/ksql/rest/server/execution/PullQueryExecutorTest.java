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

import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.entity.KsqlStatementErrorMessageMatchers.statement;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatementErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingFilters;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.server.validation.CustomValidators;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Optional;
import org.apache.kafka.common.utils.Time;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public class PullQueryExecutorTest {
  private static final RoutingFilterFactory ROUTING_FILTER_FACTORY =
      (routingOptions, hosts, active, applicationQueryId, storeName, partition) ->
          new RoutingFilters(ImmutableList.of());

  public static class Disabled {
    @Rule
    public final TemporaryEngine engine = new TemporaryEngine()
        .withConfigs(ImmutableMap.of(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG, false));

    @Mock
    private Time time;

    @Test
    public void shouldThrowExceptionIfConfigDisabled() {
      // Given:
      final Query theQuery = mock(Query.class);
      when(theQuery.isPullQuery()).thenReturn(true);
      final ConfiguredStatement<Query> query = ConfiguredStatement.of(
          PreparedStatement.of("SELECT * FROM test_table;", theQuery),
          ImmutableMap.of(),
          engine.getKsqlConfig()
      );
      PullQueryExecutor pullQueryExecutor = new PullQueryExecutor(
          engine.getEngine(), ROUTING_FILTER_FACTORY, engine.getKsqlConfig(),
          engine.getEngine().getServiceId(), time);

      // When:
      final Exception e = assertThrows(
          KsqlStatementException.class,
          () -> pullQueryExecutor.execute(query, engine.getServiceContext(), Optional.empty(), 0L)
      );

      // Then:
      assertThat(e.getMessage(), containsString(
          "Pull queries are disabled"));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class Enabled {

    @Rule
    public final TemporaryEngine engine = new TemporaryEngine();

    @Test
    public void shouldRedirectQueriesToQueryEndPoint() {
      // Given:
      final ConfiguredStatement<Query> query = ConfiguredStatement.of(
          PreparedStatement.of("SELECT * FROM test_table;", mock(Query.class)),
          ImmutableMap.of(),
          engine.getKsqlConfig()
      );

      // When:
      final KsqlRestException e = assertThrows(
          KsqlRestException.class,
          () -> CustomValidators.QUERY_ENDPOINT.validate(
              query,
              mock(SessionProperties.class),
              engine.getEngine(),
              engine.getServiceContext()
          )
      );

      // Then:
      assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
      assertThat(e, exceptionStatementErrorMessage(errorMessage(containsString(
          "The following statement types should be issued to the websocket endpoint '/query'"
      ))));
      assertThat(e, exceptionStatementErrorMessage(statement(containsString(
          "SELECT * FROM test_table;"))));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class RateLimit {

    @Rule
    public final TemporaryEngine engine = new TemporaryEngine()
        .withConfigs(ImmutableMap.of(KsqlConfig.KSQL_QUERY_PULL_MAX_QPS_CONFIG, 2));

    @Mock
    private Time time;

    @Test
    public void shouldRateLimit() {
      PullQueryExecutor pullQueryExecutor = new PullQueryExecutor(
          engine.getEngine(), ROUTING_FILTER_FACTORY, engine.getKsqlConfig(),
          engine.getEngine().getServiceId(), time);

      // When:
      pullQueryExecutor.checkRateLimit();
      assertThrows(KsqlException.class, pullQueryExecutor::checkRateLimit);
    }
  }
}
