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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.server.validation.CustomValidators;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class PullQueryExecutorTest {

  public static class Disabled {
    @Rule
    public final TemporaryEngine engine = new TemporaryEngine()
        .withConfigs(ImmutableMap.of(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG, false));

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldThrowExceptionIfConfigDisabled() {

      testForFailure(
          engine,
          expectedException,
          "Pull queries are disabled on this KSQL server"
      );
    }
  }

  public static class Enabled {
    @Rule
    public final TemporaryEngine engine = new TemporaryEngine();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldThrowExceptionOnQueryEndpoint() {
      testForFailure(
          engine,
          expectedException,
          "The following statement types should be issued to the websocket endpoint '/query'"
      );
    }
  }

  private static void testForFailure(
      TemporaryEngine engine, ExpectedException expectedException, String errorMessage
  ) {
    // Given:
    final ConfiguredStatement<Query> query = ConfiguredStatement.of(
        PreparedStatement.of("SELECT * FROM test_table;", mock(Query.class)),
        ImmutableMap.of(),
        engine.getKsqlConfig()
    );

    // Then:
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.BAD_REQUEST)));
    expectedException.expect(exceptionStatementErrorMessage(errorMessage(containsString(
        errorMessage
    ))));
    expectedException.expect(exceptionStatementErrorMessage(statement(containsString(
        "SELECT * FROM test_table"))));

    // When:
    CustomValidators.QUERY_ENDPOINT.validate(
        query,
        ImmutableMap.of(),
        engine.getEngine(),
        engine.getServiceContext()
    );
  }
}
