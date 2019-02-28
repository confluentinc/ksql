/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.validation;

import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.entity.KsqlStatementErrorMessageMatchers.statement;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatementErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.HashMap;
import java.util.Optional;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CustomValidatorsTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private KsqlConfig ksqlConfig;
  private KsqlEngine realEngine;
  private ServiceContext serviceContext;
  private KsqlEngine engine;
  private MutableMetaStore metaStore;

  @Before
  public void setUp() {
    ksqlConfig = new KsqlConfig(new HashMap<>());
    serviceContext = TestServiceContext.create();
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    engine = KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
    realEngine = engine;
  }

  @After
  public void tearDown() {
    realEngine.close();
    serviceContext.close();
  }

  @Test
  public void shouldThrowExceptionOnQueryEndpoint() {
    // Expect:
    expectBadRequest(
        "SELECT and PRINT queries must use the /query endpoint",
        "SELECT * FROM test_table;");

    // When:
    CustomValidators.QUERY_ENDPOINT.validate(
        PreparedStatement.of("SELECT * FROM test_table;", mock(Query.class)),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    );
  }

  @Test
  public void shouldThrowExceptionOnPrintTopic() {
    // Expect:
    expectBadRequest(
        "SELECT and PRINT queries must use the /query endpoint",
        "PRINT 'topic';");

    // When:
    CustomValidators.PRINT_TOPIC.validate(
        PreparedStatement.of("PRINT 'topic';", mock(PrintTopic.class)),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    );
  }

  @Test
  public void shouldFailOnUnknownSetProperty() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Unknown property: consumer.invalid");

    // When:
    CustomValidators.SET_PROPERTY.validate(
        PreparedStatement.of(
            "SET 'consumer.invalid'='value';",
            new SetProperty(Optional.empty(), "consumer.invalid", "value")),
        engine,
        serviceContext,
        ksqlConfig,
        new HashMap<>()
    );
  }

  @Test
  public void shouldAllowSetKnownProperty() {
    // No exception when:
    CustomValidators.SET_PROPERTY.validate(
        PreparedStatement.of(
            "SET '" + KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY + "' = '1';",
            new SetProperty(Optional.empty(), KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "1")),
        engine,
        serviceContext,
        ksqlConfig,
        new HashMap<>()
    );
  }

  @Test
  public void shouldFailOnInvalidSetPropertyValue() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Invalid value invalid");

    // When:
    CustomValidators.SET_PROPERTY.validate(
        PreparedStatement.of(
            "SET '" + KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY + "' = 'invalid';",
            new SetProperty(Optional.empty(), KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "invalid")),
        engine,
        serviceContext,
        ksqlConfig,
        new HashMap<>()
    );
  }

  @Test
  public void shouldFailOnUnknownUnsetProperty() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Unknown property: consumer.invalid");

    // When:
    CustomValidators.UNSET_PROPERTY.validate(
        PreparedStatement.of(
            "UNSET 'consumer.invalid';",
            new UnsetProperty(Optional.empty(), "consumer.invalid")),
        engine,
        serviceContext,
        ksqlConfig,
        new HashMap<>()
    );
  }

  @Test
  public void shouldAllowUnsetKnownProperty() {
    // No exception when:
    CustomValidators.UNSET_PROPERTY.validate(
        PreparedStatement.of(
            "UNSET '" + KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY + "';",
            new UnsetProperty(Optional.empty(), KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)),
        engine,
        serviceContext,
        ksqlConfig,
        new HashMap<>()
    );
  }

  @Test
  public void shouldFailOnTerminateUnknownQueryId() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Unknown queryId");

    // When:
    CustomValidators.TERMINATE_QUERY.validate(
        PreparedStatement.of("", new TerminateQuery("id")),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    );
  }

  @SuppressWarnings("SameParameterValue")
  private void expectBadRequest(final String message, final String statementText) {
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.BAD_REQUEST)));
    expectedException.expect(exceptionStatementErrorMessage(errorMessage(containsString(
        message))));
    expectedException.expect(exceptionStatementErrorMessage(statement(containsString(
        statementText))));
  }

}
