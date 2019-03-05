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

public class CustomValidatorsTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  KsqlConfig ksqlConfig;
  ServiceContext serviceContext;
  KsqlEngine engine;

  private KsqlEngine realEngine;

  @Before
  public void setUp() {
    ksqlConfig = new KsqlConfig(new HashMap<>());
    serviceContext = TestServiceContext.create();
    MutableMetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    engine = KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
    realEngine = engine;
  }

  @After
  public void tearDown() {
    realEngine.close();
    serviceContext.close();
  }

  @SuppressWarnings("SameParameterValue")
  void expectBadRequest(final String message, final String statementText) {
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.BAD_REQUEST)));
    expectedException.expect(exceptionStatementErrorMessage(errorMessage(containsString(
        message))));
    expectedException.expect(exceptionStatementErrorMessage(statement(containsString(
        statementText))));
  }

}
