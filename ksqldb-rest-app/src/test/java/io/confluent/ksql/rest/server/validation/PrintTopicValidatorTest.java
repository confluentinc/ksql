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

package io.confluent.ksql.rest.server.validation;

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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PrintTopicValidatorTest {

  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());

  @Mock
  private KsqlExecutionContext ksqlEngine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private DistributingExecutor distributingExecutor;
  @Mock
  private KsqlSecurityContext ksqlSecurityContext;


  @Test
  public void shouldThrowExceptionOnPrintTopic() {
    // Given:
    final ConfiguredStatement<PrintTopic> query = ConfiguredStatement
        .of(PreparedStatement.of("PRINT 'topic';", mock(PrintTopic.class)),
            SessionConfig.of(CONFIG, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> CustomValidators.PRINT_TOPIC.validate(
            query,
            mock(SessionProperties.class),
            ksqlEngine,
            serviceContext
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionStatementErrorMessage(errorMessage(containsString(
        "The following statement types should be issued to the websocket endpoint '/query'"
    ))));
    assertThat(e, exceptionStatementErrorMessage(statement(containsString(
        "PRINT 'topic';"))));
  }
}
