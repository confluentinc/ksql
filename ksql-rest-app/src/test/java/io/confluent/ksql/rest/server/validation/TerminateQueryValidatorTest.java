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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.util.KsqlStatementException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminateQueryValidatorTest extends CustomValidatorsTest {

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

}
