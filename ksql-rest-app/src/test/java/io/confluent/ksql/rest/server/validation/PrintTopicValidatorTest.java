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

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PrintTopicValidatorTest extends CustomValidatorsTest {

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

}
