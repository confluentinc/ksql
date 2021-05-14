/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.parser.json;

import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_NAME_NEEDS_QUOTES;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_NAME_NEEDS_QUOTES_TXT;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_NEEDS_QUOTES;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_NEEDS_QUOTES_TXT;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_TXT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.Test;

public class SelectExpressionSerializerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new KsqlParserSerializationModule());
  }

  @Test
  public void shouldSerializeSelectExpression() throws IOException {
    assertThat(MAPPER.writeValueAsString(SELECT_EXPRESSION), equalTo(SELECT_EXPRESSION_TXT));
  }

  @Test
  public void shouldSerializeSelectExpressionNeedingQuotes() throws IOException{
    assertThat(
        MAPPER.writeValueAsString(SELECT_EXPRESSION_NEEDS_QUOTES),
        equalTo(SELECT_EXPRESSION_NEEDS_QUOTES_TXT)
    );
  }

  @Test
  public void shouldSerializeSelectExpressionNeedingQuotesInName() throws IOException{
    assertThat(
        MAPPER.writeValueAsString(SELECT_EXPRESSION_NAME_NEEDS_QUOTES),
        equalTo(SELECT_EXPRESSION_NAME_NEEDS_QUOTES_TXT)
    );
  }
}