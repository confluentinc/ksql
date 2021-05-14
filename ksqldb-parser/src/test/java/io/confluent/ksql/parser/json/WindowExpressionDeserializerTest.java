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

import static io.confluent.ksql.parser.json.WindowExpressionTestCase.WINDOW_EXPRESSION;
import static io.confluent.ksql.parser.json.WindowExpressionTestCase.WINDOW_EXPRESSION_TXT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import java.io.IOException;
import org.junit.Test;

public class WindowExpressionDeserializerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new KsqlParserSerializationModule());
  }

  @Test
  public void shouldDeserializeWindowExpression() throws IOException {
    assertThat(
        MAPPER.readValue(WINDOW_EXPRESSION_TXT, KsqlWindowExpression.class),
        equalTo(WINDOW_EXPRESSION)
    );
  }
}