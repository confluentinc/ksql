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

package io.confluent.ksql.parser.json;

import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import java.util.concurrent.TimeUnit;

public class WindowExpressionTestCase {
  static final KsqlWindowExpression WINDOW_EXPRESSION =
      new TumblingWindowExpression(
          new WindowTimeClause(123, TimeUnit.DAYS)
      );
  static final String WINDOW_EXPRESSION_TXT =
      "{\"size\":{\"value\":123,\"timeUnit\":\"DAYS\"},"
          + "\"retention\":null,"
          + "\"gracePeriod\":null,"
          + "\"emitStrategy\":null,"
          + "\"windowType\":\"TUMBLING\"}";
}
