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

package io.confluent.ksql.parser.tree;

import static io.confluent.ksql.parser.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.confluent.ksql.parser.tree.ArithmeticUnaryExpression.Sign.PLUS;

import com.google.common.testing.EqualsTester;
import java.util.Optional;
import org.junit.Test;

public class ArithmeticUnaryExpressionTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Expression EXPRESSION_0 = new StringLiteral("bob");
  private static final Expression EXPRESSION_1 = new StringLiteral("jane");

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new ArithmeticUnaryExpression(Optional.of(SOME_LOCATION), PLUS, EXPRESSION_0),
            new ArithmeticUnaryExpression(Optional.of(OTHER_LOCATION), PLUS, EXPRESSION_0),
            ArithmeticUnaryExpression.positive(Optional.of(SOME_LOCATION), EXPRESSION_0)
        )
        .addEqualityGroup(
            new ArithmeticUnaryExpression(Optional.of(SOME_LOCATION), MINUS, EXPRESSION_0),
            ArithmeticUnaryExpression.negative(Optional.of(SOME_LOCATION), EXPRESSION_0)
        )
        .addEqualityGroup(
            new ArithmeticUnaryExpression(Optional.of(SOME_LOCATION), PLUS, EXPRESSION_1)
        )
        .testEquals();
  }
}