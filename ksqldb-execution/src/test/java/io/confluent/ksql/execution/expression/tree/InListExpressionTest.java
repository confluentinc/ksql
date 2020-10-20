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

package io.confluent.ksql.execution.expression.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class InListExpressionTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final List<Expression> SOME_EXPRESSIONS = ImmutableList.of(
      new StringLiteral("jane"));
  private static final List<Expression> OTHER_EXPRESSIONS = ImmutableList.of(
      new StringLiteral("peter"));

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new InListExpression(SOME_EXPRESSIONS),
            new InListExpression(SOME_EXPRESSIONS),
            new InListExpression(Optional.of(SOME_LOCATION), SOME_EXPRESSIONS),
            new InListExpression(Optional.of(OTHER_LOCATION), SOME_EXPRESSIONS)
        )
        .addEqualityGroup(
            new InListExpression(OTHER_EXPRESSIONS)
        )
        .testEquals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnNoExpressions() {
    new InListExpression(Collections.emptyList());
  }
}