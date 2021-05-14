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

package io.confluent.ksql.execution.expression.tree;

import static org.mockito.Mockito.mock;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Optional;
import org.junit.Test;

public class InPredicateTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Expression SOME_VALUE = new StringLiteral("jane");
  private static final InListExpression SOME_LIST = mock(InListExpression.class);

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new InPredicate(SOME_VALUE, SOME_LIST),
            new InPredicate(SOME_VALUE, SOME_LIST),
            new InPredicate(Optional.of(SOME_LOCATION), SOME_VALUE, SOME_LIST),
            new InPredicate(Optional.of(OTHER_LOCATION), SOME_VALUE, SOME_LIST)
        )
        .addEqualityGroup(
            new InPredicate(new StringLiteral("different"), SOME_LIST)
        )
        .addEqualityGroup(
            new InPredicate(SOME_VALUE, mock(InListExpression.class))
        )
        .testEquals();
  }
}