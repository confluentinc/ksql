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

import com.google.common.testing.EqualsTester;
import java.util.Optional;
import org.junit.Test;

public class BetweenPredicateTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Expression EXP_0 = new StringLiteral("bob");
  private static final Expression EXP_1 = new StringLiteral("jane");
  private static final Expression EXP_2 = new StringLiteral("liz");

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new BetweenPredicate(EXP_0, EXP_1, EXP_2),
            new BetweenPredicate(EXP_0, EXP_1, EXP_2),
            new BetweenPredicate(Optional.empty(), EXP_0, EXP_1, EXP_2),
            new BetweenPredicate(Optional.of(SOME_LOCATION), EXP_0, EXP_1, EXP_2)
        )
        .addEqualityGroup(
            new BetweenPredicate(EXP_1, EXP_1, EXP_2)
        )
        .addEqualityGroup(
            new BetweenPredicate(EXP_0, EXP_0, EXP_2)
        )
        .addEqualityGroup(
            new BetweenPredicate(EXP_0, EXP_1, EXP_0)
        )
        .testEquals();
  }
}