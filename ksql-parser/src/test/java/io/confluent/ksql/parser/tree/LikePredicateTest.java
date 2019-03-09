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

import static org.mockito.Mockito.mock;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Optional;
import org.junit.Test;

public class LikePredicateTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Expression EXP_0 = mock(Expression.class);
  private static final Expression EXP_1 = mock(Expression.class);
  private static final Expression DIFF = mock(Expression.class);

  @Test
  public void shouldThrowNpeOnConstruction() {
    new NullPointerTester()
        .setDefault(NodeLocation.class, SOME_LOCATION)
        .setDefault(Expression.class, EXP_0)
        .testAllPublicConstructors(LikePredicate.class);
  }

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new LikePredicate(EXP_0, EXP_1),
            new LikePredicate(EXP_0, EXP_1),
            new LikePredicate(Optional.of(SOME_LOCATION), EXP_0, EXP_1),
            new LikePredicate(Optional.of(OTHER_LOCATION), EXP_0, EXP_1)
        )
        .addEqualityGroup(
            new LikePredicate(DIFF, EXP_1)
        )
        .addEqualityGroup(
            new LikePredicate(EXP_0, DIFF)
        )
        .testEquals();
  }
}