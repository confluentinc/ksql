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

package io.confluent.ksql.parser.tree;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.tree.JoinedSource.Type;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class JoinedSourceTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Relation RELATION_0 = new Table(SourceName.of("bob"));
  private static final Relation RELATION_1 = new Table(SourceName.of("pete"));
  private static final JoinCriteria SOME_CRITERIA = new JoinOn(new StringLiteral("j"));
  private static final JoinCriteria OTHER_CRITERIA = new JoinOn(new StringLiteral("p"));
  private static final Optional<WithinExpression> SOME_WITHIN =
      Optional.of(new WithinExpression(1, TimeUnit.SECONDS));

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new JoinedSource(Optional.empty(), RELATION_0, Type.INNER, SOME_CRITERIA, SOME_WITHIN),
            new JoinedSource(Optional.of(SOME_LOCATION), RELATION_0, Type.INNER, SOME_CRITERIA, SOME_WITHIN),
            new JoinedSource(Optional.of(OTHER_LOCATION), RELATION_0, Type.INNER, SOME_CRITERIA, SOME_WITHIN)
        )
        .addEqualityGroup(
            new JoinedSource(Optional.of(OTHER_LOCATION), RELATION_0, Type.OUTER, SOME_CRITERIA, SOME_WITHIN)
        )
        .addEqualityGroup(
            new JoinedSource(Optional.of(OTHER_LOCATION), RELATION_1, Type.OUTER, SOME_CRITERIA, SOME_WITHIN)
        )
        .addEqualityGroup(
            new JoinedSource(Optional.of(OTHER_LOCATION), RELATION_1, Type.OUTER, OTHER_CRITERIA, SOME_WITHIN)
        )
        .addEqualityGroup(
            new JoinedSource(Optional.of(OTHER_LOCATION), RELATION_1, Type.OUTER, OTHER_CRITERIA, Optional.empty())
        )
        .testEquals();
  }
}