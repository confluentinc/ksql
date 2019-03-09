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

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Test;


public class QueryTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Relation SOME_RELATION = new Table(QualifiedName.of("bob"));
  private static final Relation OTHER_RELATION = new Table(QualifiedName.of("pete"));
  private static final Select SOME_SELECT = new Select(ImmutableList.of(
      new AllColumns(Optional.empty())));
  private static final Select OTHER_SELECT = new Select(ImmutableList.of(new SingleColumn(
      new QualifiedNameReference(QualifiedName.of("Bob")), Optional.empty(), Optional.empty())));
  private static final Relation SOME_FROM = new Table(QualifiedName.of("from"));
  ;
  private static final Optional<WindowExpression> SOME_WINDOW = Optional.of(
      mock(WindowExpression.class)
  );
  private static final Optional<Expression> SOME_WHERE = Optional.of(
      mock(Expression.class)
  );
  private static final Optional<GroupBy> SOME_GROUP_BY = Optional.of(
      mock(GroupBy.class)
  );
  private static final Optional<Expression> SOME_HAVING = Optional.of(
      mock(Expression.class)
  );
  private static final OptionalInt SOME_LIMIT = OptionalInt.of(1);

  @Test
  public void shouldThrowNpeOnConstruction() {
    new NullPointerTester()
        .setDefault(NodeLocation.class, SOME_LOCATION)
        .setDefault(Relation.class, SOME_RELATION)
        .setDefault(Select.class, SOME_SELECT)
        .testAllPublicConstructors(Query.class);
  }

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new Query(SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, SOME_LIMIT),
            new Query(SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, SOME_LIMIT),
            new Query(Optional.of(SOME_LOCATION), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, SOME_LIMIT),
            new Query(Optional.of(OTHER_LOCATION), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(OTHER_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(SOME_SELECT, OTHER_RELATION, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(SOME_SELECT, SOME_FROM, Optional.empty(),
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(SOME_SELECT, SOME_FROM, SOME_WINDOW,
                Optional.empty(), SOME_GROUP_BY, SOME_HAVING, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, Optional.empty(), SOME_HAVING, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, Optional.empty(), SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, OptionalInt.empty())
        )
        .testEquals();
  }
}