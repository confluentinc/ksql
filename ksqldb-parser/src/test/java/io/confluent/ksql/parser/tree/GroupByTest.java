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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GroupByTest {

  private static final NodeLocation LOCATION = new NodeLocation(1, 4);

  @Mock
  private Expression exp1;
  @Mock
  private Expression exp2;

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            new GroupBy(Optional.empty(), ImmutableList.of(exp1, exp2)),
            new GroupBy(Optional.empty(), ImmutableList.of(exp1, exp2)),
            new GroupBy(Optional.of(LOCATION), ImmutableList.of(exp1, exp2))
        )
        .addEqualityGroup(
            new GroupBy(Optional.empty(), ImmutableList.of(exp1))
        )
        .testEquals();
  }

  @Test
  public void shouldMaintainGroupByOrder() {
    // Given:
    final List<Expression> original = ImmutableList.of(exp1, exp2);

    final GroupBy groupBy = new GroupBy(Optional.empty(), original);

    // When:
    final List<Expression> result = groupBy.getGroupingExpressions();

    // Then:
    assertThat(result, is(original));
  }

  @Test
  public void shouldThrowOnDuplicateGroupBy() {
    // Given:
    final List<Expression> withDuplicate = ImmutableList.of(exp1, exp1);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new GroupBy(Optional.empty(), withDuplicate)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Duplicate GROUP BY expression: " + exp1));
  }

  @Test
  public void shouldThrowOnEmptyGroupBy() {
    // Given:
    final List<Expression> empty = ImmutableList.of();

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new GroupBy(Optional.empty(), empty)
    );

    // Then:
    assertThat(e.getMessage(), containsString("GROUP BY requires at least one expression"));
  }
}