/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

public class ImplicitlyCastResolverTest {
  private static final SqlDecimal DECIMAL_5_2 = SqlDecimal.of(5, 2);

  @Test
  public void shouldNotResolveNonDecimalTarget() {
    // When
    final Expression expression =
        ImplicitlyCastResolver.resolve(new IntegerLiteral(5), SqlTypes.STRING);

    // Then
    assertThat(expression, instanceOf(IntegerLiteral.class));
    assertThat(((IntegerLiteral)expression).getValue(), is(5));
  }

  @Test
  public void shouldCastToDecimal() {
    // Given
    final Map<Literal, BigDecimal> fromLiterals = ImmutableMap.of(
        new IntegerLiteral(5), new BigDecimal("5.00"),
        new LongLiteral(5), new BigDecimal("5.00"),
        new DoubleLiteral(5), new BigDecimal("5.00"),
        new DecimalLiteral(BigDecimal.TEN), new BigDecimal("10.00"),
        new DecimalLiteral(new BigDecimal("10.1")), new BigDecimal("10.10")
    );

    for (final Map.Entry<Literal, BigDecimal> entry : fromLiterals.entrySet()) {
      final Literal literal = entry.getKey();
      final BigDecimal expected = entry.getValue();

      // When
      final Expression expression =
          ImplicitlyCastResolver.resolve(literal, DECIMAL_5_2);

      // Then
      assertThat("Should cast " + literal.getClass().getSimpleName() + " to " + DECIMAL_5_2,
          expression, instanceOf(DecimalLiteral.class));
      assertThat("Should cast " + literal.getClass().getSimpleName() + " to " + DECIMAL_5_2,
          ((DecimalLiteral)expression).getValue(),
          is(expected)
      );
    }
  }

  @Test
  public void shouldNotCastToDecimal() {
    // Given
    final List<Literal> fromLiterals = Arrays.asList(
        new BooleanLiteral("true"),
        new StringLiteral("10.2"),
        new DecimalLiteral(BigDecimal.valueOf(10.133))
    );

    for (final Literal literal : fromLiterals) {
      // When
      final Expression expression =
          ImplicitlyCastResolver.resolve(literal, DECIMAL_5_2);

      // Then
      assertThat("Should not cast " + literal.getClass().getSimpleName() + " to " + DECIMAL_5_2,
          expression, instanceOf(literal.getClass()));
      assertThat("Should not cast " + literal.getClass().getSimpleName() + " to " + DECIMAL_5_2,
          expression.equals(literal), is(true));
    }
  }
}
