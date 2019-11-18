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

package io.confluent.ksql.parser;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.Operator;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExpressionParserTest {
  private static final IntegerLiteral ONE = new IntegerLiteral(1);
  private static final IntegerLiteral TWO = new IntegerLiteral(2);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldParseExpression() {
    // When:
    final Expression parsed = ExpressionParser.parseExpression("1 + 2");

    // Then:
    assertThat(
        parsed,
        equalTo(new ArithmeticBinaryExpression(parsed.getLocation(), Operator.ADD, ONE, TWO))
    );
  }

  @Test
  public void shouldParseSelectExpression() {
    // When:
    final SelectExpression parsed =
        ExpressionParser.parseSelectExpression("1 + 2 AS `three`");

    // Then:
    assertThat(
        parsed,
        equalTo(
            SelectExpression.of(
                ColumnName.of("three"),
                new ArithmeticBinaryExpression(
                    parsed.getExpression().getLocation(), Operator.ADD, ONE, TWO
                )
            )
        )
    );
  }

  @Test
  public void shouldThrowOnSelectExpressionWithoutAlias() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Select item must have identifier in: 1 + 2");

    // When:
    ExpressionParser.parseSelectExpression("1 + 2");
  }

  @Test
  public void shouldThrowOnAllColumns() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Illegal select item type in: *");

    // When:
    ExpressionParser.parseSelectExpression("*");
  }

  @Test
  public void shouldParseWindowExpression() {
    // When:
    final KsqlWindowExpression parsed = ExpressionParser.parseWindowExpression(
        "TUMBLING (SIZE 1 DAYS)"
    );

    // Then:
    assertThat(
        parsed,
        equalTo(new TumblingWindowExpression(parsed.getLocation(), 1, TimeUnit.DAYS))
    );
  }
}