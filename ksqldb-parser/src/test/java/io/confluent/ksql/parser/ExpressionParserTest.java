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

package io.confluent.ksql.parser;

import static io.confluent.ksql.parser.ExpressionParser.parseSelectExpression;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.Operator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ExpressionParserTest {
  private static final IntegerLiteral ONE = new IntegerLiteral(1);
  private static final IntegerLiteral TWO = new IntegerLiteral(2);

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
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> parseSelectExpression("1 + 2")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Select item must have identifier in: 1 + 2"));
  }

  @Test
  public void shouldThrowOnAllColumns() {
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> parseSelectExpression("*")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Illegal select item type in: *"));
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
        equalTo(new TumblingWindowExpression(
            parsed.getLocation(),
            new WindowTimeClause(1, TimeUnit.DAYS),
            Optional.empty(),
            Optional.empty()))
    );
  }

  @Test
  public void shouldParseWindowExpressionWithRetention() {
    // When:
    final KsqlWindowExpression parsed = ExpressionParser.parseWindowExpression(
        "TUMBLING (SIZE 1 DAYS, RETENTION 2 DAYS, GRACE PERIOD 2 DAYS)"
    );

    // Then:
    assertThat(
        parsed,
        equalTo(new TumblingWindowExpression(
            parsed.getLocation(),
            new WindowTimeClause(1, TimeUnit.DAYS),
            Optional.of(new WindowTimeClause(2, TimeUnit.DAYS)),
            Optional.of(new WindowTimeClause(2, TimeUnit.DAYS)))
        )
    );
  }
}