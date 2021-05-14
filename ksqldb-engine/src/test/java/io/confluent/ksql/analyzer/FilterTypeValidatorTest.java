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


package io.confluent.ksql.analyzer;

import static io.confluent.ksql.schema.ksql.Column.Namespace.VALUE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.INTEGER;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.STRING;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.confluent.ksql.analyzer.FilterTypeValidator.FilterType;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FilterTypeValidatorTest {
  private static final ColumnName COLUMN1 = ColumnName.of("col1");
  private static final ColumnName COLUMN2 = ColumnName.of("col2");

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private LogicalSchema schema;

  private FilterTypeValidator validator;

  @Before
  public void setUp() {
    validator = new FilterTypeValidator(schema, functionRegistry, FilterType.WHERE);
  }


  @Test
  public void shouldThrowOnBadTypeComparison() {
    // Given:
    final Expression left = new UnqualifiedColumnReferenceExp(COLUMN1);
    final Expression right = new IntegerLiteral(10);

    final Expression comparision = new ComparisonExpression(Type.EQUAL, left, right);

    when(schema.findValueColumn(any()))
        .thenReturn(Optional.of(Column.of(COLUMN1, STRING, VALUE, 10)));

    // When:
    assertThrows("Error in WHERE expression: "
        + "Cannot compare col1 (STRING) to 10 (INTEGER) with EQUAL.",
        KsqlException.class,
        () -> validator.validateFilterExpression(comparision));
  }

  @Test
  public void shouldNotThrowOnGoodTypeComparison() {
    // Given:
    final Expression left = new UnqualifiedColumnReferenceExp(COLUMN1);
    final Expression right = new IntegerLiteral(10);

    final Expression comparision = new ComparisonExpression(Type.EQUAL, left, right);

    when(schema.findValueColumn(any()))
        .thenReturn(Optional.of(Column.of(COLUMN1, INTEGER, VALUE, 10)));

    // When:
    validator.validateFilterExpression(comparision);
  }

  @Test
  public void shouldThrowOnBadTypeComparison_twoVars() {
    // Given:
    final Expression left = new UnqualifiedColumnReferenceExp(COLUMN1);
    final Expression right = new UnqualifiedColumnReferenceExp(COLUMN2);

    final Expression comparision = new ComparisonExpression(Type.EQUAL, left, right);

    when(schema.findValueColumn(COLUMN1))
        .thenReturn(Optional.of(Column.of(COLUMN1, STRING, VALUE, 10)));
    when(schema.findValueColumn(COLUMN2))
        .thenReturn(Optional.of(Column.of(COLUMN2, INTEGER, VALUE, 10)));

    // When:
    assertThrows("Error in WHERE expression: "
            + "Cannot compare col1 (STRING) to col2 (INTEGER) with EQUAL.",
        KsqlException.class,
        () -> validator.validateFilterExpression(comparision));
  }

  @Test
  public void shouldThrowOnBadType() {
    // Given:
    final Expression literal = new IntegerLiteral(10);

    // When:
    assertThrows("Type error in WHERE expression: "
            + "Should evaluate to boolean but is 10 (INTEGER) instead.",
        KsqlException.class,
        () -> validator.validateFilterExpression(literal));
  }

  @Test
  public void shouldThrowOnBadTypeCompoundComparison_leftError() {
    // Given:
    final Expression left1 = new UnqualifiedColumnReferenceExp(COLUMN1);
    final Expression right1 = new UnqualifiedColumnReferenceExp(COLUMN2);
    final Expression comparision1 = new ComparisonExpression(Type.EQUAL, left1, right1);

    final Expression left2 = new UnqualifiedColumnReferenceExp(COLUMN1);
    final Expression right2 = new StringLiteral("foo");
    final Expression comparision2 = new ComparisonExpression(Type.EQUAL, left2, right2);

    final Expression expression = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
        comparision1, comparision2);

    when(schema.findValueColumn(COLUMN1))
        .thenReturn(Optional.of(Column.of(COLUMN1, STRING, VALUE, 10)));
    when(schema.findValueColumn(COLUMN2))
        .thenReturn(Optional.of(Column.of(COLUMN2, INTEGER, VALUE, 10)));

    // When:
    assertThrows("Error in WHERE expression: "
            + "Cannot compare col1 (STRING) to col2 (INTEGER) with EQUAL.",
        KsqlException.class,
        () -> validator.validateFilterExpression(expression));
  }

  @Test
  public void shouldThrowOnBadTypeCompoundComparison_rightError() {
    // Given:
    final Expression left1 = new UnqualifiedColumnReferenceExp(COLUMN2);
    final Expression right1 = new IntegerLiteral(10);
    final Expression comparision1 = new ComparisonExpression(Type.EQUAL, left1, right1);

    final Expression left2 = new UnqualifiedColumnReferenceExp(COLUMN1);
    final Expression right2 = new UnqualifiedColumnReferenceExp(COLUMN2);
    final Expression comparision2 = new ComparisonExpression(Type.EQUAL, left2, right2);

    final Expression expression = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
        comparision1, comparision2);

    when(schema.findValueColumn(COLUMN1))
        .thenReturn(Optional.of(Column.of(COLUMN1, STRING, VALUE, 10)));
    when(schema.findValueColumn(COLUMN2))
        .thenReturn(Optional.of(Column.of(COLUMN2, INTEGER, VALUE, 10)));

    // When:
    assertThrows("Error in WHERE expression: "
            + "Cannot compare col1 (STRING) to col2 (INTEGER) with EQUAL.",
        KsqlException.class,
        () -> validator.validateFilterExpression(expression));
  }
}
