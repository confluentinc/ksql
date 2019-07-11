/*
 * Copyright 2018 Confluent Inc.
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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DecimalLiteral;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.InListExpression;
import io.confluent.ksql.parser.tree.InPredicate;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;

public class ExpressionFormatterTest {

  @Test
  public void shouldFormatBooleanLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new BooleanLiteral("true")), equalTo("true"));
  }

  @Test
  public void shouldFormatStringLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new StringLiteral("string")), equalTo("'string'"));
  }

  @Test
  public void shouldFormatSubscriptExpression() {
    assertThat(ExpressionFormatter.formatExpression(new SubscriptExpression(
            new StringLiteral("abc"),
            new DoubleLiteral(3.0))),
        equalTo("'abc'[3.0]"));
  }

  @Test
  public void shouldFormatLongLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new LongLiteral(1)), equalTo("1"));
  }

  @Test
  public void shouldFormatDoubleLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new DoubleLiteral(2.0)), equalTo("2.0"));
  }

  @Test
  public void shouldFormatDecimalLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new DecimalLiteral("3.5")), equalTo("DECIMAL '3.5'"));
  }

  @Test
  public void shouldFormatTimeLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new TimeLiteral("17/9/2017")), equalTo("TIME '17/9/2017'"));
  }

  @Test
  public void shouldFormatTimestampLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new TimestampLiteral("15673839303")), equalTo("TIMESTAMP '15673839303'"));
  }

  @Test
  public void shouldFormatNullLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new NullLiteral()), equalTo("null"));
  }

  @Test
  public void shouldFormatQualifiedNameReference() {
    assertThat(ExpressionFormatter.formatExpression(new QualifiedNameReference(QualifiedName.of("name"))), equalTo("name"));
  }

  @Test
  public void shouldFormatDereferenceExpression() {
    assertThat(ExpressionFormatter.formatExpression(new DereferenceExpression(new StringLiteral("foo"), "name")), equalTo("'foo'->name"));
  }

  @Test
  public void shouldFormatFunctionCallWithCount() {
    final FunctionCall functionCall = new FunctionCall(QualifiedName.of("function", "COUNT"),
        Collections.singletonList(new StringLiteral("name")));

    assertThat(ExpressionFormatter.formatExpression(functionCall), equalTo("function.COUNT('name')"));
  }

  @Test
  public void shouldFormatFunctionCountStar() {
    final FunctionCall functionCall = new FunctionCall(QualifiedName.of("function", "COUNT"), Collections.emptyList());
    assertThat(ExpressionFormatter.formatExpression(functionCall), equalTo("function.COUNT(*)"));
  }

  @Test
  public void shouldFormatFunctionWithDistinct() {
    final FunctionCall functionCall = new FunctionCall(
        QualifiedName.of("function", "COUNT"),
        Collections.singletonList(new StringLiteral("name")));
    assertThat(ExpressionFormatter.formatExpression(functionCall), equalTo("function.COUNT('name')"));
  }

  @Test
  public void shouldFormatLogicalBinaryExpression() {
    final LogicalBinaryExpression expression = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
        new StringLiteral("a"),
        new StringLiteral("b"));
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("('a' AND 'b')"));
  }

  @Test
  public void shouldFormatNotExpression() {
    assertThat(ExpressionFormatter.formatExpression(new NotExpression(new LongLiteral(1))), equalTo("(NOT 1)"));
  }

  @Test
  public void shouldFormatComparisonExpression() {
    assertThat(ExpressionFormatter.formatExpression(
        new ComparisonExpression(ComparisonExpression.Type.EQUAL,
            new LongLiteral(1),
            new LongLiteral(1))),
        equalTo("(1 = 1)"));
  }

  @Test
  public void shouldFormatIsNullPredicate() {
    assertThat(ExpressionFormatter.formatExpression(new IsNullPredicate(new StringLiteral("name"))),
        equalTo("('name' IS NULL)"));
  }

  @Test
  public void shouldFormatIsNotNullPredicate() {
    assertThat(ExpressionFormatter.formatExpression(new IsNotNullPredicate(new StringLiteral("name"))),
        equalTo("('name' IS NOT NULL)"));
  }

  @Test
  public void shouldFormatArithmeticUnary() {
    assertThat(ExpressionFormatter.formatExpression(
        ArithmeticUnaryExpression.negative(Optional.empty(), new LongLiteral(1))),
        equalTo("-1"));
  }

  @Test
  public void shouldFormatArithmeticBinary() {
    assertThat(ExpressionFormatter.formatExpression(new ArithmeticBinaryExpression(Operator.ADD,
            new LongLiteral(1), new LongLiteral(2))),
        equalTo("(1 + 2)"));
  }

  @Test
  public void shouldFormatLikePredicate() {
    final LikePredicate predicate = new LikePredicate(new StringLiteral("string"), new StringLiteral("*"));
    assertThat(ExpressionFormatter.formatExpression(predicate), equalTo("('string' LIKE '*')"));
  }

  @Test
  public void shouldFormatCast() {
    // Given:
    final Cast cast = new Cast(
        new LongLiteral(1),
        new Type(SqlTypes.DOUBLE));

    // When:
    final String result = ExpressionFormatter.formatExpression(cast);

    // Then:
    assertThat(result, equalTo("CAST(1 AS DOUBLE)"));
  }

  @Test
  public void shouldFormatSearchedCaseExpression() {
    final SearchedCaseExpression expression = new SearchedCaseExpression(
        Collections.singletonList(
            new WhenClause(new StringLiteral("foo"),
                new LongLiteral(1))),
        Optional.empty());
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("(CASE WHEN 'foo' THEN 1 END)"));
  }

  @Test
  public void shouldFormatSearchedCaseExpressionWithDefaultValue() {
    final SearchedCaseExpression expression = new SearchedCaseExpression(
        Collections.singletonList(
            new WhenClause(new StringLiteral("foo"),
                new LongLiteral(1))),
        Optional.of(new LongLiteral(2)));
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("(CASE WHEN 'foo' THEN 1 ELSE 2 END)"));
  }

  @Test
  public void shouldFormatSimpleCaseExpressionWithDefaultValue() {
    final SimpleCaseExpression expression = new SimpleCaseExpression(
        new StringLiteral("operand"),
        Collections.singletonList(
            new WhenClause(new StringLiteral("foo"),
                new LongLiteral(1))),
        Optional.of(new LongLiteral(2)));
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("(CASE 'operand' WHEN 'foo' THEN 1 ELSE 2 END)"));
  }

  @Test
  public void shouldFormatSimpleCaseExpression() {
    final SimpleCaseExpression expression = new SimpleCaseExpression(
        new StringLiteral("operand"),
        Collections.singletonList(
            new WhenClause(new StringLiteral("foo"),
                new LongLiteral(1))),
        Optional.empty());
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("(CASE 'operand' WHEN 'foo' THEN 1 END)"));
  }

  @Test
  public void shouldFormatWhen() {
    assertThat(ExpressionFormatter.formatExpression(new WhenClause(new LongLiteral(1), new LongLiteral(2))), equalTo("WHEN 1 THEN 2"));
  }

  @Test
  public void shouldFormatBetweenPredicate() {
    final BetweenPredicate predicate = new BetweenPredicate(new StringLiteral("blah"), new LongLiteral(5), new LongLiteral(10));
    assertThat(ExpressionFormatter.formatExpression(predicate), equalTo("('blah' BETWEEN 5 AND 10)"));
  }

  @Test
  public void shouldFormatInPredicate() {
    final InPredicate predicate = new InPredicate(
        new StringLiteral("foo"),
        new InListExpression(ImmutableList.of(new StringLiteral("a"))));

    assertThat(ExpressionFormatter.formatExpression(predicate), equalTo("('foo' IN ('a'))"));
  }

  @Test
  public void shouldFormatInListExpression() {
    assertThat(ExpressionFormatter.formatExpression(new InListExpression(Collections.singletonList(new StringLiteral("a")))), equalTo("('a')"));
  }

  @Test
  public void shouldFormatStruct() {
    final SqlStruct struct = SqlStruct.builder()
        .field("field1", SqlTypes.INTEGER)
        .field("field2", SqlTypes.STRING)
        .build();

    assertThat(
        ExpressionFormatter.formatExpression(new Type(struct)),
        equalTo("STRUCT<field1 INTEGER, field2 STRING>"));
  }

  @Test
  public void shouldFormatStructWithColumnWithReservedWordName() {
    final SqlStruct struct = SqlStruct.builder()
        .field("END", SqlTypes.INTEGER)
        .build();

    assertThat(
        ExpressionFormatter.formatExpression(new Type(struct)),
        equalTo("STRUCT<`END` INTEGER>"));
  }

  @Test
  public void shouldFormatMap() {
    final SqlMap map = SqlTypes.map(SqlTypes.BIGINT);
    assertThat(ExpressionFormatter.formatExpression(new Type(map)),
        equalTo("MAP<VARCHAR, BIGINT>"));
  }

  @Test
  public void shouldFormatArray() {
    final SqlArray array = SqlTypes.array(SqlTypes.BOOLEAN);
    assertThat(ExpressionFormatter.formatExpression(new Type(array)), equalTo("ARRAY<BOOLEAN>"));
  }
}