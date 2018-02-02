/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.parser;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.BinaryLiteral;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DecimalLiteral;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.ExistsPredicate;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Extract;
import io.confluent.ksql.parser.tree.FieldReference;
import io.confluent.ksql.parser.tree.FrameBound;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.GenericLiteral;
import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.InListExpression;
import io.confluent.ksql.parser.tree.InPredicate;
import io.confluent.ksql.parser.tree.IntervalLiteral;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LambdaExpression;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullIfExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryBody;
import io.confluent.ksql.parser.tree.Row;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubqueryExpression;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.SymbolReference;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.Values;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.parser.tree.Window;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WindowFrame;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class ExpressionFormatterTest {

  @Test
  public void shouldFormatRow() {
    final String result =
        ExpressionFormatter.formatExpression(new Row(Arrays.asList(new LongLiteral("1"),
            new QualifiedNameReference(QualifiedName.of(Arrays.asList("a", "b"))))));
    assertThat(result, equalTo("ROW (1, a.b)"));
  }

  @Test
  public void shouldFormatExtract() {
    final String result
        = ExpressionFormatter.formatExpression(new Extract(new StringLiteral("17/12/2017"), Extract.Field.DAY));
    assertThat(result, equalTo("EXTRACT(DAY FROM '17/12/2017')"));
  }

  @Test
  public void shouldFormatBooleanLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new BooleanLiteral("true")), equalTo("true"));
  }

  @Test
  public void shouldFormatStringLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new StringLiteral("string")), equalTo("'string'"));
  }

  @Test
  public void shouldFormatBinaryLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new BinaryLiteral("0F")), equalTo("X'0F'"));
  }

  @Test
  public void shouldFormatSubscriptExpression() {
    assertThat(ExpressionFormatter.formatExpression(new SubscriptExpression(
            new StringLiteral("abc"),
            new DoubleLiteral("3.0"))),
        equalTo("'abc'[3.0]"));
  }

  @Test
  public void shouldFormatLongLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new LongLiteral("1")), equalTo("1"));
  }

  @Test
  public void shouldFormatDoubleLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new DoubleLiteral("2.0")), equalTo("2.0"));
  }

  @Test
  public void shouldFormatDecimalLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new DecimalLiteral("3.5")), equalTo("DECIMAL '3.5'"));
  }

  @Test
  public void shouldFormatGenericLiteral() {
    assertThat(ExpressionFormatter.formatExpression(new GenericLiteral("type", "value")), equalTo("type 'value'"));
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
  public void shouldFormatIntervalLiteralWithoutEnd() {
    assertThat(ExpressionFormatter.formatExpression(
        new IntervalLiteral("10/1/2012",
            IntervalLiteral.Sign.POSITIVE,
            IntervalLiteral.IntervalField.SECOND)), equalTo("INTERVAL  '10/1/2012' SECOND"));
  }

  @Test
  public void shouldFormatIntervalLiteralWithEnd() {
    assertThat(ExpressionFormatter.formatExpression(
        new IntervalLiteral("10/1/2012",
            IntervalLiteral.Sign.POSITIVE,
            IntervalLiteral.IntervalField.SECOND,
            Optional.of(IntervalLiteral.IntervalField.MONTH))), equalTo("INTERVAL  '10/1/2012' SECOND TO MONTH"));
  }

  @Test
  public void shouldFormatQualifiedNameReference() {
    assertThat(ExpressionFormatter.formatExpression(new QualifiedNameReference(QualifiedName.of("name"))), equalTo("name"));
  }

  @Test
  public void shouldFormatSymbolReference() {
    assertThat(ExpressionFormatter.formatExpression(new SymbolReference("symbol")), equalTo("symbol"));
  }

  @Test
  public void shouldFormatDereferenceExpression() {
    assertThat(ExpressionFormatter.formatExpression(new DereferenceExpression(new StringLiteral("foo"), "name")), equalTo("'foo'.name"));
  }

  @Test
  public void shouldFormatFieldReference() {
    assertThat(ExpressionFormatter.formatExpression(new FieldReference(1)), equalTo(":input(1)"));
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
    final FunctionCall functionCall = new FunctionCall(new NodeLocation(1, 1),
        QualifiedName.of("function", "COUNT"),
        true,
        Collections.singletonList(new StringLiteral("name")));
    assertThat(ExpressionFormatter.formatExpression(functionCall), equalTo("function.COUNT(DISTINCT 'name')"));
  }

  @Test
  public void shouldFormatFunctionCallWithWindow() {
    final FunctionCall functionCall = new FunctionCall(new NodeLocation(1, 1),
        QualifiedName.of("function"),
        Optional.of(new Window("window",
            new WindowExpression("blah", new TumblingWindowExpression(1L, TimeUnit.SECONDS)))),
        false,
        Collections.singletonList(new StringLiteral("name")));

    assertThat(ExpressionFormatter.formatExpression(functionCall),
        equalTo("function('name') OVER  WINDOW  WINDOW blah  TUMBLING ( SIZE 1 SECONDS ) "));
  }

  @Test
  public void shouldFormatLambdaExpression() {
    final LambdaExpression expression = new LambdaExpression(Arrays.asList("a", "b"), new StringLiteral("something"));
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("(a, b) -> 'something'"));
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
    assertThat(ExpressionFormatter.formatExpression(new NotExpression(new LongLiteral("1"))), equalTo("(NOT 1)"));
  }

  @Test
  public void shouldFormatComparisonExpression() {
    assertThat(ExpressionFormatter.formatExpression(
        new ComparisonExpression(ComparisonExpression.Type.EQUAL,
            new LongLiteral("1"),
            new LongLiteral("1"))),
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
  public void shouldFormatNullIfExpression() {
    assertThat(ExpressionFormatter.formatExpression(new NullIfExpression(
            new StringLiteral("first"),
            new StringLiteral("second"))),
        equalTo("NULLIF('first', 'second')"));
  }

  @Test
  public void shouldFormatArithmeticUnary() {
    assertThat(ExpressionFormatter.formatExpression(new ArithmeticUnaryExpression(ArithmeticUnaryExpression.Sign.MINUS,
        new LongLiteral("1"))),
        equalTo("-1"));
  }

  @Test
  public void shouldFormatArithmeticBinary() {
    assertThat(ExpressionFormatter.formatExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD,
            new LongLiteral("1"), new LongLiteral("2"))),
        equalTo("(1 + 2)"));
  }

  @Test
  public void shouldFormatLikePredicate() {
    final LikePredicate predicate = new LikePredicate(new StringLiteral("string"), new StringLiteral("*"), new StringLiteral("\\"));
    assertThat(ExpressionFormatter.formatExpression(predicate), equalTo("('string' LIKE '*' ESCAPE '\\')"));
  }

  @Test
  public void shouldFormatCast() {
    assertThat(ExpressionFormatter.formatExpression(new Cast(new LongLiteral("1"), "Double", false)), equalTo("CAST(1 AS Double)"));
  }

  @Test
  public void shouldFormatTryCast() {
    assertThat(ExpressionFormatter.formatExpression(new Cast(new LongLiteral("1"), "Double", true)), equalTo("TRY_CAST(1 AS Double)"));
  }

  @Test
  public void shouldFormatSearchedCaseExpression() {
    final SearchedCaseExpression expression = new SearchedCaseExpression(
        Collections.singletonList(
            new WhenClause(new StringLiteral("foo"),
                new LongLiteral("1"))),
        Optional.empty());
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("(CASE WHEN 'foo' THEN 1 END)"));
  }

  @Test
  public void shouldFormatSearchedCaseExpressionWithDefaultValue() {
    final SearchedCaseExpression expression = new SearchedCaseExpression(
        Collections.singletonList(
            new WhenClause(new StringLiteral("foo"),
                new LongLiteral("1"))),
        Optional.of(new LongLiteral("2")));
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("(CASE WHEN 'foo' THEN 1 ELSE 2 END)"));
  }

  @Test
  public void shouldFormatSimpleCaseExpressionWithDefaultValue() {
    final SimpleCaseExpression expression = new SimpleCaseExpression(
        new StringLiteral("operand"),
        Collections.singletonList(
            new WhenClause(new StringLiteral("foo"),
                new LongLiteral("1"))),
        Optional.of(new LongLiteral("2")));
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("(CASE 'operand' WHEN 'foo' THEN 1 ELSE 2 END)"));
  }

  @Test
  public void shouldFormatSimpleCaseExpression() {
    final SimpleCaseExpression expression = new SimpleCaseExpression(
        new StringLiteral("operand"),
        Collections.singletonList(
            new WhenClause(new StringLiteral("foo"),
                new LongLiteral("1"))),
        Optional.empty());
    assertThat(ExpressionFormatter.formatExpression(expression), equalTo("(CASE 'operand' WHEN 'foo' THEN 1 END)"));
  }

  @Test
  public void shouldFormatWhen() {
    assertThat(ExpressionFormatter.formatExpression(new WhenClause(new LongLiteral("1"), new LongLiteral("2"))), equalTo("WHEN 1 THEN 2"));
  }

  @Test
  public void shouldFormatBetweenPredicate() {
    final BetweenPredicate predicate = new BetweenPredicate(new StringLiteral("blah"), new LongLiteral("5"), new LongLiteral("10"));
    assertThat(ExpressionFormatter.formatExpression(predicate), equalTo("('blah' BETWEEN 5 AND 10)"));
  }

  @Test
  public void shouldFormatInPredicate() {
    assertThat(ExpressionFormatter.formatExpression(new InPredicate(new StringLiteral("foo"), new StringLiteral("a"))),
        equalTo("('foo' IN 'a')"));
  }

  @Test
  public void shouldFormatInListExpression() {
    assertThat(ExpressionFormatter.formatExpression(new InListExpression(Collections.singletonList(new StringLiteral("a")))), equalTo("('a')"));
  }

}