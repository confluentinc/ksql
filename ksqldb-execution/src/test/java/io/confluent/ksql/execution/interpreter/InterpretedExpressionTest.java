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

package io.confluent.ksql.execution.interpreter;

import static io.confluent.ksql.execution.testutil.TestExpressions.COL11;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL1;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL3;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL7;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL8;
import static io.confluent.ksql.execution.testutil.TestExpressions.SCHEMA;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.IntegerType;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class InterpretedExpressionTest {

  public final static Schema ADDRESS_SCHEMA = SchemaBuilder.struct()
      .field("NUMBER", SchemaBuilder.int64())
      .field("STREET", SchemaBuilder.string())
      .field("CITY", SchemaBuilder.string())
      .field("STATE", SchemaBuilder.string())
      .field("ZIPCODE", SchemaBuilder.int64())
      .schema();

  private Struct ADDRESS = new Struct(ADDRESS_SCHEMA)
      .put("NUMBER", 123L)
      .put("STREET", "Main Street")
      .put("CITY", "Mountain View")
      .put("STATE", "CA")
      .put("ZIPCODE", 90210L);

  private GenericRow ROW = new GenericRow().appendAll(ImmutableList.of(
      15, "A", "b", 4.5, ImmutableList.of(2.5), ImmutableMap.of(123, 6.7), ADDRESS, 35,
      new BigDecimal("3.4"), new BigDecimal("8.9"), new Timestamp(1234), true
  ));

  @Mock
  private FunctionRegistry functionRegistry;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private KsqlConfig ksqlConfig;

  @Before
  public void init() {
    ksqlConfig = new KsqlConfig(Collections.emptyMap());
  }

  private InterpretedExpression interpreter(Expression expression) {
    return InterpretedExpressionFactory.create(
        expression,
        SCHEMA,
        functionRegistry,
        ksqlConfig
    );
  }

  private GenericRow make(int rowNum, Object val) {
    int i = 0;
    GenericRow row = new GenericRow();
    for (Object o : ROW.values()) {
      if (i == rowNum) {
        row.append(val);
      } else {
        row.append(o);
      }
      i++;
    }
    return row;
  }

  @Test
  public void shouldEvaluateComparisons_double() {
    // Given:
    final Expression expression1 = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        COL3,
        new DoubleLiteral(-10.0)
    );
    final Expression expression2 = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN,
        COL3,
        new DoubleLiteral(10)
    );
    final Expression expression3 = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        COL3,
        new DoubleLiteral(6.5)
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);
    InterpretedExpression interpreter3 = interpreter(expression3);

    // Then:
    assertThat(interpreter1.evaluate(make(3, 5d)), is(true));
    assertThat(interpreter1.evaluate(make(3, -20d)), is(false));
    assertThat(interpreter2.evaluate(make(3, 5d)), is(true));
    assertThat(interpreter2.evaluate(make(3, 20d)), is(false));
    assertThat(interpreter3.evaluate(make(3, 6.5d)), is(true));
    assertThat(interpreter3.evaluate(make(3, 8.5d)), is(false));
  }

  @Test
  public void shouldEvaluateComparisons_int() {
    // Given:
    final Expression expression1 = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        COL7,
        new IntegerLiteral(10)
    );
    final Expression expression2 = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN,
        COL7,
        new IntegerLiteral(20)
    );
    final Expression expression3 = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        COL7,
        new DoubleLiteral(30)
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);
    InterpretedExpression interpreter3 = interpreter(expression3);

    // Then:
    assertThat(interpreter1.evaluate(make(7, 30)), is(true));
    assertThat(interpreter1.evaluate(make(7, 4)), is(false));
    assertThat(interpreter2.evaluate(make(7, 13)), is(true));
    assertThat(interpreter2.evaluate(make(7, 20)), is(false));
    assertThat(interpreter3.evaluate(make(7, 30)), is(true));
    assertThat(interpreter3.evaluate(make(7, 31)), is(false));
  }

  @Test
  public void shouldEvaluateComparisons_decimal() {
    // Given:
    final Expression expression1 = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        COL8,
        new DecimalLiteral(new BigDecimal("1.2"))
    );
    final Expression expression2 = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN,
        COL8,
        new DecimalLiteral(new BigDecimal("5.1"))
    );
    final Expression expression3 = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        COL8,
        new DecimalLiteral(new BigDecimal("10.4"))
    );
    final Expression expression4 = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        COL8,
        new IntegerLiteral(5)
    );
    final Expression expression5 = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN,
        COL8,
        new DoubleLiteral(6.5)
    );
    final Expression expression6 = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN,
        COL8,
        new LongLiteral(10L)
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);
    InterpretedExpression interpreter3 = interpreter(expression3);
    InterpretedExpression interpreter4 = interpreter(expression4);
    InterpretedExpression interpreter5 = interpreter(expression5);
    InterpretedExpression interpreter6 = interpreter(expression6);

    // Then:
    assertThat(interpreter1.evaluate(make(8, new BigDecimal("3.4"))), is(true));
    assertThat(interpreter1.evaluate(make(8, new BigDecimal("1.1"))), is(false));
    assertThat(interpreter2.evaluate(make(8, new BigDecimal("4.9"))), is(true));
    assertThat(interpreter2.evaluate(make(8, new BigDecimal("5.2"))), is(false));
    assertThat(interpreter3.evaluate(make(8, new BigDecimal("10.4"))), is(true));
    assertThat(interpreter3.evaluate(make(8, new BigDecimal("10.5"))), is(false));
    assertThat(interpreter4.evaluate(make(8, new BigDecimal("6.5"))), is(true));
    assertThat(interpreter4.evaluate(make(8, new BigDecimal("4.5"))), is(false));
    assertThat(interpreter5.evaluate(make(8, new BigDecimal("5.5"))), is(true));
    assertThat(interpreter5.evaluate(make(8, new BigDecimal("7.5"))), is(false));
    assertThat(interpreter6.evaluate(make(8, new BigDecimal("7"))), is(true));
    assertThat(interpreter6.evaluate(make(8, new BigDecimal("19.567"))), is(false));
  }

  @Test
  public void shouldEvaluateLogicalExpressions_and() {
    // Given:
    final Expression expression1 = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        COL11,
        new BooleanLiteral(true)
    );
    final Expression expression2 = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        COL11,
        new BooleanLiteral(false)
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);

    // Then:
    assertThat(interpreter1.evaluate(make(11, true)), is(true));
    assertThat(interpreter1.evaluate(make(11, false)), is(false));
    assertThat(interpreter2.evaluate(make(11, true)), is(false));
    assertThat(interpreter2.evaluate(make(11, false)), is(false));
  }

  @Test
  public void shouldEvaluateLogicalExpressions_or() {
    // Given:
    final Expression expression1 = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.OR,
        COL11,
        new BooleanLiteral(true)
    );
    final Expression expression2 = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.OR,
        COL11,
        new BooleanLiteral(false)
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);

    // Then:
    assertThat(interpreter1.evaluate(make(11, true)), is(true));
    assertThat(interpreter1.evaluate(make(11, false)), is(true));
    assertThat(interpreter2.evaluate(make(11, true)), is(true));
    assertThat(interpreter2.evaluate(make(11, false)), is(false));
  }

  @Test
  public void shouldHandleFunctionCallsWithGenerics() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    when(udf.newInstance(any())).thenReturn(new AddUdf());
    givenUdf("FOO", udfFactory, udf);
    when(udf.parameters()).thenReturn(ImmutableList.of(GenericType.of("T"), GenericType.of("T")));

    // When:
    InterpretedExpression interpreter1 = interpreter(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(
                new IntegerLiteral(1),
                new IntegerLiteral(1))
        )
    );
    final Object object = interpreter1.evaluate(ROW);

    // Then:
    assertThat(object, is(2));
  }

  @Test
  public void shouldHandleFunctionCalls_intParams() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    when(udf.newInstance(any())).thenReturn(new AddUdf());
    givenUdf("FOO", udfFactory, udf);
    when(udf.parameters()).thenReturn(ImmutableList.of(IntegerType.INSTANCE, IntegerType.INSTANCE));

    // When:
    InterpretedExpression interpreter1 = interpreter(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(
                new IntegerLiteral(1),
                new IntegerLiteral(1))
        )
    );
    final Object object = interpreter1.evaluate(ROW);


    // Then:
    assertThat(object, is(2));
  }

  @Test
  public void shouldEvaluateIsNullPredicate() {
    // Given:
    final Expression expression1 = new IsNullPredicate(
        COL11
    );
    final Expression expression2 = new IsNullPredicate(
        new NullLiteral()
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);

    // Then:
    assertThat(interpreter1.evaluate(make(11, true)), is(false));
    assertThat(interpreter1.evaluate(make(11, null)), is(true));
    assertThat(interpreter2.evaluate(ROW), is(true));
  }

  @Test
  public void shouldEvaluateIsNotNullPredicate() {
    // Given:
    final Expression expression1 = new IsNotNullPredicate(
        COL11
    );
    final Expression expression2 = new IsNotNullPredicate(
        new NullLiteral()
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);

    // Then:
    assertThat(interpreter1.evaluate(make(11, true)), is(true));
    assertThat(interpreter1.evaluate(make(11, null)), is(false));
    assertThat(interpreter2.evaluate(ROW), is(false));
  }

  @Test
  public void shouldEvaluateCastToInteger() {
    // Given:
    final Expression cast1 = new Cast(
        new LongLiteral(10L),
        new Type(SqlPrimitiveType.of("INTEGER"))
    );
    final Expression cast2 = new Cast(
        new StringLiteral("1234"),
        new Type(SqlPrimitiveType.of("INTEGER"))
    );
    final Expression cast3 = new Cast(
         new DoubleLiteral(12.5),
        new Type(SqlPrimitiveType.of("INTEGER"))
    );
    final Expression cast4 = new Cast(
        new DecimalLiteral(new BigDecimal("4567.5")),
        new Type(SqlPrimitiveType.of("INTEGER"))
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(cast1);
    InterpretedExpression interpreter2 = interpreter(cast2);
    InterpretedExpression interpreter3 = interpreter(cast3);
    InterpretedExpression interpreter4 = interpreter(cast4);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(10));
    assertThat(interpreter2.evaluate(ROW), is(1234));
    assertThat(interpreter3.evaluate(ROW), is(12));
    assertThat(interpreter4.evaluate(ROW), is(4567));
  }

  @Test
  public void shouldEvaluateCastToBigint() {
    // Given:
    final Expression cast1 = new Cast(
        new IntegerLiteral(10),
        new Type(SqlPrimitiveType.of("BIGINT"))
    );
    final Expression cast2 = new Cast(
        new StringLiteral("1234"),
        new Type(SqlPrimitiveType.of("BIGINT"))
    );
    final Expression cast3 = new Cast(
        new DoubleLiteral(12.5),
        new Type(SqlPrimitiveType.of("BIGINT"))
    );
    final Expression cast4 = new Cast(
        new DecimalLiteral(new BigDecimal("4567.5")),
        new Type(SqlPrimitiveType.of("BIGINT"))
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(cast1);
    InterpretedExpression interpreter2 = interpreter(cast2);
    InterpretedExpression interpreter3 = interpreter(cast3);
    InterpretedExpression interpreter4 = interpreter(cast4);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(10L));
    assertThat(interpreter2.evaluate(ROW), is(1234L));
    assertThat(interpreter3.evaluate(ROW), is(12L));
    assertThat(interpreter4.evaluate(ROW), is(4567L));
  }

  @Test
  public void shouldEvaluateCastToDouble() {
    // Given:
    final Expression cast1 = new Cast(
        new LongLiteral(10L),
        new Type(SqlPrimitiveType.of("DOUBLE"))
    );
    final Expression cast2 = new Cast(
        new StringLiteral("1234.5"),
        new Type(SqlPrimitiveType.of("DOUBLE"))
    );
    final Expression cast3 = new Cast(
        new IntegerLiteral(12),
        new Type(SqlPrimitiveType.of("DOUBLE"))
    );
    final Expression cast4 = new Cast(
        new DecimalLiteral(new BigDecimal("4567.5")),
        new Type(SqlPrimitiveType.of("DOUBLE"))
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(cast1);
    InterpretedExpression interpreter2 = interpreter(cast2);
    InterpretedExpression interpreter3 = interpreter(cast3);
    InterpretedExpression interpreter4 = interpreter(cast4);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(10d));
    assertThat(interpreter2.evaluate(ROW), is(1234.5d));
    assertThat(interpreter3.evaluate(ROW), is(12d));
    assertThat(interpreter4.evaluate(ROW), is(4567.5d));
  }

  @Test
  public void shouldEvaluateCastToDecimal() {
    // Given:
    final Expression cast1 = new Cast(
        new LongLiteral(10L),
        new Type(SqlTypes.decimal(10, 1))
    );
    final Expression cast2 = new Cast(
        new StringLiteral("1234.5"),
        new Type(SqlTypes.decimal(10, 1))
    );
    final Expression cast3 = new Cast(
        new IntegerLiteral(12),
        new Type(SqlTypes.decimal(10, 1))
    );
    final Expression cast4 = new Cast(
        new DoubleLiteral(4567.5),
        new Type(SqlTypes.decimal(10, 1))
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(cast1);
    InterpretedExpression interpreter2 = interpreter(cast2);
    InterpretedExpression interpreter3 = interpreter(cast3);
    InterpretedExpression interpreter4 = interpreter(cast4);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(BigDecimal.valueOf(10L).setScale(1)));
    assertThat(interpreter2.evaluate(ROW), is(BigDecimal.valueOf(1234.5d).setScale(1)));
    assertThat(interpreter3.evaluate(ROW), is(BigDecimal.valueOf(12).setScale(1)));
    assertThat(interpreter4.evaluate(ROW), is(BigDecimal.valueOf(4567.5d).setScale(1)));
  }

  @Test
  public void shouldEvaluateCastToTimestamp() {
    // Given:
    final Expression cast1 = new Cast(
        new TimestampLiteral(Timestamp.from(Instant.ofEpochMilli(1000))),
        new Type(SqlPrimitiveType.of("TIMESTAMP"))
    );
    final Expression cast2 = new Cast(
        new StringLiteral("2017-11-13T23:59:58"),
        new Type(SqlPrimitiveType.of("TIMESTAMP"))
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(cast1);
    InterpretedExpression interpreter2 = interpreter(cast2);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(new Timestamp(1000L)));
    assertThat(interpreter2.evaluate(ROW), is(new Timestamp(1510617598000L)));
  }

  @Test
  public void shouldEvaluateCastToString() {
    // Given:
    final Expression cast1 = new Cast(
        new IntegerLiteral(10),
        new Type(SqlPrimitiveType.of("STRING"))
    );
    final Expression cast2 = new Cast(
        new LongLiteral(1234L),
        new Type(SqlPrimitiveType.of("STRING"))
    );
    final Expression cast3 = new Cast(
        new DoubleLiteral(12.5),
        new Type(SqlPrimitiveType.of("STRING"))
    );
    final Expression cast4 = new Cast(
        new DecimalLiteral(new BigDecimal("4567.5")),
        new Type(SqlPrimitiveType.of("STRING"))
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(cast1);
    InterpretedExpression interpreter2 = interpreter(cast2);
    InterpretedExpression interpreter3 = interpreter(cast3);
    InterpretedExpression interpreter4 = interpreter(cast4);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is("10"));
    assertThat(interpreter2.evaluate(ROW), is("1234"));
    assertThat(interpreter3.evaluate(ROW), is("12.5"));
    assertThat(interpreter4.evaluate(ROW), is("4567.5"));
  }

  @Test
  public void shouldEvaluateCastToArray() {
    // Given:
    final Expression cast1 = new Cast(
        new CreateArrayExpression(ImmutableList.of(
            new StringLiteral("1"), new StringLiteral("2"))),
        new Type(SqlTypes.array(SqlTypes.INTEGER))
    );
    final Expression cast2 = new Cast(
        new CreateArrayExpression(ImmutableList.of(
            new DoubleLiteral(2.5), new DoubleLiteral(3.6))),
        new Type(SqlTypes.array(SqlTypes.INTEGER))
    );
    final Expression cast3 = new Cast(
        new CreateArrayExpression(ImmutableList.of(
            new DoubleLiteral(2.5), new DoubleLiteral(3.6))),
        new Type(SqlTypes.array(SqlTypes.STRING))
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(cast1);
    InterpretedExpression interpreter2 = interpreter(cast2);
    InterpretedExpression interpreter3 = interpreter(cast3);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(ImmutableList.of(1, 2)));
    assertThat(interpreter2.evaluate(ROW), is(ImmutableList.of(2, 3)));
    assertThat(interpreter3.evaluate(ROW), is(ImmutableList.of("2.5", "3.6")));
  }

  @Test
  public void shouldEvaluateCastToMap() {
    // Given:
    final Expression cast1 = new Cast(
        new CreateMapExpression(ImmutableMap.of(
            new StringLiteral("1"), new StringLiteral("2"),
            new StringLiteral("3"), new StringLiteral("4"))),
        new Type(SqlTypes.map(SqlTypes.INTEGER, SqlTypes.INTEGER))
    );
    final Expression cast2 = new Cast(
        new CreateMapExpression(ImmutableMap.of(
            new DoubleLiteral(2.5), new StringLiteral("2"),
            new DoubleLiteral(3.5), new StringLiteral("4"))),
        new Type(SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT))
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(cast1);
    InterpretedExpression interpreter2 = interpreter(cast2);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(ImmutableMap.of(1, 2, 3, 4)));
    assertThat(interpreter2.evaluate(ROW), is(ImmutableMap.of("2.5", 2L, "3.5", 4L)));
  }

  @Test
  public void shouldEvaluateArithmetic() {
    // Given:
    final Expression expression1 = new ArithmeticBinaryExpression(
        Operator.ADD, new IntegerLiteral(1), new IntegerLiteral(2)
    );
    final Expression expression2 = new ArithmeticBinaryExpression(
        Operator.ADD, new IntegerLiteral(1), new LongLiteral(4)
    );
    final Expression expression3 = new ArithmeticBinaryExpression(
        Operator.ADD, new DoubleLiteral(5.5), new LongLiteral(4)
    );
    final Expression expression4 = new ArithmeticBinaryExpression(
        Operator.MULTIPLY, new IntegerLiteral(5), new LongLiteral(4)
    );
    final Expression expression5 = new ArithmeticBinaryExpression(
        Operator.DIVIDE, new LongLiteral(18), new LongLiteral(3)
    );
    final Expression expression6 = new ArithmeticBinaryExpression(
        Operator.MODULUS, new LongLiteral(20), new LongLiteral(3)
    );
    final Expression expression7 = new ArithmeticBinaryExpression(
        Operator.ADD, new DecimalLiteral(new BigDecimal("12.5").setScale(2)),
        new DecimalLiteral(new BigDecimal("1.25").setScale(2))
    );
    final Expression expression8 = new ArithmeticBinaryExpression(
        Operator.ADD, new DecimalLiteral(new BigDecimal("12.5").setScale(2)),
        new DoubleLiteral(2.0d)
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);
    InterpretedExpression interpreter3 = interpreter(expression3);
    InterpretedExpression interpreter4 = interpreter(expression4);
    InterpretedExpression interpreter5 = interpreter(expression5);
    InterpretedExpression interpreter6 = interpreter(expression6);
    InterpretedExpression interpreter7 = interpreter(expression7);
    InterpretedExpression interpreter8 = interpreter(expression8);


    // Then:
    assertThat(interpreter1.evaluate(ROW), is(3));
    assertThat(interpreter2.evaluate(ROW), is(5L));
    assertThat(interpreter3.evaluate(ROW), is(9.5d));
    assertThat(interpreter4.evaluate(ROW), is(20L));
    assertThat(interpreter5.evaluate(ROW), is(6L));
    assertThat(interpreter6.evaluate(ROW), is(2L));
    assertThat(interpreter7.evaluate(ROW), is(BigDecimal.valueOf(13.75).setScale(2)));
    assertThat(interpreter8.evaluate(ROW), is(14.5d));
  }

  @Test
  public void shouldEvaluateSearchedCase() {
    // Given:
    final Expression case1 = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.GREATER_THAN,
                    COL7,
                    new IntegerLiteral(10)
                ),
                new StringLiteral("Large")
            ),
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.GREATER_THAN,
                    COL7,
                    new IntegerLiteral(5)
                ),
                new StringLiteral("Medium")
            ),
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.GREATER_THAN,
                    COL7,
                    new IntegerLiteral(2)
                ),
                new StringLiteral("Small")
            )
        ),
        Optional.of(new StringLiteral("Tiny"))
    );
    final Expression case2 = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.LESS_THAN,
                    COL7,
                    new IntegerLiteral(6)
                ),
                new StringLiteral("Blah")
            )
        ),
        Optional.empty()
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(case1);
    InterpretedExpression interpreter2 = interpreter(case2);

    // Then:
    assertThat(interpreter1.evaluate(make(7, 12)), is("Large"));
    assertThat(interpreter1.evaluate(make(7, 9)), is("Medium"));
    assertThat(interpreter1.evaluate(make(7, 3)), is("Small"));
    assertThat(interpreter1.evaluate(make(7, 1)), is("Tiny"));
    assertThat(interpreter2.evaluate(make(7, 1)), is("Blah"));
    assertThat(interpreter2.evaluate(make(7, 10)), nullValue());
  }

  @Test
  public void shouldEvaluateLikePredicate() {
    // Given:
    final Expression expression1 = new LikePredicate(
        new StringLiteral("catdog"), new StringLiteral("ca%og"), Optional.empty()
    );
    final Expression expression2 = new LikePredicate(
        new StringLiteral("cat%og"), new StringLiteral("cat\\%og"), Optional.empty()
    );
    final Expression expression3 = new LikePredicate(
        new StringLiteral("cat%og"), new StringLiteral("cat\\%og"), Optional.of('\\')
    );


    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);
    InterpretedExpression interpreter3 = interpreter(expression3);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(true));
    assertThat(interpreter2.evaluate(ROW), is(false));
    assertThat(interpreter3.evaluate(ROW), is(true));
  }

  @Test
  public void shouldEvaluateStruct() {
    // Given:
    final Expression expression1 = new CreateStructExpression(
        ImmutableList.of(
            new Field("A", new IntegerLiteral(10)),
            new Field("B", new StringLiteral("abc"))
        )
    );


    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(
        new Struct(SchemaBuilder.struct().optional()
            .field("A", SchemaBuilder.int32().optional().build())
            .field("B", SchemaBuilder.string().optional().build())
            .build())
            .put("A", 10)
            .put("B", "abc")));
  }

  @Test
  public void shouldEvaluateStructDereference() {
    // Given:
    final Expression expression1 = new DereferenceExpression(
        Optional.empty(),
        new CreateStructExpression(
          ImmutableList.of(
              new Field("A", new IntegerLiteral(10)),
              new Field("B", new StringLiteral("abc"))
          )
        ),
        "A"
    );


    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(10));
  }

  @Test
  public void shouldEvaluateSubscriptExpression() {
    // Given:
    final Expression expression1 = new SubscriptExpression(
        new CreateArrayExpression(ImmutableList.of(
            new StringLiteral("1"), new StringLiteral("2"))),
        new IntegerLiteral(1)
    );
    final Expression expression2 = new SubscriptExpression(
        new CreateMapExpression(ImmutableMap.of(
            new StringLiteral("a"), new LongLiteral(123),
            new StringLiteral("b"), new LongLiteral(456))),
        new StringLiteral("a")
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is("1"));
    assertThat(interpreter2.evaluate(ROW), is(123L));
  }

  @Test
  public void shouldEvaluateBetween() {
    // Given:
    final Expression expression1 = new BetweenPredicate(
        new IntegerLiteral(4), new IntegerLiteral(3), new IntegerLiteral(8)
    );
    final Expression expression2 = new BetweenPredicate(
        new IntegerLiteral(0), new IntegerLiteral(3), new IntegerLiteral(8)
    );
    final Expression expression3 = new BetweenPredicate(
        new StringLiteral("b"), new StringLiteral("a"), new StringLiteral("c")
    );
    final Expression expression4 = new BetweenPredicate(
        new StringLiteral("z"), new StringLiteral("a"), new StringLiteral("c")
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);
    InterpretedExpression interpreter3 = interpreter(expression3);
    InterpretedExpression interpreter4 = interpreter(expression4);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(true));
    assertThat(interpreter2.evaluate(ROW), is(false));
    assertThat(interpreter3.evaluate(ROW), is(true));
    assertThat(interpreter4.evaluate(ROW), is(false));
  }

  @Test
  public void shouldEvaluateUnaryArithmetic() {
    // Given:
    final Expression expression1 = new ArithmeticUnaryExpression(
        Optional.empty(), Sign.PLUS, new IntegerLiteral(1)
    );
    final Expression expression2 = new ArithmeticUnaryExpression(
        Optional.empty(), Sign.MINUS, new IntegerLiteral(1)
    );
    final Expression expression3 = new ArithmeticUnaryExpression(
        Optional.empty(), Sign.MINUS, new DecimalLiteral(new BigDecimal("345.5"))
    );
    final Expression expression4 = new ArithmeticUnaryExpression(
        Optional.empty(), Sign.MINUS, new DoubleLiteral(45.5d)
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(expression1);
    InterpretedExpression interpreter2 = interpreter(expression2);
    InterpretedExpression interpreter3 = interpreter(expression3);
    InterpretedExpression interpreter4 = interpreter(expression4);

    // Then:
    assertThat(interpreter1.evaluate(ROW), is(1));
    assertThat(interpreter2.evaluate(ROW), is(-1));
    assertThat(interpreter3.evaluate(ROW), is(new BigDecimal("-345.5")));
    assertThat(interpreter4.evaluate(ROW), is(-45.5d));
  }

  @Test
  public void shouldEvaluateInPredicate() {
    // Given:
    final Expression in1 = new InPredicate(
        COL7,
        new InListExpression(ImmutableList.of(
            new IntegerLiteral(4),
            new IntegerLiteral(6),
            new IntegerLiteral(8)
        ))
    );
    final Expression in2 = new InPredicate(
        COL1,
        new InListExpression(ImmutableList.of(
            new StringLiteral("a"),
            new StringLiteral("b"),
            new StringLiteral("c")
        ))
    );

    // When:
    InterpretedExpression interpreter1 = interpreter(in1);
    InterpretedExpression interpreter2 = interpreter(in2);

    // Then:
    assertThat(interpreter1.evaluate(make(7, 1)), is(false));
    assertThat(interpreter1.evaluate(make(7, 6)), is(true));
    assertThat(interpreter1.evaluate(make(7, 8)), is(true));
    assertThat(interpreter1.evaluate(make(7, 10)), is(false));

    assertThat(interpreter2.evaluate(make(1, "z")), is(false));
    assertThat(interpreter2.evaluate(make(1, "a")), is(true));
    assertThat(interpreter2.evaluate(make(1, "c")), is(true));
  }

  private void givenUdf(
      final String name, final UdfFactory factory, final KsqlScalarFunction function
  ) {
    when(functionRegistry.isAggregate(FunctionName.of(name))).thenReturn(false);
    when(functionRegistry.getUdfFactory(FunctionName.of(name))).thenReturn(factory);
    when(factory.getFunction(anyList())).thenReturn(function);
    when(function.getReturnType(anyList())).thenReturn(SqlTypes.INTEGER);
    final UdfMetadata metadata = mock(UdfMetadata.class);
    when(factory.getMetadata()).thenReturn(metadata);
  }

  private static class AddUdf implements Kudf {

    @Override
    public Object evaluate(Object... args) {
      int a = (Integer) args[0];
      int b = (Integer) args[1];
      return a + b;
    }
  }
}
