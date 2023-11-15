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

package io.confluent.ksql.execution.codegen;

import static io.confluent.ksql.execution.testutil.TestExpressions.ARRAYCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.BYTESCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL0;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL1;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL3;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL7;
import static io.confluent.ksql.execution.testutil.TestExpressions.DATECOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.MAPCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.SCHEMA;
import static io.confluent.ksql.execution.testutil.TestExpressions.TIMECOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.TIMESTAMPCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.literal;
import static io.confluent.ksql.name.SourceName.of;
import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.codegen.CodeGenTestUtil.Evaluator;
import io.confluent.ksql.execution.codegen.helpers.CastEvaluator;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DateLiteral;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.IntervalUnit;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaVariable;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SqlToJavaVisitorTest {

  @Mock
  private FunctionRegistry functionRegistry;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private SqlToJavaVisitor sqlToJavaVisitor;
  private KsqlConfig ksqlConfig;

  @Before
  public void init() {
    final AtomicInteger funCounter = new AtomicInteger();
    final AtomicInteger structCounter = new AtomicInteger();
    ksqlConfig = new KsqlConfig(Collections.emptyMap());
    sqlToJavaVisitor = new SqlToJavaVisitor(
        SCHEMA,
        functionRegistry,
        ref -> ref.text().replace(".", "_"),
        name -> name.text() + "_" + funCounter.getAndIncrement(),
        struct -> "schema" + structCounter.getAndIncrement(),
        ksqlConfig
    );
  }

  @Test
  public void shouldProcessBasicJavaMath() {
    // Given:
    final Expression expression = new ArithmeticBinaryExpression(Operator.ADD, COL0, COL3);

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(((java.lang.Long) arguments.get(\"COL0\")) + ((java.lang.Double) arguments.get(\"COL3\")))"));
  }

  @Test
  public void shouldProcessArrayExpressionCorrectly() {
    // Given:
    final Expression expression = new SubscriptExpression(ARRAYCOL, literal(0));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression,
        equalTo("((Double) (((java.util.List) arguments.get(\"COL4\")) == null ? null : (ArrayAccess.arrayAccess((java.util.List) ((java.util.List) arguments.get(\"COL4\")), ((int) 0)))))")
    );
  }

  @Test
  public void shouldProcessMapExpressionCorrectly() {
    // Given:
    final Expression expression = new SubscriptExpression(MAPCOL, new StringLiteral("key1"));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("((Double) (((java.util.Map) arguments.get(\"COL5\")) == null ? null : ((java.util.Map)((java.util.Map) arguments.get(\"COL5\"))).get(\"key1\")))"));
  }

  @Test
  public void shouldProcessCreateArrayExpressionCorrectly() {
    // Given:
    Expression expression = new CreateArrayExpression(
        ImmutableList.of(
            new SubscriptExpression(MAPCOL, new StringLiteral("key1")),
            new DoubleLiteral(1.0d)
        )
    );

    // When:
    String java = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        java,
        equalTo("((List)new ArrayBuilder(2)" +
                ".add( (new Supplier<Object>() {@Override public Object get() { try {  return ((Double) (((java.util.Map) arguments.get(\"COL5\")) == null ? null : ((java.util.Map)((java.util.Map) arguments.get(\"COL5\"))).get(\"key1\"))); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing array item\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get())" +
                ".add( (new Supplier<Object>() {@Override public Object get() { try {  return 1E0; } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing array item\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get()).build())"));
    }

  @Test
  public void shouldProcessCreateMapExpressionCorrectly() {
    // Given:
    Expression expression = new CreateMapExpression(
        ImmutableMap.of(
            new StringLiteral("foo"),
            new SubscriptExpression(MAPCOL, new StringLiteral("key1")),
            new StringLiteral("bar"),
            new DoubleLiteral(1.0d)
        )
    );

    // When:
    String java = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(java, equalTo("((Map)new MapBuilder(2).put( (new Supplier<Object>() {@Override public Object get() { try {  return \"foo\"; } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing map key\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get(),  (new Supplier<Object>() {@Override public Object get() { try {  return ((Double) (((java.util.Map) arguments.get(\"COL5\")) == null ? null : ((java.util.Map)((java.util.Map) arguments.get(\"COL5\"))).get(\"key1\"))); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing map value\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get()).put( (new Supplier<Object>() {@Override public Object get() { try {  return \"bar\"; } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing map key\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get(),  (new Supplier<Object>() {@Override public Object get() { try {  return 1E0; } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing map value\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get()).build())"));
  }

  @Test
  public void shouldProcessStructExpressionCorrectly() {
    // Given:
    final Expression expression = new CreateStructExpression(
        ImmutableList.of(
            new Field("col1", new StringLiteral("foo")),
            new Field("col2", new SubscriptExpression(MAPCOL, new StringLiteral("key1")))
        )
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression,
        equalTo("((Struct)new Struct(((org.apache.kafka.connect.data.Schema) arguments.get(\"schema0\"))).put(\"col1\", (new Supplier<Object>() {@Override public Object get() { try {  return \"foo\"; } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing struct field\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get()).put(\"col2\", (new Supplier<Object>() {@Override public Object get() { try {  return ((Double) (((java.util.Map) arguments.get(\"COL5\")) == null ? null : ((java.util.Map)((java.util.Map) arguments.get(\"COL5\"))).get(\"key1\"))); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing struct field\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get()))"));
 }

  @Test
  public void shouldProcessStructExpressionWithDereferencesCorrectly() {
    // Given:

    final Expression structExpression = new CreateStructExpression(
        ImmutableList.of(
            new Field("col1", new StringLiteral("foo")),
            new Field("col2", new SubscriptExpression(MAPCOL, new StringLiteral("key1")))
        )
    );
    final Expression expression = new DereferenceExpression(Optional.empty(),
        structExpression,
        "col2"
        );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression,
        equalTo("((Double)(((Struct)new Struct(((org.apache.kafka.connect.data.Schema) arguments.get(\"schema0\"))).put(\"col1\", (new Supplier<Object>() {@Override public Object get() { try {  return \"foo\"; } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing struct field\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get()).put(\"col2\", (new Supplier<Object>() {@Override public Object get() { try {  return ((Double) (((java.util.Map) arguments.get(\"COL5\")) == null ? null : ((java.util.Map)((java.util.Map) arguments.get(\"COL5\"))).get(\"key1\"))); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing struct field\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get())) == null ? null : ((Struct)new Struct(((org.apache.kafka.connect.data.Schema) arguments.get(\"schema0\"))).put(\"col1\", (new Supplier<Object>() {@Override public Object get() { try {  return \"foo\"; } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing struct field\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get()).put(\"col2\", (new Supplier<Object>() {@Override public Object get() { try {  return ((Double) (((java.util.Map) arguments.get(\"COL5\")) == null ? null : ((java.util.Map)((java.util.Map) arguments.get(\"COL5\"))).get(\"key1\"))); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing struct field\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get())).get(\"col2\")))"));
  }

  @Test
  public void shouldPostfixFunctionInstancesWithUniqueId() {
    // Given:
    final UdfFactory ssFactory = mock(UdfFactory.class);
    final KsqlScalarFunction ssFunction = mock(KsqlScalarFunction.class);
    final UdfFactory catFactory = mock(UdfFactory.class);
    final KsqlScalarFunction catFunction = mock(KsqlScalarFunction.class);
    givenUdf("SUBSTRING", ssFactory, ssFunction, SqlTypes.STRING);
    when(ssFunction.parameters())
        .thenReturn(ImmutableList.of(ParamTypes.STRING, ParamTypes.INTEGER, ParamTypes.INTEGER));
    givenUdf("CONCAT", catFactory, catFunction, SqlTypes.STRING);
    when(catFunction.parameters())
        .thenReturn(ImmutableList.of(ParamTypes.STRING, ParamTypes.STRING));
    final FunctionName ssName = FunctionName.of("SUBSTRING");
    final FunctionName catName = FunctionName.of("CONCAT");
    final FunctionCall substring1 = new FunctionCall(
        ssName,
        ImmutableList.of(COL1, new IntegerLiteral(1), new IntegerLiteral(3))
    );
    final FunctionCall substring2 = new FunctionCall(
        ssName,
        ImmutableList.of(COL1, new IntegerLiteral(4), new IntegerLiteral(5))
    );
    final FunctionCall concat = new FunctionCall(
        catName,
        ImmutableList.of(new StringLiteral("-"), substring2)
    );
    final Expression expression = new FunctionCall(
        catName,
        ImmutableList.of(substring1, concat)
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    final String expected = "((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"CONCAT_0\")).evaluate( (new Supplier<Object>() {@Override public Object get() { try {  return ((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"SUBSTRING_1\")).evaluate(((java.lang.String) arguments.get(\"COL1\")), 1, 3)); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing SUBSTRING\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get(),  (new Supplier<Object>() {@Override public Object get() { try {  return ((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"CONCAT_2\")).evaluate(\"-\",  (new Supplier<Object>() {@Override public Object get() { try {  return ((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"SUBSTRING_3\")).evaluate(((java.lang.String) arguments.get(\"COL1\")), 4, 5)); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing SUBSTRING\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get())); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing CONCAT\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return defaultValue; }}}).get()))";

    assertThat(javaExpression, is(expected));
  }

  @Test
  public void shouldImplicitlyCastFunctionCallParameters() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("FOO", udfFactory, udf, SqlTypes.STRING);
    when(udf.parameters()).thenReturn(ImmutableList.of(ParamTypes.DOUBLE, ParamTypes.LONG));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(new DecimalLiteral(new BigDecimal("1.2")), new IntegerLiteral(1))
        )
    );

    // Then:
      final String doubleCast = sqlToJavaVisitor.process(new Cast(
              new DecimalLiteral(new BigDecimal("1.2")),
              new io.confluent.ksql.execution.expression.tree.Type(SqlTypes.DOUBLE)));

      final String longCast = sqlToJavaVisitor.process(new Cast(
              new IntegerLiteral(1),
              new io.confluent.ksql.execution.expression.tree.Type(SqlTypes.BIGINT)));

    assertThat(javaExpression, is(
        "((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"FOO_0\")).evaluate(" +doubleCast + ", " + longCast + "))"
    ));
  }

  @Test
  public void shouldImplicitlyCastFunctionCallParametersVariadic() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("FOO", udfFactory, udf, SqlTypes.STRING);
    when(udf.parameters()).thenReturn(ImmutableList.of(ParamTypes.DOUBLE, ArrayType.of(ParamTypes.LONG)));
    when(udf.isVariadic()).thenReturn(true);

    // When:
    final String javaExpression = sqlToJavaVisitor.process(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(
                new DecimalLiteral(new BigDecimal("1.2")),
                new IntegerLiteral(1),
                new IntegerLiteral(1))
        )
    );

    // Then:
    final String doubleCast = sqlToJavaVisitor.process(new Cast(
            new DecimalLiteral(new BigDecimal("1.2")),
            new io.confluent.ksql.execution.expression.tree.Type(SqlTypes.DOUBLE)));

    final String longCast = sqlToJavaVisitor.process(new Cast(
            new IntegerLiteral(1),
            new io.confluent.ksql.execution.expression.tree.Type(SqlTypes.BIGINT)));

    assertThat(javaExpression, is(
        "((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"FOO_0\")).evaluate(" +doubleCast + ", " + longCast + ", " + longCast + "))"
    ));
  }

  @Test
  public void shouldHandleFunctionCallsWithGenerics() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("FOO", udfFactory, udf, SqlTypes.STRING);
    when(udf.parameters()).thenReturn(ImmutableList.of(GenericType.of("T"), GenericType.of("T")));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(
                new IntegerLiteral(1),
                new IntegerLiteral(1))
        )
    );

    // Then:
    assertThat(javaExpression, is("((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"FOO_0\")).evaluate(1, 1))"));
  }

  @Test
  public void shouldEscapeQuotesInStringLiteral() {
    // Given:
    final Expression expression = new StringLiteral("\"foo\"");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("\"\\\"foo\\\"\""));
  }

  @Test
  public void shouldEscapeQuotesInStringLiteralQuote() {
    // Given:
    final Expression expression = new StringLiteral("\\\"");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("\"\\\\\\\"\""));
  }

  @Test
  public void shouldGenerateCorrectCodeForComparisonWithNegativeNumbers() {
    // Given:
    final Expression expression = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        COL3,
        new DoubleLiteral(-10.0)
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression, equalTo(
            "((((Object)(((java.lang.Double) arguments.get(\"COL3\")))) == null || ((Object)(-1E1)) == null) ? false : (((java.lang.Double) arguments.get(\"COL3\")) > -1E1))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePattern() {
    // Given:
    final Expression expression = new LikePredicate(COL1, new StringLiteral("%foo"), Optional.empty());

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("LikeEvaluator.matches(((java.lang.String) arguments.get(\"COL1\")), \"%foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithEscape() {
    // Given:
    final Expression expression = new LikePredicate(COL1, new StringLiteral("%foo"), Optional.of('!'));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("LikeEvaluator.matches(((java.lang.String) arguments.get(\"COL1\")), \"%foo\", '!')"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithColRef() {
    // Given:
    final Expression expression = new LikePredicate(COL1, COL1, Optional.empty());

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("LikeEvaluator.matches(((java.lang.String) arguments.get(\"COL1\")), ((java.lang.String) arguments.get(\"COL1\")))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatement() {
    // Given:
    final Expression expression = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(10)),
                new StringLiteral("small")
            ),
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(100)),
                new StringLiteral("medium")
            )
        ),
        Optional.of(new StringLiteral("large"))
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // ThenL
    assertThat(
        javaExpression, equalTo(
            "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.copyOf(Arrays.asList( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(10)) == null) ? false : (((java.lang.Integer) arguments.get(\"COL7\")) < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(100)) == null) ? false : (((java.lang.Integer) arguments.get(\"COL7\")) < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }}))), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"large\"; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatementWith13Conditions() {
    // Given:
    final ImmutableList<Integer> numbers = ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    final ImmutableList<String> numberNames = ImmutableList.of("zero", "one", "two", "three", "four", "five",
                                                       "six", "seven", "eight", "nine", "ten",
                                                       "eleven", "twelve");

    final ImmutableList<WhenClause> arg = numbers
            .stream()
            .map(n -> new WhenClause(
            new ComparisonExpression(
                    ComparisonExpression.Type.EQUAL, COL7, new IntegerLiteral(n)),
            new StringLiteral(numberNames.get(n))
    )).collect(ImmutableList.toImmutableList());

    final Expression expression = new SearchedCaseExpression(
            arg,
            Optional.empty()
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // ThenL
    assertThat(
            javaExpression, equalTo(
                    "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.copyOf(Arrays.asList( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(0)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 0) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 0))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"zero\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(1)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 1) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 1))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"one\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(2)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 2) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 2))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"two\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(3)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 3) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 3))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"three\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(4)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 4) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 4))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"four\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(5)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 5) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 5))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"five\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(6)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 6) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 6))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"six\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(7)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 7) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 7))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"seven\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(8)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 8) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 8))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"eight\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(9)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 9) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 9))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"nine\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(10)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 10) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 10))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"ten\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(11)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 11) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 11))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"eleven\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(12)) == null) ? false : ((((java.lang.Integer) arguments.get(\"COL7\")) <= 12) && (((java.lang.Integer) arguments.get(\"COL7\")) >= 12))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"twelve\"; }}))), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return null; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatementWithNoElse() {
    // Given:
    final Expression expression = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(10)),
                new StringLiteral("small")
            ),
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(100)),
                new StringLiteral("medium")
            )
        ),
        Optional.empty()
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // ThenL
    assertThat(
        javaExpression, equalTo(
            "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.copyOf(Arrays.asList( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(10)) == null) ? false : (((java.lang.Integer) arguments.get(\"COL7\")) < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(((java.lang.Integer) arguments.get(\"COL7\")))) == null || ((Object)(100)) == null) ? false : (((java.lang.Integer) arguments.get(\"COL7\")) < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }}))), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return null; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalAdd() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(((java.math.BigDecimal) arguments.get(\"COL8\")).add(((java.math.BigDecimal) arguments.get(\"COL8\")), new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))")
    );
  }

  @Test
  public void shouldGenerateCastLongToDecimalInBinaryExpression() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, containsString("DecimalUtil.cast(((java.lang.Long) arguments.get(\"COL0\")), 19, 0)"));
  }

  @Test
  public void shouldGenerateCastDecimalToDoubleInBinaryExpression() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    final String doubleCast = CastEvaluator.generateCode(
        "((java.math.BigDecimal) arguments.get(\"COL8\"))", SqlTypes.decimal(2, 1), SqlTypes.DOUBLE, ksqlConfig);
    assertThat(java, containsString(doubleCast));
  }

    @Test
    public void shouldGenerateCastExpressionsWhichAreComparable() {
        // Given:
        final Expression cast = new Cast(new StringLiteral("2020-01-01"), new io.confluent.ksql.execution.expression.tree.Type(SqlTypes.DATE));
        final ComparisonExpression exp = new ComparisonExpression(Type.GREATER_THAN_OR_EQUAL, cast, cast);

        // When:
        final String java = sqlToJavaVisitor.process(exp);

        // Then:
        final Evaluator evaluator = CodeGenTestUtil.cookCode(java, Boolean.class);
        evaluator.evaluate();
    }

  @Test
  public void shouldGenerateCorrectCodeForDecimalSubtract() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.SUBTRACT,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(((java.math.BigDecimal) arguments.get(\"COL8\")).subtract(((java.math.BigDecimal) arguments.get(\"COL8\")), new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalMultiply() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.MULTIPLY,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(((java.math.BigDecimal) arguments.get(\"COL8\")).multiply(((java.math.BigDecimal) arguments.get(\"COL8\")), new MathContext(5, RoundingMode.UNNECESSARY)).setScale(2))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDivide() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.DIVIDE,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(((java.math.BigDecimal) arguments.get(\"COL8\")).divide(((java.math.BigDecimal) arguments.get(\"COL8\")), new MathContext(8, RoundingMode.UNNECESSARY)).setScale(6))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalMod() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.MODULUS,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(((java.math.BigDecimal) arguments.get(\"COL8\")).remainder(((java.math.BigDecimal) arguments.get(\"COL8\")), new MathContext(2, RoundingMode.UNNECESSARY)).setScale(1))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.math.BigDecimal) arguments.get(\"COL8\")).compareTo(((java.math.BigDecimal) arguments.get(\"COL9\"))) == 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalGT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.math.BigDecimal) arguments.get(\"COL8\")).compareTo(((java.math.BigDecimal) arguments.get(\"COL9\"))) > 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalGEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.math.BigDecimal) arguments.get(\"COL8\")).compareTo(((java.math.BigDecimal) arguments.get(\"COL9\"))) >= 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalLT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.math.BigDecimal) arguments.get(\"COL8\")).compareTo(((java.math.BigDecimal) arguments.get(\"COL9\"))) < 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalLEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.math.BigDecimal) arguments.get(\"COL8\")).compareTo(((java.math.BigDecimal) arguments.get(\"COL9\"))) <= 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalIsDistinct() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.IS_DISTINCT_FROM,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.math.BigDecimal) arguments.get(\"COL8\")).compareTo(((java.math.BigDecimal) arguments.get(\"COL9\"))) != 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDoubleEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.math.BigDecimal) arguments.get(\"COL8\")).compareTo(BigDecimal.valueOf(((java.lang.Double) arguments.get(\"COL3\")))) == 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDoubleDecimalEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(BigDecimal.valueOf(((java.lang.Double) arguments.get(\"COL3\"))).compareTo(((java.math.BigDecimal) arguments.get(\"COL8\"))) == 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalNegation() {
    // Given:
    final ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
        Optional.empty(),
        Sign.MINUS,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(((java.math.BigDecimal) arguments.get(\"COL8\")).negate(new MathContext(2, RoundingMode.UNNECESSARY)))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalUnaryPlus() {
    // Given:
    final ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
        Optional.empty(),
        Sign.PLUS,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(((java.math.BigDecimal) arguments.get(\"COL8\")).plus(new MathContext(2, RoundingMode.UNNECESSARY)))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForTimeTimeLT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.LESS_THAN,
        TIMECOL,
        TIMECOL
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.sql.Time) arguments.get(\"COL12\")).compareTo(((java.sql.Time) arguments.get(\"COL12\"))) < 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForTimeStringEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.EQUAL,
        TIMECOL,
        new StringLiteral("01:23:45")
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.sql.Time) arguments.get(\"COL12\")).compareTo(SqlTimeTypes.parseTime(\"01:23:45\")) == 0)"));
  }

  @Test
  public void shouldThrowOnTimestampTimeLEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.LESS_THAN_OR_EQUAL,
        TIMESTAMPCOL,
        TIMECOL
    );

    // Then:
    final Exception e = assertThrows(KsqlException.class, () -> sqlToJavaVisitor.process(compExp));
    assertThat(e.getMessage(), is("Unexpected comparison to TIME: TIMESTAMP"));
  }

  @Test
  public void shouldThrowOnTimeDateNEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.NOT_EQUAL,
        TIMECOL,
        DATECOL
    );

    // Then:
    final Exception e = assertThrows(KsqlException.class, () -> sqlToJavaVisitor.process(compExp));
    assertThat(e.getMessage(), is("Unexpected comparison to TIME: DATE"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDateDateLT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.LESS_THAN,
        DATECOL,
        DATECOL
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.sql.Date) arguments.get(\"COL13\")).compareTo(((java.sql.Date) arguments.get(\"COL13\"))) < 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDateStringEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.EQUAL,
        DATECOL,
        new StringLiteral("2021-06-23")
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.sql.Date) arguments.get(\"COL13\")).compareTo(SqlTimeTypes.parseDate(\"2021-06-23\")) == 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForTimestampTimestampLT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.LESS_THAN,
        TIMESTAMPCOL,
        TIMESTAMPCOL
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.sql.Timestamp) arguments.get(\"COL10\")).compareTo(((java.sql.Timestamp) arguments.get(\"COL10\"))) < 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForTimestampStringEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.EQUAL,
        TIMESTAMPCOL,
        new StringLiteral("2020-01-01T00:00:00")
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.sql.Timestamp) arguments.get(\"COL10\")).compareTo(SqlTimeTypes.parseTimestamp(\"2020-01-01T00:00:00\")) == 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForTimestampStringGEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.GREATER_THAN_OR_EQUAL,
        new StringLiteral("2020-01-01T00:00:00"),
        TIMESTAMPCOL
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(SqlTimeTypes.parseTimestamp(\"2020-01-01T00:00:00\").compareTo(((java.sql.Timestamp) arguments.get(\"COL10\"))) >= 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForTimestampDateGT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.GREATER_THAN,
        TIMESTAMPCOL,
        DATECOL
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.sql.Timestamp) arguments.get(\"COL10\")).compareTo(((java.sql.Date) arguments.get(\"COL13\"))) > 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForBytesBytesGT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        Type.GREATER_THAN,
        BYTESCOL,
        BYTESCOL
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(((java.nio.ByteBuffer) arguments.get(\"COL14\")).compareTo(((java.nio.ByteBuffer) arguments.get(\"COL14\"))) > 0)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForIntervalUnit() {
    // Given:
    final IntervalUnit intervalUnit = new IntervalUnit(TimeUnit.DAYS);

    // When:
    final String java = sqlToJavaVisitor.process(intervalUnit);

    // Then:
    assertThat(java, containsString("TimeUnit.DAYS"));
  }

  @Test
  public void shouldGenerateCorrectCodeForTime() {
    // Given:
    final TimeLiteral time = new TimeLiteral(new Time(185000));

    // When:
    final String java = sqlToJavaVisitor.process(time);

    // Then:
    assertThat(java, is("00:03:05"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDate() {
    // Given:
    final DateLiteral time = new DateLiteral(new Date(864000000));

    // When:
    final String java = sqlToJavaVisitor.process(time);

    // Then:
    assertThat(java, is("1970-01-11"));
  }

  @Test
  public void shouldThrowOnQualifiedColumnReference() {
    // Given:
    final Expression expression = new QualifiedColumnReferenceExp(
        of("foo"),
        ColumnName.of("bar")
    );

    // When:
    assertThrows(
        UnsupportedOperationException.class,
        () -> sqlToJavaVisitor.process(expression)
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForInPredicate() {
    // Given:
    final Expression expression = new InPredicate(
        COL0,
        new InListExpression(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)))
    );

    // When:
    final String java = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(java, is("InListEvaluator.matches(((java.lang.Long) arguments.get(\"COL0\")),1L,2L)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLambdaExpression() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("ABS", udfFactory, udf, SqlTypes.STRING);
    givenUdf("TRANSFORM", udfFactory, udf, SqlTypes.STRING);
    when(udf.parameters()).
        thenReturn(ImmutableList.of(
            ArrayType.of(ParamTypes.DOUBLE),
            LambdaType.of(ImmutableList.of(
                ParamTypes.DOUBLE),
                ParamTypes.DOUBLE))
        );

    final Expression expression = new FunctionCall (
        FunctionName.of("TRANSFORM"),
        ImmutableList.of(
            ARRAYCOL,
            new LambdaFunctionCall(
                ImmutableList.of("x"),
                (new FunctionCall(FunctionName.of("ABS"), ImmutableList.of(new LambdaVariable("X")))))));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then
    assertThat(
        javaExpression, equalTo(
            "((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"TRANSFORM_0\")).evaluate(((java.util.List) arguments.get(\"COL4\")), new Function() {\n" +
            " @Override\n" +
            " public Object apply(Object arg1) {\n" +
            "   final Double x = (Double) arg1;\n" +
            "   return ((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"ABS_1\")).evaluate(X));\n" +
            " }\n" +
            "}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLambdaExpressionWithTwoArguments() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("REDUCE", udfFactory, udf, SqlTypes.STRING);
    when(udf.parameters()).
        thenReturn(ImmutableList.of(
            ArrayType.of(ParamTypes.DOUBLE),
            ParamTypes.DOUBLE,
            LambdaType.of(
                ImmutableList.of(ParamTypes.DOUBLE, ParamTypes.DOUBLE),
                ParamTypes.DOUBLE))
        );

    final Expression expression = new FunctionCall (
        FunctionName.of("REDUCE"),
        ImmutableList.of(
            ARRAYCOL,
            COL3,
            new LambdaFunctionCall(
                ImmutableList.of("X", "S"),
                (new ArithmeticBinaryExpression(
                    Operator.ADD,
                    new LambdaVariable("X"),
                    new LambdaVariable("S")))
            )));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then
    assertThat(
        javaExpression, equalTo(
            "((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"REDUCE_0\")).evaluate(((java.util.List) arguments.get(\"COL4\")), ((java.lang.Double) arguments.get(\"COL3\")), new BiFunction() {\n" +
                " @Override\n" +
                " public Object apply(Object arg1, Object arg2) {\n" +
                "   final Double X = (Double) arg1;\n" +
                "   final Double S = (Double) arg2;\n" +
                "   return (X + S);\n" +
                " }\n" +
                "}))"));
  }
 
  @Test
  public void shouldGenerateCorrectCodeForFunctionWithMultipleLambdas() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("function", udfFactory, udf, SqlTypes.STRING);
    when(udf.parameters()).
        thenReturn(ImmutableList.of(
            ArrayType.of(ParamTypes.DOUBLE),
            ParamTypes.STRING,
            LambdaType.of(
                ImmutableList.of(ParamTypes.DOUBLE, ParamTypes.STRING),
                ParamTypes.DOUBLE),
            LambdaType.of(
                ImmutableList.of(ParamTypes.DOUBLE, ParamTypes.STRING),
                ParamTypes.STRING)
        ));

    final Expression expression = new FunctionCall (
        FunctionName.of("function"),
        ImmutableList.of(
            ARRAYCOL,
            COL1,
            new LambdaFunctionCall(
                ImmutableList.of("X", "S"),
                new ArithmeticBinaryExpression(
                    Operator.ADD,
                    new LambdaVariable("X"),
                    new LambdaVariable("X"))
            ),
            new LambdaFunctionCall(
                ImmutableList.of("X", "S"),
                new SearchedCaseExpression(
                    ImmutableList.of(
                        new WhenClause(
                            new ComparisonExpression(
                                ComparisonExpression.Type.LESS_THAN, new LambdaVariable("X"), new IntegerLiteral(10)),
                            new StringLiteral("test")
                        ),
                        new WhenClause(
                            new ComparisonExpression(
                                ComparisonExpression.Type.LESS_THAN, new LambdaVariable("X"), new IntegerLiteral(100)),
                            new StringLiteral("test2")
                        )
                    ),
                    Optional.of(new LambdaVariable("S"))
                )
            )));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then
    assertThat(
        javaExpression, equalTo("((String) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"function_0\")).evaluate(((java.util.List) arguments.get(\"COL4\")), ((java.lang.String) arguments.get(\"COL1\")), new BiFunction() {\n"
            + " @Override\n"
            + " public Object apply(Object arg1, Object arg2) {\n"
            + "   final Double X = (Double) arg1;\n"
            + "   final String S = (String) arg2;\n"
            + "   return (X + X);\n"
            + " }\n"
            + "}, new BiFunction() {\n"
            + " @Override\n"
            + " public Object apply(Object arg1, Object arg2) {\n"
            + "   final Double X = (Double) arg1;\n"
            + "   final String S = (String) arg2;\n"
            + "   return ((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.copyOf(Arrays.asList( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(X)) == null || ((Object)(10)) == null) ? false : (X < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"test\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(X)) == null || ((Object)(100)) == null) ? false : (X < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"test2\"; }}))), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return S; }}));\n"
            + " }\n"
            + "}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForNestedLambdas() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("nested", udfFactory, udf, SqlTypes.DOUBLE);
    when(udf.parameters()).
        thenReturn(ImmutableList.of(
            ArrayType.of(ParamTypes.DOUBLE),
            ParamTypes.DOUBLE,
            LambdaType.of(
                ImmutableList.of(ParamTypes.DOUBLE, ParamTypes.INTEGER),
                ParamTypes.INTEGER))
        );

    final Expression expression = new ArithmeticBinaryExpression(
        Operator.ADD,
        new FunctionCall(
            FunctionName.of("nested"),
            ImmutableList.of(
                ARRAYCOL,
                new IntegerLiteral(0),
                new LambdaFunctionCall(
                    ImmutableList.of("A", "B"),
                    new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new FunctionCall(
                            FunctionName.of("nested"),
                            ImmutableList.of(
                                ARRAYCOL,
                                new IntegerLiteral(0),
                                new LambdaFunctionCall(
                                    ImmutableList.of("Q", "V"),
                                    new ArithmeticBinaryExpression(
                                        Operator.ADD,
                                        new LambdaVariable("Q"),
                                        new LambdaVariable("V"))
                                ))),
                        new LambdaVariable("B"))
                ))),
        new IntegerLiteral(5)
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then
    assertThat(
        javaExpression, equalTo(
            "(((Double) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"nested_0\")).evaluate(((java.util.List) arguments.get(\"COL4\")),  (new Supplier<java.lang.Double>() {@Override public java.lang.Double get() { try {  return ((Double)NullSafe.apply(0,new Function() {\n" +
            " @Override\n" +
            " public Object apply(Object arg1) {\n" +
            "   final Integer val = (Integer) arg1;\n" +
            "   return val.doubleValue();\n" +
            " }\n" +
            "})); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing DOUBLE\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return (java.lang.Double) defaultValue; }}}).get(), new BiFunction() {\n" +
            " @Override\n" +
            " public Object apply(Object arg1, Object arg2) {\n" +
            "   final Double A = (Double) arg1;\n" +
            "   final Integer B = (Integer) arg2;\n" +
            "   return (((Double) ((io.confluent.ksql.function.udf.Kudf) arguments.get(\"nested_1\")).evaluate(((java.util.List) arguments.get(\"COL4\")),  (new Supplier<java.lang.Double>() {@Override public java.lang.Double get() { try {  return ((Double)NullSafe.apply(0,new Function() {\n" +
            " @Override\n" +
            " public Object apply(Object arg1) {\n" +
            "   final Integer val = (Integer) arg1;\n" +
            "   return val.doubleValue();\n" +
            " }\n" +
            "})); } catch (Exception e) {  logger.error(RecordProcessingError.recordProcessingError(    \"Error processing DOUBLE\",    e instanceof InvocationTargetException? e.getCause() : e,    row));  return (java.lang.Double) defaultValue; }}}).get(), new BiFunction() {\n" +
            " @Override\n" +
            " public Object apply(Object arg1, Object arg2) {\n" +
            "   final Double Q = (Double) arg1;\n" +
            "   final Integer V = (Integer) arg2;\n" +
            "   return (Q + V);\n" +
            " }\n" +
            "})) + B);\n" +
            " }\n" +
            "})) + 5)"));
  }

  @Test
  public void shouldThrowOnSimpleCase() {
    // Given:
    final Expression expression = new SimpleCaseExpression(
        COL0,
        ImmutableList.of(new WhenClause(new IntegerLiteral(10), new StringLiteral("ten"))),
        empty()
    );

    // When:
    assertThrows(
        UnsupportedOperationException.class,
        () -> sqlToJavaVisitor.process(expression)
    );
  }

  @Test
  public void shouldProcessTimeLiteral() {
    assertThat(sqlToJavaVisitor.process(new TimeLiteral(new Time(1000))), is("00:00:01"));
  }

  @Test
  public void shouldHandleManyArguments() {
    // Given:
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    for (int i = 0; i < 500; i++) {
      schemaBuilder.valueColumn(ColumnName.of("COL" + i), SqlTypes.STRING);
    }
    final LogicalSchema schema = schemaBuilder.build();

    final AtomicInteger funCounter = new AtomicInteger();
    final AtomicInteger structCounter = new AtomicInteger();
    final SqlToJavaVisitor sqlToJavaVisitor = new SqlToJavaVisitor(
            schema,
            functionRegistry,
            ref -> ref.text().replace(".", "_"),
            name -> name.text() + "_" + funCounter.getAndIncrement(),
            struct -> "schema" + structCounter.getAndIncrement(),
            ksqlConfig
    );

    final List<Expression> expressions = new ArrayList<>();
    for (int i = 0; i < 500 ; i++) {
      expressions.add(new UnqualifiedColumnReferenceExp(ColumnName.of("COL" + i)));
    }
    final Expression expression = new CreateArrayExpression(expressions);

    // When:
    final String java = sqlToJavaVisitor.process(expression);

    // Then:
    final Map<String, Object> arguments = IntStream.range(0, 500).boxed().collect(Collectors.toMap(i -> "COL" + i, String::valueOf));
    final Evaluator evaluator = CodeGenTestUtil.cookCode(java, List.class);
    evaluator.evaluate(arguments);
  }

  private void givenUdf(
      final String name,
      final UdfFactory factory,
      final KsqlScalarFunction function,
      final SqlType returnType
  ) {
    when(functionRegistry.isAggregate(FunctionName.of(name))).thenReturn(false);
    when(functionRegistry.getUdfFactory(FunctionName.of(name))).thenReturn(factory);
    when(factory.getFunction(anyList())).thenReturn(function);
    when(function.getReturnType(anyList())).thenReturn(returnType);
    final UdfMetadata metadata = mock(UdfMetadata.class);
    when(factory.getMetadata()).thenReturn(metadata);
  }
}
