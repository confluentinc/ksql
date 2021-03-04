package io.confluent.ksql.function;

import static io.confluent.ksql.function.KsqlScalarFunction.INTERNAL_PATH;
import static io.confluent.ksql.function.types.ArrayType.of;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.INTEGER;
import static java.lang.System.lineSeparator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlIntervalUnit;
import io.confluent.ksql.schema.ksql.types.SqlLambdaResolved;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class UdfIndexTest {

  private static final ParamType STRING_VARARGS = ArrayType.of(ParamTypes.STRING);
  private static final ParamType STRING = ParamTypes.STRING;
  private static final ParamType DECIMAL = ParamTypes.DECIMAL;
  private static final ParamType INT = ParamTypes.INTEGER;
  private static final ParamType LONG = ParamTypes.LONG;
  private static final ParamType DOUBLE = ParamTypes.DOUBLE;
  private static final ParamType STRUCT1 = StructType.builder().field("a", STRING).build();
  private static final ParamType STRUCT2 = StructType.builder().field("b", INT).build();
  private static final ParamType MAP1 = MapType.of(STRING, STRING);
  private static final ParamType MAP2 = MapType.of(INT, INT);
  private static final ParamType INTERVALUNIT = ParamTypes.INTERVALUNIT;
  private static final ParamType LAMBDA_KEY_FUNCTION = LambdaType.of(ImmutableList.of(GenericType.of("A")), GenericType.of("C"));
  private static final ParamType LAMBDA_VALUE_FUNCTION = LambdaType.of(ImmutableList.of(GenericType.of("B")), GenericType.of("D"));
  private static final ParamType LAMBDA_BI_FUNCTION = LambdaType.of(ImmutableList.of(GenericType.of("A"), GenericType.of("B")), GenericType.of("C"));
  private static final ParamType LAMBDA_BI_FUNCTION_STRING = LambdaType.of(ImmutableList.of(STRING, STRING), GenericType.of("A"));

  private static final ParamType GENERIC_LIST = ArrayType.of(GenericType.of("T"));
  private static final ParamType GENERIC_MAP = MapType.of(GenericType.of("A"), GenericType.of("B"));

  private static final SqlType ARRAY_ARG = SqlTypes.array(INTEGER);
  private static final SqlType MAP1_ARG = SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING);
  private static final SqlType MAP2_ARG = SqlTypes.map(SqlTypes.STRING, INTEGER);
  private static final SqlType DECIMAL1_ARG = SqlTypes.decimal(4, 2);

  private static final SqlType STRUCT1_ARG = SqlTypes.struct().field("a", SqlTypes.STRING).build();
  private static final SqlType STRUCT2_ARG = SqlTypes.struct().field("b", SqlTypes.INTEGER).build();

  private static final FunctionName EXPECTED = FunctionName.of("expected");
  private static final FunctionName OTHER = FunctionName.of("other");
  private static final FunctionName FIRST_FUNC = FunctionName.of("first_func");
  private static final FunctionName SECOND_FUNC = FunctionName.of("second_func");

  private UdfIndex<KsqlScalarFunction> udfIndex;

  @Before
  public void setUp() {
    udfIndex = new UdfIndex<>("name", true);
  }

  @Test
  public void shouldThrowOnAddIfFunctionWithSameNameAndParamsExists() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, DOUBLE)
    );

    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udfIndex.addFunction(function(EXPECTED, false, DOUBLE))
    );

    // Then:
    assertThat(e.getMessage(), startsWith("Can't add function `expected` with parameters [DOUBLE] "
        + "as a function with the same name and parameter types already exists"));
  }

  @Test
  public void shouldFindNoArgs() {
    // Given:
    givenFunctions(
        function(EXPECTED, false)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of());

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArg() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArgWithCast() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, LONG)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.INTEGER)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindPreferredOneArgWithCast() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, false, LONG),
        function(EXPECTED, false, INT),
        function(OTHER, false, DOUBLE)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.INTEGER)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoDifferentArgs() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
        .getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.INTEGER)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoSameArgs() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING, STRING)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
        .getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArgConflict() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING),
        function(OTHER, false, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoArgSameFirstConflict() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING, STRING),
        function(OTHER, false, STRING, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectStruct() {
    // Given:
    givenFunctions(
        function(OTHER, false, STRUCT2),
        function(EXPECTED, false, STRUCT1)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(STRUCT1_ARG)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectMap() {
    // Given:
    givenFunctions(
        function(OTHER, false, MAP2),
        function(EXPECTED, false, MAP1)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(MAP1_ARG)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseIntervalUnit() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, INTERVALUNIT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(
        SqlIntervalUnit.INSTANCE)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectLambdaFunction() {
    // Given:
    givenFunctions(
        function(FIRST_FUNC, false, GENERIC_MAP, LAMBDA_KEY_FUNCTION)
    );
    givenFunctions(
        function(SECOND_FUNC, false, GENERIC_MAP, LAMBDA_VALUE_FUNCTION)
    );

    // When:
    final KsqlScalarFunction first_fun = udfIndex.getFunction(
        ImmutableList.of(
            SqlArgument.of(MAP2_ARG),
            SqlArgument.of(
                SqlLambdaResolved.of(
                    ImmutableList.of(SqlTypes.STRING),
                    SqlTypes.STRING))));

    final KsqlScalarFunction second_fun = udfIndex.getFunction(
        ImmutableList.of(
            SqlArgument.of(MAP2_ARG),
            SqlArgument.of(
                SqlLambdaResolved.of(
                    ImmutableList.of(INTEGER),
                    INTEGER))));

    // Then:
    assertThat(first_fun.name(), equalTo(FIRST_FUNC));
    assertThat(second_fun.name(), equalTo(SECOND_FUNC));
  }

  @Test
  public void shouldChooseCorrectLambdaBiFunction() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, GENERIC_MAP, LAMBDA_BI_FUNCTION)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(
        ImmutableList.of(
            SqlArgument.of(MAP1_ARG),
            SqlArgument.of(
                SqlLambdaResolved.of(
                    ImmutableList.of(SqlTypes.STRING, SqlTypes.STRING),
                    INTEGER))));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectLambdaForTypeSpecificCollections() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, MAP1, LAMBDA_BI_FUNCTION_STRING)
    );

    // When:
    final KsqlScalarFunction fun1 = udfIndex.getFunction(
        ImmutableList.of(
            SqlArgument.of(MAP1_ARG),
            SqlArgument.of(
                SqlLambdaResolved.of(
                    ImmutableList.of(SqlTypes.STRING, SqlTypes.STRING),
                    SqlTypes.BOOLEAN))));

    final KsqlScalarFunction fun2 = udfIndex.getFunction(
        ImmutableList.of(
            SqlArgument.of(MAP1_ARG),
            SqlArgument.of(
                SqlLambdaResolved.of(
                    ImmutableList.of(SqlTypes.STRING, SqlTypes.STRING),
                    INTEGER))));

    final Exception e = assertThrows(
        Exception.class,
        () -> udfIndex.getFunction(
            ImmutableList.of(
                SqlArgument.of(MAP1_ARG),
                SqlArgument.of(
                    SqlLambdaResolved.of(
                        ImmutableList.of(SqlTypes.BOOLEAN, INTEGER),
                        INTEGER))))
    );

    // Then:
    assertThat(fun1.name(), equalTo(EXPECTED));
    assertThat(fun2.name(), equalTo(EXPECTED));
    assertThat(e.getMessage(), containsString("does not accept parameters (" +
        "MAP<STRING, STRING>, LAMBDA (BOOLEAN, INTEGER) => INTEGER)."));
    assertThat(e.getMessage(), containsString("Valid alternatives are:"
        + lineSeparator()
        + "expected(MAP<VARCHAR, VARCHAR>, LAMBDA (VARCHAR, VARCHAR) => A)"));
  }

  @Test
  public void shouldThrowOnInvalidLambdaMapping() {
    // Given:
    givenFunctions(
        function(OTHER, false, GENERIC_MAP, LAMBDA_BI_FUNCTION)
    );

    // When:
    final Exception e1 = assertThrows(
        Exception.class,
        () -> udfIndex.getFunction(
            ImmutableList.of(
                SqlArgument.of(MAP1_ARG),
                SqlArgument.of(
                    SqlLambdaResolved.of(
                        ImmutableList.of(SqlTypes.BOOLEAN, SqlTypes.STRING),
                        INTEGER))))
    );

    final Exception e2 = assertThrows(
        Exception.class,
        () -> udfIndex.getFunction(
            ImmutableList.of(
                SqlArgument.of(MAP1_ARG),
                SqlArgument.of(
                    SqlLambdaResolved.of(
                        ImmutableList.of(SqlTypes.STRING, SqlTypes.STRING, SqlTypes.STRING),
                        INTEGER)
                )))
    );

    // Then:
    assertThat(e1.getMessage(), containsString("does not accept parameters (" +
        "MAP<STRING, STRING>, LAMBDA (BOOLEAN, STRING) => INTEGER)."));
    assertThat(e1.getMessage(), containsString("Valid alternatives are:"
        + lineSeparator()
        + "other(MAP<A, B>, LAMBDA (A, B) => C)"));

    assertThat(e2.getMessage(), containsString("does not accept parameters (" +
        "MAP<STRING, STRING>, LAMBDA (STRING, STRING, STRING) => INTEGER)."));
    assertThat(e2.getMessage(), containsString("Valid alternatives are:"
        + lineSeparator()
        + "other(MAP<A, B>, LAMBDA (A, B) => C)"));
  }

  @Test
  public void shouldAllowAnyDecimal() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, DECIMAL)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(DECIMAL1_ARG)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsEmpty() {
    // Given:
    givenFunctions(
        function(EXPECTED, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of());

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsOne() {
    // Given:
    givenFunctions(
        function(EXPECTED, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsTwo() {
    // Given:
    givenFunctions(
        function(EXPECTED, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
        .getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithStruct() {
    // Given:
    givenFunctions(
        function(EXPECTED, true, ArrayType.of(STRUCT1))
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(STRUCT1_ARG), SqlArgument.of(STRUCT1_ARG)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithList() {
    // Given:
    givenFunctions(
        function(EXPECTED, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
        .getFunction(ImmutableList.of(SqlArgument.of(SqlArray.of(SqlTypes.STRING))));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgs() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverMultipleVarArgs() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS),
        function("two", true, STRING, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseVarArgsIfSpecificDoesntMatch() {
    // Given:
    givenFunctions(
        function(OTHER, false, STRING),
        function(EXPECTED, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
        .getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithNullValues() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithPartialNullValues() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING, STRING)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(null, SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseFirstAddedWithNullValues() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING),
        function(OTHER, false, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithNullValues() {
    // Given:
    givenFunctions(
        function(EXPECTED, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(new SqlArgument[]{null}));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithSomeNullValues() {
    // Given:
    givenFunctions(
        function(EXPECTED, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(null, SqlArgument.of(SqlTypes.STRING), null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValues() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfDifferingSchemas() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING, INT),
        function(OTHER, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(SqlArgument.of(null, null), SqlArgument.of(null, null)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfSameSchemas() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING, STRING),
        function(OTHER, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(SqlArgument.of(null, null), SqlArgument.of(null, null)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfPartialNulls() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING, INT),
        function(OTHER, true, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(SqlArgument.of(SqlTypes.STRING), null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectlyInComplicatedTopology() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, STRING, INT, STRING, INT), function(OTHER, true, STRING_VARARGS),
        function("two", true, STRING, STRING_VARARGS),
        function("three", true, STRING, INT, STRING_VARARGS),
        function("four", true, STRING, INT, STRING, INT, STRING_VARARGS),
        function("five", true, INT, INT, STRING, INT, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(INTEGER), null, SqlArgument.of(INTEGER)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindGenericMethodWithIntParam() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, GENERIC_LIST)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(SqlArgument.of(SqlArray.of(SqlTypes.INTEGER))));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindGenericMethodWithStringParam() {
    // Given:
    givenFunctions(
        function(EXPECTED, false, GENERIC_LIST)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(SqlArgument.of(SqlArray.of(SqlTypes.STRING))));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchGenericMethodWithMultipleIdenticalGenerics() {
    // Given:
    final GenericType generic = GenericType.of("A");
    givenFunctions(
        function(EXPECTED, false, generic, generic)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(INTEGER), SqlArgument.of(INTEGER)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchGenericMethodWithMultipleGenerics() {
    // Given:
    final GenericType genericA = GenericType.of("A");
    final GenericType genericB = GenericType.of("B");
    givenFunctions(
        function(EXPECTED, false, genericA, genericB)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(INTEGER), SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchNestedGenericMethodWithMultipleGenerics() {
    // Given:
    final ArrayType generic = ArrayType.of(GenericType.of("A"));
    givenFunctions(
        function(EXPECTED, false, generic, generic)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlArray.of(SqlTypes.INTEGER)), SqlArgument.of(SqlArray.of(SqlTypes.INTEGER))));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldNotMatchIfParamLengthDiffers() {
    // Given:
    givenFunctions(
        function(OTHER, false, STRING)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING)))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
        + "(STRING, STRING)"));
  }

  @Test
  public void shouldNotMatchIfNoneFound() {
    // Given:
    givenFunctions(
        function(OTHER, false, STRING)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(ImmutableList.of(SqlArgument.of(INTEGER)))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
        + "(INTEGER)"));
  }

  @Test
  public void shouldNotMatchIfNoneFoundWithNull() {
    // Given:
    givenFunctions(
        function(OTHER, false, STRING, INT)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(Arrays.asList(SqlArgument.of(INTEGER), null))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
        + "(INTEGER, null)"));
  }

  @Test
  public void shouldNotChooseSpecificWhenTrickyVarArgLoop() {
    // Given:
    givenFunctions(
        function(OTHER, false, STRING, INT),
        function("two", true, STRING_VARARGS)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(INTEGER), SqlArgument.of(SqlTypes.STRING)))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
        + "(STRING, INTEGER, STRING)"));
  }

  @Test
  public void shouldNotMatchWhenNullTypeInArgsIfParamLengthDiffers() {
    // Given:
    givenFunctions(
        function(OTHER, false, STRING)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(Arrays.asList(SqlArgument.of(SqlTypes.STRING), null))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
        + "(STRING, null)"));
  }

  @Test
  public void shouldNotMatchVarargDifferentStructs() {
    // Given:
    givenFunctions(
        function(OTHER, true, ArrayType.of(STRUCT1))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(ImmutableList.of(SqlArgument.of(STRUCT1_ARG), SqlArgument.of(STRUCT2_ARG)))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
        + "(STRUCT<a STRING>, STRUCT<b INTEGER>)"));
  }

  @Test
  public void shouldNotMatchGenericMethodWithAlreadyReservedTypes() {
    // Given:
    final GenericType generic = GenericType.of("A");
    givenFunctions(
        function(EXPECTED, false, generic, generic)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(ImmutableList.of(SqlArgument.of(INTEGER), SqlArgument.of(SqlTypes.STRING)))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
        + "(INTEGER, STRING)"));
  }

  @Test
  public void shouldNotMatchNestedGenericMethodWithAlreadyReservedTypes() {
    // Given:
    final ArrayType generic = ArrayType.of(GenericType.of("A"));
    givenFunctions(
        function(EXPECTED, false, generic, generic)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex
            .getFunction(ImmutableList.of(SqlArgument.of(SqlArray.of(SqlTypes.INTEGER)), SqlArgument.of(SqlArray.of(SqlTypes.STRING))))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
        + "(ARRAY<INTEGER>, ARRAY<STRING>)"));
  }

  @Test
  public void shouldIncludeAvailableSignaturesIfNotMatchFound() {
    // Given:
    final ArrayType generic = of(GenericType.of("A"));
    givenFunctions(
        function(OTHER, true, false, STRING, INT),
        function(OTHER, true, STRING_VARARGS),
        function(OTHER, false, generic)
    );

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(INTEGER), SqlArgument.of(SqlTypes.STRING)))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Valid alternatives are:"
        + lineSeparator()
        + "other(VARCHAR...)"
        + lineSeparator()
        + "other(ARRAY<A>)"
        + lineSeparator()
        + "other(VARCHAR paramName, INT paramName)"));
  }

  @Test
  public void shouldSupportMatchAndImplicitCastEnabled() {
    // Given:
    givenFunctions(
            function(EXPECTED, false, DOUBLE)
    );

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(INTEGER)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldThrowIfNoExactMatchAndImplicitCastDisabled() {
    // Given:
    udfIndex = new UdfIndex<>("name", false);
    givenFunctions(
            function(OTHER, false, DOUBLE)
    );

    // When:
    final Exception e = assertThrows(
            KsqlException.class,
            () -> udfIndex.getFunction(ImmutableList.of(SqlArgument.of(INTEGER)))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
            + "(INTEGER)"));
  }

  private void givenFunctions(final KsqlScalarFunction... functions) {
    Arrays.stream(functions).forEach(udfIndex::addFunction);
  }

  private static KsqlScalarFunction function(
      final String name,
      final boolean isVarArgs,
      final ParamType... args
  ) {
    return function(FunctionName.of(name), isVarArgs, args);
  }

  private static KsqlScalarFunction function(
      final FunctionName name,
      final boolean isVarArgs,
      final ParamType... args
  ) {
    return function(name, false, isVarArgs, args);
  }

  private static KsqlScalarFunction function(
      final FunctionName name,
      final boolean namedParams,
      final boolean isVarArgs,
      final ParamType... args
  ) {
    final Function<KsqlConfig, Kudf> udfFactory = ksqlConfig -> {
      try {
        return new MyUdf();
      } catch (final Exception e) {
        throw new KsqlException("Failed to create instance of kudfClass "
            + MyUdf.class + " for function " + name, e);
      }
    };

    final List<ParameterInfo> paramInfos = Arrays.stream(args)
        .map(type -> new ParameterInfo(namedParams ? "paramName" : "", type, "", false))
        .collect(Collectors.toList());

    return KsqlScalarFunction.create(
        (params, arguments) -> SqlTypes.STRING,
        STRING,
        paramInfos,
        name,
        MyUdf.class,
        udfFactory,
        "",
        INTERNAL_PATH,
        isVarArgs);
  }

  private static final class MyUdf implements Kudf {

    @Override
    public Object evaluate(final Object... args) {
      return null;
    }
  }


}