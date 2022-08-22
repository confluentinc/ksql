package io.confluent.ksql.function;

import static io.confluent.ksql.function.KsqlScalarFunction.INTERNAL_PATH;
import static io.confluent.ksql.function.types.ArrayType.of;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
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
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class UdfIndexTest {

  private static final ParamType STRING_VARARGS = ArrayType.of(ParamTypes.STRING);

  private static final ParamType INT_VARARGS = ArrayType.of(ParamTypes.INTEGER);

  private static final ParamType OBJ_VARARGS = ArrayType.of(ParamTypes.ANY);
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
        function(EXPECTED, -1, DOUBLE)
    );

    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udfIndex.addFunction(function(EXPECTED, -1, DOUBLE))
    );

    // Then:
    assertThat(e.getMessage(), startsWith("Can't add function `expected` with parameters [DOUBLE] "
        + "as a function with the same name and parameter types already exists"));
  }

  @Test
  public void shouldThrowOnAddFunctionSameParamsExceptOneVariadic() {
    // Given:
    givenFunctions(
            function(EXPECTED, -1, STRING_VARARGS)
    );

    // When:
    final Exception e = assertThrows(
            KsqlFunctionException.class,
            () -> udfIndex.addFunction(function(EXPECTED, 0, STRING_VARARGS))
    );

    // Then:
    assertThat(e.getMessage(), startsWith("Can't add function `expected` with parameters"
            + " [ARRAY<VARCHAR>] as a function with the same name and parameter types already"
            + " exists"));
  }

  @Test
  public void shouldFindNoArgs() {
    // Given:
    givenFunctions(
        function(EXPECTED, -1)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of());

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNoArgsVariadic() {
    // Given:
    givenFunctions(
            function(EXPECTED, 0, STRING_VARARGS)
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
        function(EXPECTED, -1, STRING)
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
        function(EXPECTED, -1, LONG)};
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
        function(OTHER, -1, LONG),
        function(EXPECTED, -1, INT),
        function(OTHER, -1, DOUBLE)
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
        function(EXPECTED, -1, STRING, INT)
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
        function(EXPECTED, -1, STRING, STRING)
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
        function(EXPECTED, -1, STRING),
        function(OTHER, -1, INT)
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
        function(EXPECTED, -1, STRING, STRING),
        function(OTHER, -1, STRING, INT)
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
        function(OTHER, -1, STRUCT2),
        function(EXPECTED, -1, STRUCT1)
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
        function(OTHER, -1, MAP2),
        function(EXPECTED, -1, MAP1)
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
        function(EXPECTED, -1, INTERVALUNIT)
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
        function(FIRST_FUNC, -1, GENERIC_MAP, LAMBDA_KEY_FUNCTION)
    );
    givenFunctions(
        function(SECOND_FUNC, -1, GENERIC_MAP, LAMBDA_VALUE_FUNCTION)
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
        function(EXPECTED, -1, GENERIC_MAP, LAMBDA_BI_FUNCTION)
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
        function(EXPECTED, -1, MAP1, LAMBDA_BI_FUNCTION_STRING)
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
        function(OTHER, -1, GENERIC_MAP, LAMBDA_BI_FUNCTION)
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
        function(EXPECTED, -1, DECIMAL)
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
        function(EXPECTED, 0, STRING_VARARGS)
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
        function(EXPECTED, 0, STRING_VARARGS)
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
        function(EXPECTED, 0, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
        .getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsManyEven() {
    // Given:
    givenFunctions(
            function(EXPECTED, 2, INT, INT, STRING_VARARGS, STRING, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
            .getFunction(ImmutableList.of(
                    SqlArgument.of(SqlTypes.INTEGER), SqlArgument.of(SqlTypes.INTEGER),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.INTEGER)
            ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsManyOdd() {
    // Given:
    givenFunctions(
            function(EXPECTED, 2, INT, INT, STRING_VARARGS, STRING, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
            .getFunction(ImmutableList.of(
                    SqlArgument.of(SqlTypes.INTEGER), SqlArgument.of(SqlTypes.INTEGER),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
                    SqlArgument.of(SqlTypes.INTEGER)
            ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithStruct() {
    // Given:
    givenFunctions(
        function(EXPECTED, 0, ArrayType.of(STRUCT1))
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
        function(EXPECTED, 0, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
        .getFunction(ImmutableList.of(SqlArgument.of(SqlArray.of(SqlTypes.STRING))));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithList() {
    // Given:
    givenFunctions(
            function(EXPECTED, -1, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex
            .getFunction(ImmutableList.of(SqlArgument.of(SqlArray.of(SqlTypes.STRING))));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldNotCreateListWhenNotVariadic() {
    // Given:
    givenFunctions(
            function(FIRST_FUNC, -1, ArrayType.of(ParamTypes.INTEGER))
    );

    // When:
    final Exception e = assertThrows(
            KsqlException.class,
            () -> udfIndex.getFunction(ImmutableList.of(
                    SqlArgument.of(SqlTypes.INTEGER)
            ))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters "
            + "(INTEGER)"));
  }

  @Test
  public void shouldChooseSpecificOverOnlyVarArgs() {
    // Given:
    givenFunctions(
        function(EXPECTED, -1, STRING),
        function(OTHER, 0, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgsAtEnd() {
    // Given:
    givenFunctions(
            function(EXPECTED, -1, INT, INT, STRING),
            function(OTHER, 2, INT, INT, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.INTEGER), SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.STRING)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgsInMiddle() {
    // Given:
    givenFunctions(
            function(EXPECTED, -1, INT, INT, STRING, STRING, STRING, STRING, INT),
            function(OTHER, 2, INT, INT, STRING_VARARGS, STRING, STRING, STRING, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.INTEGER), SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.INTEGER)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgsAtBeginning() {
    // Given:
    givenFunctions(
            function(EXPECTED, -1, STRING, STRING, STRING, STRING, INT),
            function(OTHER, 0, STRING_VARARGS, STRING, STRING, STRING, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.INTEGER)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverOnlyVarArgsReversedInsertionOrder() {
    // Given:
    givenFunctions(
            function(OTHER, 0, STRING_VARARGS),
            function(EXPECTED, -1, STRING, STRING, STRING, STRING)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgsAtEndReversedInsertionOrder() {
    // Given:
    givenFunctions(
            function(OTHER, 2, INT, INT, STRING_VARARGS),
            function(EXPECTED, -1, INT, INT, STRING, STRING, STRING, STRING)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.INTEGER), SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgsInMiddleReversedInsertionOrder() {
    // Given:
    givenFunctions(
            function(OTHER, 2, INT, INT, STRING_VARARGS, INT),
            function(EXPECTED, -1, INT, INT, STRING, STRING, STRING, STRING, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.INTEGER), SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.INTEGER)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgsAtBeginningReversedInsertionOrder() {
    // Given:
    givenFunctions(
            function(OTHER, 0, STRING_VARARGS, INT),
            function(EXPECTED, -1, STRING, STRING, STRING, STRING, INT)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.INTEGER)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverMultipleVarArgs() {
    // Given:
    givenFunctions(
        function(EXPECTED, -1, STRING),
        function(OTHER, 0, STRING_VARARGS),
        function("two", 1, STRING, STRING_VARARGS)
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
        function(OTHER, -1, STRING),
        function(EXPECTED, 0, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseVarArgsIfSpecificDoesntMatchMultipleArgs() {
    // Given:
    givenFunctions(
            function(OTHER, -1, STRING, STRING, STRING, STRING),
            function(EXPECTED, 0, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.STRING), SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.STRING)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithNullValues() {
    // Given:
    givenFunctions(
        function(EXPECTED, -1, STRING)
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
        function(EXPECTED, -1, STRING, STRING)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(null, SqlArgument.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithSomeNullValues() {
    // Given:
    givenFunctions(
        function(EXPECTED, 0, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(null, SqlArgument.of(SqlTypes.STRING), null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindMiddleVarargWithSomeNullValues() {
    // Given:
    givenFunctions(
            function(EXPECTED, 1, INT, STRING_VARARGS, STRING)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(
            SqlArgument.of(SqlTypes.INTEGER), null, SqlArgument.of(SqlTypes.STRING)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValues() {
    // Given:
    givenFunctions(
        function(EXPECTED, -1, STRING),
        function(OTHER, 0, STRING_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseMidVarargWithNullValues() {
    // Given:
    givenFunctions(
            function(OTHER, -1, INT, STRING),
            function(EXPECTED, 1, INT, STRING_VARARGS, STRING)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(
            SqlArgument.of(SqlTypes.INTEGER), null, SqlArgument.of(SqlTypes.STRING)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfDifferingSchemas() {
    // Given:
    givenFunctions(
        function(EXPECTED, -1, STRING, INT),
        function(OTHER, 0, STRING_VARARGS)
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
        function(EXPECTED, -1, STRING, STRING),
        function(OTHER, 0, STRING_VARARGS)
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
        function(EXPECTED, -1, STRING, INT),
        function(OTHER, 0, STRING_VARARGS)
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
        function(EXPECTED, -1, STRING, INT, STRING, INT), function(OTHER, 0, STRING_VARARGS),
        function("two", 1, STRING, STRING_VARARGS),
        function("three", 2, STRING, INT, STRING_VARARGS),
        function("four", 4, STRING, INT, STRING, INT, STRING_VARARGS),
        function("five", 4, INT, INT, STRING, INT, STRING_VARARGS)
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
        function(EXPECTED, -1, GENERIC_LIST)
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
        function(EXPECTED, -1, GENERIC_LIST)
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
        function(EXPECTED, -1, generic, generic)
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
        function(EXPECTED, -1, genericA, genericB)
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
        function(EXPECTED, -1, generic, generic)
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
        function(EXPECTED, -1, STRING)
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
        function(EXPECTED, -1, STRING)
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
        function(EXPECTED, -1, STRING, INT)
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
        function(EXPECTED, -1, STRING, INT),
        function("two", 0, STRING_VARARGS)
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
        function(EXPECTED, -1, STRING)
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
        function(OTHER, 0, ArrayType.of(STRUCT1))
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
        function(EXPECTED, -1, generic, generic)
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
        function(EXPECTED, -1, generic, generic)
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
        function(OTHER, true, -1, STRING, INT),
        function(OTHER, 0, STRING_VARARGS),
        function(OTHER, -1, generic)
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
            function(EXPECTED, -1, DOUBLE)
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
            function(EXPECTED, -1, DOUBLE)
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
  public void shouldThrowOnAmbiguousImplicitCastWithoutGenerics() {
    // Given:
    givenFunctions(
        function(FIRST_FUNC, -1, LONG, LONG),
        function(SECOND_FUNC, -1, DOUBLE, DOUBLE)
    );

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
        () -> udfIndex
            .getFunction(ImmutableList.of(SqlArgument.of(INTEGER), SqlArgument.of(BIGINT))));

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' cannot be resolved due " +
        "to ambiguous method parameters "
        + "(INTEGER, BIGINT)"));
  }

  @Test
  public void shouldFindFewerGenerics() {
    // Given:
    givenFunctions(
        function(EXPECTED, -1, INT, GenericType.of("A"), INT),
        function(OTHER, -1, INT, GenericType.of("A"), GenericType.of("B"))
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList
        .of(SqlArgument.of(INTEGER), SqlArgument.of(INTEGER), SqlArgument.of(INTEGER)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindFewerGenericsWithEarlierVariadic() {
    // Given:
    givenFunctions(
            function(EXPECTED, 0, INT_VARARGS, GenericType.of("A"), INT, INT),
            function(OTHER, 3, INT, GenericType.of("A"), GenericType.of("B"), INT_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(INTEGER), SqlArgument.of(INTEGER),
            SqlArgument.of(INTEGER), SqlArgument.of(INTEGER)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindFewerGenericsWithoutObjVariadic() {
    // Given:
    givenFunctions(
            function(EXPECTED, 3, INT, GenericType.of("A"), INT, INT_VARARGS),
            function(OTHER, 3, INT, GenericType.of("B"), INT, OBJ_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(INTEGER), SqlArgument.of(INTEGER),
            SqlArgument.of(INTEGER), SqlArgument.of(INTEGER)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindPreferGenericVariadicToObjVariadic() {
    // Given:
    givenFunctions(
            function(EXPECTED, 3, INT, GenericType.of("A"), INT, ArrayType.of(GenericType.of("C"))),
            function(OTHER, 3, INT, GenericType.of("B"), INT, OBJ_VARARGS)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(INTEGER), SqlArgument.of(INTEGER),
            SqlArgument.of(INTEGER), SqlArgument.of(INTEGER)
    ));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldThrowOnAmbiguousImplicitCastWithGenerics() {
    // Given:
    givenFunctions(
        function(FIRST_FUNC, -1, LONG, GenericType.of("A"), GenericType.of("B")),
        function(SECOND_FUNC, -1, DOUBLE, GenericType.of("A"), GenericType.of("B"))
    );

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
        () -> udfIndex
            .getFunction(ImmutableList
                .of(SqlArgument.of(INTEGER), SqlArgument.of(INTEGER), SqlArgument.of(INTEGER))));

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' cannot be resolved due " +
        "to ambiguous method parameters "
        + "(INTEGER, INTEGER, INTEGER)"));
  }

  @Test
  public void shouldThrowOnAmbiguousImplicitCastWithGenericsAndVariadics() {
    // Given:
    givenFunctions(
            function(FIRST_FUNC, 3, LONG, GenericType.of("A"), GenericType.of("B"), INT_VARARGS),
            function(SECOND_FUNC, 3, DOUBLE, GenericType.of("A"), GenericType.of("B"), INT_VARARGS)
    );

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
            () -> udfIndex.getFunction(ImmutableList.of(
                    SqlArgument.of(INTEGER), SqlArgument.of(INTEGER),
                    SqlArgument.of(INTEGER), SqlArgument.of(INTEGER))
            ));

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' cannot be resolved due "
            + "to ambiguous method parameters "
            + "(INTEGER, INTEGER, INTEGER, INTEGER)"));
  }

  @Test
  public void shouldChooseSpecificWhenTwoVariadicsAndSpecificMatch() {
    // Given:
    givenFunctions(
            function(OTHER, 1, LONG, INT_VARARGS, STRING, DOUBLE),
            function(EXPECTED, -1, LONG, INT, STRING, DOUBLE),
            function(OTHER, 2, LONG, INT, STRING_VARARGS, DOUBLE)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.BIGINT),
            SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.DOUBLE))
    );

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseLaterVariadicWhenTwoVariadicsMatch() {
    // Given:
    givenFunctions(
            function(OTHER, 1, LONG, INT_VARARGS, STRING, DOUBLE),
            function(EXPECTED, 2, LONG, INT, STRING_VARARGS, DOUBLE)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.BIGINT),
            SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.DOUBLE))
    );

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseLaterVariadicWhenTwoVariadicsMatchDiffBranches() {
    // Given:
    givenFunctions(
            function(OTHER, 1, GenericType.of("A"), INT_VARARGS, STRING, DOUBLE),
            function(EXPECTED, 2, GenericType.of("B"), INT, STRING_VARARGS, DOUBLE)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.BIGINT),
            SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.DOUBLE))
    );

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseLaterVariadicWhenTwoVariadicsMatchReversedInsertionOrder() {
    // Given:
    givenFunctions(
            function(EXPECTED, 2, LONG, INT, STRING_VARARGS, DOUBLE),
            function(OTHER, 1, LONG, INT_VARARGS, STRING, DOUBLE)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.BIGINT),
            SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.DOUBLE))
    );

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseLaterVariadicWhenTwoObjVariadicsMatch() {
    // Given:
    givenFunctions(
            function(OTHER, 1, GenericType.of("A"), OBJ_VARARGS, STRING, DOUBLE),
            function(EXPECTED, 2, GenericType.of("B"), INT, OBJ_VARARGS, DOUBLE)
    );

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(
            SqlArgument.of(SqlTypes.BIGINT),
            SqlArgument.of(SqlTypes.INTEGER),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.DOUBLE))
    );

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  private void givenFunctions(final KsqlScalarFunction... functions) {
    Arrays.stream(functions).forEach(udfIndex::addFunction);
  }

  private static KsqlScalarFunction function(
      final String name,
      final int variadicIndex,
      final ParamType... args
  ) {
    return function(FunctionName.of(name), variadicIndex, args);
  }

  private static KsqlScalarFunction function(
      final FunctionName name,
      final int variadicIndex,
      final ParamType... args
  ) {
    return function(name, false, variadicIndex, args);
  }

  private static KsqlScalarFunction function(
      final FunctionName name,
      final boolean namedParams,
      final int variadicIndex,
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

    final List<ParameterInfo> paramInfos = IntStream.range(0, args.length)
            .mapToObj(index -> new ParameterInfo(
                    namedParams ? "paramName" : "", 
                    args[index], 
                    "", 
                    index == variadicIndex
            )).collect(Collectors.toList());

    return KsqlScalarFunction.create(
        (params, arguments) -> SqlTypes.STRING,
        STRING,
        paramInfos,
        name,
        MyUdf.class,
        udfFactory,
        "",
        INTERNAL_PATH,
        variadicIndex >= 0);
  }

  private static final class MyUdf implements Kudf {

    @Override
    public Object evaluate(final Object... args) {
      return null;
    }
  }


}