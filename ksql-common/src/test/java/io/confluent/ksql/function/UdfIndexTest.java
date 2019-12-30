package io.confluent.ksql.function;

import static io.confluent.ksql.function.KsqlScalarFunction.INTERNAL_PATH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.types.SqlArray;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UdfIndexTest {

  private static final ParamType STRING_VARARGS = ArrayType.of(ParamTypes.STRING);
  private static final ParamType STRING = ParamTypes.STRING;
  private static final ParamType DECIMAL = ParamTypes.DECIMAL;
  private static final ParamType INT = ParamTypes.INTEGER;
  private static final ParamType STRUCT1 = StructType.builder().field("a", STRING).build();
  private static final ParamType STRUCT2 = StructType.builder().field("b", INT).build();
  private static final ParamType MAP1 = MapType.of(STRING);
  private static final ParamType MAP2 = MapType.of(INT);

  private static final ParamType GENERIC_LIST = ArrayType.of(GenericType.of("T"));

  private static final SqlType MAP1_ARG = SqlTypes.map(SqlTypes.STRING);
  private static final SqlType DECIMAL1_ARG = SqlTypes.decimal(4, 2);

  private static final SqlType STRUCT1_ARG = SqlTypes.struct().field("a", SqlTypes.STRING).build();
  private static final SqlType STRUCT2_ARG = SqlTypes.struct().field("b", SqlTypes.INTEGER).build();

  private static final FunctionName EXPECTED = FunctionName.of("expected");
  private static final FunctionName OTHER = FunctionName.of("other");

  private UdfIndex<KsqlScalarFunction> udfIndex;

  @Before
  public void setUp() {
    udfIndex = new UdfIndex<KsqlScalarFunction>("name");
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldFindNoArgs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(EXPECTED, false)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of());

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArg() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoDifferentArgs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING, SqlTypes.INTEGER));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoSameArgs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING, SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArgConflict() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING),
        function(OTHER, false, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoArgSameFirstConflict() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, STRING),
        function(OTHER, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING, SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectStruct() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRUCT2),
        function(EXPECTED, false, STRUCT1)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRUCT1_ARG));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectMap() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, MAP2),
        function(EXPECTED, false, MAP1)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(MAP1_ARG));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldAllowAnyDecimal() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, DECIMAL)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(DECIMAL1_ARG));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsEmpty() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of());

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsOne() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsTwo() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING, SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithStruct() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, ArrayType.of(STRUCT1))};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRUCT1_ARG, STRUCT1_ARG));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithList() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArray.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverMultipleVarArgs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS),
        function("two", true, STRING, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseVarArgsIfSpecificDoesntMatch() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRING),
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING, SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithNullValues() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithPartialNullValues() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(null, SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseFirstAddedWithNullValues() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING),
        function(OTHER, false, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithNullValues() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(new SqlType[]{null}));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithSomeNullValues() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(null, SqlTypes.STRING, null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValues() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfDifferingSchemas() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, INT),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(new SqlType[]{null, null}));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfSameSchemas() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, STRING),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(new SqlType[]{null, null}));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfPartialNulls() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, INT),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(SqlTypes.STRING, null));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectlyInComplicatedTopology() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, INT, STRING, INT), function(OTHER, true, STRING_VARARGS),
        function("two", true, STRING, STRING_VARARGS),
        function("three", true, STRING, INT, STRING_VARARGS),
        function("four", true, STRING, INT, STRING, INT, STRING_VARARGS),
        function("five", true, INT, INT, STRING, INT, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(SqlTypes.STRING, SqlTypes.INTEGER, null, SqlTypes.INTEGER));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindGenericMethodWithIntParam() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, GENERIC_LIST)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(SqlArray.of(SqlTypes.INTEGER)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindGenericMethodWithStringParam() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, GENERIC_LIST)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(SqlArray.of(SqlTypes.STRING)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchGenericMethodWithMultipleIdenticalGenerics() {
    // Given:
    final GenericType generic = GenericType.of("A");
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.INTEGER, SqlTypes.INTEGER));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchGenericMethodWithMultipleGenerics() {
    // Given:
    final GenericType genericA = GenericType.of("A");
    final GenericType genericB = GenericType.of("B");
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, genericA, genericB)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlTypes.INTEGER, SqlTypes.STRING));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchNestedGenericMethodWithMultipleGenerics() {
    // Given:
    final ArrayType generic = ArrayType.of(GenericType.of("A"));
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(SqlArray.of(SqlTypes.INTEGER), SqlArray.of(SqlTypes.INTEGER)));

    // Then:
    assertThat(fun.name(), equalTo(EXPECTED));
  }

  @Test
  public void shouldNotMatchIfParamLengthDiffers() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[STRING, STRING]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING, SqlTypes.STRING));
  }

  @Test
  public void shouldNotMatchIfNoneFound() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[INTEGER]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(SqlTypes.INTEGER));

  }

  @Test
  public void shouldNotMatchIfNoneFoundWithNull() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[INTEGER, null]"));

    // When:
    udfIndex.getFunction(Arrays.asList(SqlTypes.INTEGER, null));

  }

  @Test
  public void shouldNotChooseSpecificWhenTrickyVarArgLoop() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, false, STRING, INT),
        function("two", true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[STRING, INTEGER, STRING]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(SqlTypes.STRING, SqlTypes.INTEGER, SqlTypes.STRING));

  }

  @Test
  public void shouldNotMatchWhenNullTypeInArgsIfParamLengthDiffers() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[STRING, null]"));

    // When:
    udfIndex.getFunction(Arrays.asList(SqlTypes.STRING, null));

  }

  @Test
  public void shouldNotMatchVarargDifferentStructs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, true, ArrayType.of(STRUCT1))};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[STRUCT<a STRING>, STRUCT<b INTEGER>]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(STRUCT1_ARG, STRUCT2_ARG));
  }

  @Test
  public void shouldNotMatchGenericMethodWithAlreadyReservedTypes() {
    // Given:
    final GenericType generic = GenericType.of("A");
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[INTEGER, STRING]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(SqlTypes.INTEGER, SqlTypes.STRING));
  }

  @Test
  public void shouldNotMatchNestedGenericMethodWithAlreadyReservedTypes() {
    // Given:
    final ArrayType generic = ArrayType.of(GenericType.of("A"));
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[ARRAY<INTEGER>, ARRAY<STRING>]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(SqlArray.of(SqlTypes.INTEGER), SqlArray.of(SqlTypes.STRING)));
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
    final Function<KsqlConfig, Kudf> udfFactory = ksqlConfig -> {
      try {
        return new MyUdf();
      } catch (final Exception e) {
        throw new KsqlException("Failed to create instance of kudfClass "
            + MyUdf.class + " for function " + name, e);
      }
    };

    final List<ParameterInfo> paramInfos = Arrays.stream(args)
        .map(type -> new ParameterInfo("", type, "", false))
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