package io.confluent.ksql.function;

import static com.google.common.collect.ImmutableList.of;
import static io.confluent.ksql.function.GenericsUtil.generic;
import static io.confluent.ksql.function.KsqlScalarFunction.INTERNAL_PATH;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.array;
import static org.apache.kafka.connect.data.SchemaBuilder.bytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

public class UdfIndexTest {

  private static final Schema STRING_VARARGS = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA);
  private static final Schema STRING = Schema.OPTIONAL_STRING_SCHEMA;
  private static final Schema INT = Schema.OPTIONAL_INT32_SCHEMA;
  private static final Schema STRUCT1 = SchemaBuilder.struct().field("a", STRING).build();
  private static final Schema STRUCT2 = SchemaBuilder.struct().field("b", INT).build();
  private static final Schema STRUCT3 = SchemaBuilder.struct().field("c", INT).field("d", INT).build();
  private static final Schema STRUCT3_PERMUTE = SchemaBuilder.struct().field("d", INT).field("c", INT).build();
  private static final Schema MAP1 = SchemaBuilder.map(STRING, STRING).build();
  private static final Schema MAP2 = SchemaBuilder.map(STRING, INT).build();
  private static final Schema DECIMAL1 = DecimalUtil.builder(2, 1).build();
  private static final Schema DECIMAL2 = DecimalUtil.builder(3, 1).build();

  private static final Schema GENERIC_LIST = GenericsUtil.array("T").build();
  private static final Schema STRING_LIST = SchemaBuilder.array(STRING).build();
  private static final Schema INT_LIST = SchemaBuilder.array(INT).build();

  private static final FunctionName EXPECTED = FunctionName.of("expected");
  private static final FunctionName OTHER = FunctionName.of("other");

  private UdfIndex<KsqlScalarFunction> udfIndex;

  @Before
  public void setUp() {
    udfIndex = new UdfIndex<KsqlScalarFunction>("name");
  }

  @Test
  public void shouldFindNoArgs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(EXPECTED, false)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of());

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArg() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoDifferentArgs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, INT));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoSameArgs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArgConflict() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING),
        function(OTHER, false, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoArgSameFirstConflict() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, STRING),
        function(OTHER, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectStruct() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRUCT2),
        function(EXPECTED, false, STRUCT1)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRUCT1));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectMap() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, MAP2),
        function(EXPECTED, false, MAP1)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(MAP1));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectDecimal() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, DECIMAL1)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(DECIMAL1));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldAllowAnyDecimal() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, DECIMAL1)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(DECIMAL2));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectPermutedStruct() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, false, STRUCT3_PERMUTE),
        function(EXPECTED, false, STRUCT3)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRUCT3));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
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
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsOne() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsTwo() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithStruct() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, SchemaBuilder.array(STRUCT1).build())};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRUCT1, STRUCT1));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithList() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING_VARARGS));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
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
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseVarArgsIfSpecificDoesntMatch() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRING),
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
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
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithPartialNullValues() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(null, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
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
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithNullValues() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{null}));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithSomeNullValues() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(null, STRING, null));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
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
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfDifferingSchemas() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, INT),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{null, null}));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfSameSchemas() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, STRING),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{null, null}));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfPartialNulls() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, STRING, INT),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(STRING, null));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
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
    final KsqlScalarFunction fun = udfIndex.getFunction(Arrays.asList(STRING, INT, null, INT));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindGenericMethodWithIntParam() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, GENERIC_LIST)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(INT_LIST));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindGenericMethodWithStringParam() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, GENERIC_LIST)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(Collections.singletonList(STRING_LIST));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchGenericMethodWithMultipleIdenticalGenerics() {
    // Given:
    final Schema generic = GenericsUtil.generic("A").build();
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(INT, INT));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchGenericMethodWithMultipleGenerics() {
    // Given:
    final Schema genericA = GenericsUtil.generic("A").build();
    final Schema genericB = GenericsUtil.generic("B").build();
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, genericA, genericB)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(INT, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchNestedGenericMethodWithMultipleGenerics() {
    // Given:
    final Schema generic = GenericsUtil.array("A").build();
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlScalarFunction fun = udfIndex.getFunction(ImmutableList.of(INT_LIST, INT_LIST));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldNotMatchIfParamLengthDiffers() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRING)};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(of(STRING, STRING))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept "
        + "parameters of types:[VARCHAR, VARCHAR]"));
  }

  @Test
  public void shouldNotMatchIfNoneFound() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRING)};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(of(INT))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept " +
        "parameters of types:[INT]"));

  }

  @Test
  public void shouldNotMatchIfNullAndPrimitive() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, false, INT32_SCHEMA)};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(singletonList(null))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept " +
        "parameters of types:[null]"));

  }

  @Test
  public void shouldNotMatchIfNullAndPrimitiveVararg() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, true, array(INT32_SCHEMA))};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(singletonList(null))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters of types:"
        + "[null]"));

  }

  @Test
  public void shouldNotMatchIfNoneFoundWithNull() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, false, STRING, INT)};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(asList(INT, null))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters of types:"
        + "[INT, null]"));

  }

  @Test
  public void shouldNotChooseSpecificWhenTrickyVarArgLoop() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, false, STRING, INT),
        function("two", true, STRING_VARARGS)};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(of(STRING, INT, STRING))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters of types:"
        + "[VARCHAR, INT, VARCHAR]"));
  }

  @Test
  public void shouldNotMatchWhenNullTypeInArgsIfParamLengthDiffers() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{function(OTHER, false, STRING)};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(asList(STRING, null))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters of types:"
        + "[VARCHAR, null]"));
  }

  @Test
  public void shouldNotMatchVarargDifferentStructs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, true, array(STRUCT1).build())};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(of(STRUCT1, STRUCT2))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters of types:"
        + "[STRUCT<a VARCHAR>, STRUCT<b INT>]"));
  }

  @Test
  public void shouldNotMatchPermutedStructs() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(OTHER, false, STRUCT3)};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(of(STRUCT3_PERMUTE))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters of types:"
        + "[STRUCT<d INT, c INT>]"));
  }

  @Test
  public void shouldNotMatchGenericMethodWithAlreadyReservedTypes() {
    // Given:
    final Schema generic = generic("A").build();
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(of(INT, STRING))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters of types:"
        + "[INT, VARCHAR]"));
  }

  @Test
  public void shouldNotMatchNestedGenericMethodWithAlreadyReservedTypes() {
    // Given:
    final Schema generic = GenericsUtil.array("A").build();
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(of(INT_LIST, STRING_LIST))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters of types:"
        + "[ARRAY<INT>, ARRAY<VARCHAR>]"));
  }

  @Test
  public void shouldNotFindArbitraryBytesTypes() {
    // Given:
    final KsqlScalarFunction[] functions = new KsqlScalarFunction[]{
        function(EXPECTED, false, DECIMAL1)};
    stream(functions).forEach(udfIndex::addFunction);

    // When
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udfIndex.getFunction(of(bytes().build()))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Function 'name' does not accept parameters of types:"
        + "[BYTES]"));
  }

  private static KsqlScalarFunction function(
      final String name,
      final boolean isVarArgs,
      final Schema... args
  ) {
    return function(FunctionName.of(name), isVarArgs, args);
  }

  private static KsqlScalarFunction function(
      final FunctionName name,
      final boolean isVarArgs,
      final Schema... args
  ) {
    final Function<KsqlConfig, Kudf> udfFactory = ksqlConfig -> {
      try {
        return new MyUdf();
      } catch (final Exception e) {
        throw new KsqlException("Failed to create instance of kudfClass "
            + MyUdf.class + " for function " + name, e);
      }
    };

    return KsqlScalarFunction.create(
        ignored -> Schema.OPTIONAL_STRING_SCHEMA,
        Schema.OPTIONAL_STRING_SCHEMA,
        Arrays.asList(args),
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