package io.confluent.ksql.function;

import static io.confluent.ksql.function.KsqlFunction.INTERNAL_PATH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

  private static final Schema GENERIC_LIST = GenericsUtil.array("T").build();
  private static final Schema STRING_LIST = SchemaBuilder.array(STRING).build();
  private static final Schema INT_LIST = SchemaBuilder.array(INT).build();

  private static final String EXPECTED = "expected";
  private static final String OTHER = "other";

  private UdfIndex<KsqlFunction> udfIndex;

  @Before
  public void setUp() {
    udfIndex = new UdfIndex<KsqlFunction>("name");
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldFindNoArgs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of());

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArg() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoDifferentArgs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, INT));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoSameArgs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArgConflict() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING),
        function(OTHER, false, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoArgSameFirstConflict() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING, STRING),
        function(OTHER, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectStruct() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRUCT2),
        function(EXPECTED, false, STRUCT1)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRUCT1));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectMap() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, MAP2),
        function(EXPECTED, false, MAP1)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(MAP1));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectPermutedStruct() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(OTHER, false, STRUCT3_PERMUTE),
        function(EXPECTED, false, STRUCT3)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRUCT3));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsEmpty() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of());

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsOne() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsTwo() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithStruct() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, true, SchemaBuilder.array(STRUCT1).build())};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRUCT1, STRUCT1));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithList() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING_VARARGS));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverVarArgs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseSpecificOverMultipleVarArgs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS),
        function("two", true, STRING, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseVarArgsIfSpecificDoesntMatch() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING),
        function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(STRING, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithNullValues() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithPartialNullValues() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(null, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseFirstAddedWithNullValues() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING),
        function(OTHER, false, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithNullValues() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{null}));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithSomeNullValues() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(null, STRING, null));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValues() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Collections.singletonList(null));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfDifferingSchemas() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING, INT),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{null, null}));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfSameSchemas() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING, STRING),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{null, null}));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseNonVarargWithNullValuesOfPartialNulls() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING, INT),
        function(OTHER, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING, null));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldChooseCorrectlyInComplicatedTopology() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, STRING, INT, STRING, INT), function(OTHER, true, STRING_VARARGS),
        function("two", true, STRING, STRING_VARARGS),
        function("three", true, STRING, INT, STRING_VARARGS),
        function("four", true, STRING, INT, STRING, INT, STRING_VARARGS),
        function("five", true, INT, INT, STRING, INT, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING, INT, null, INT));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindGenericMethodWithIntParam() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, GENERIC_LIST)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Collections.singletonList(INT_LIST));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindGenericMethodWithStringParam() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, GENERIC_LIST)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Collections.singletonList(STRING_LIST));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchGenericMethodWithMultipleIdenticalGenerics() {
    // Given:
    final Schema generic = GenericsUtil.generic("A").build();
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(INT, INT));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchGenericMethodWithMultipleGenerics() {
    // Given:
    final Schema genericA = GenericsUtil.generic("A").build();
    final Schema genericB = GenericsUtil.generic("B").build();
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, genericA, genericB)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(INT, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldMatchNestedGenericMethodWithMultipleGenerics() {
    // Given:
    final Schema generic = GenericsUtil.array("A").build();
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(ImmutableList.of(INT_LIST, INT_LIST));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldNotMatchIfParamLengthDiffers() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[VARCHAR, VARCHAR]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(STRING, STRING));
  }

  @Test
  public void shouldNotMatchIfNoneFound() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[INT]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(INT));

  }

  @Test
  public void shouldNotMatchIfNullAndPrimitive() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(OTHER, false, Schema.INT32_SCHEMA)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[null]"));

    // When:
    udfIndex.getFunction(Collections.singletonList(null));

  }

  @Test
  public void shouldNotMatchIfNullAndPrimitiveVararg() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(OTHER, true, SchemaBuilder.array(Schema.INT32_SCHEMA))};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[null]"));

    // When:
    udfIndex.getFunction(Collections.singletonList(null));

  }

  @Test
  public void shouldNotMatchIfNoneFoundWithNull() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[INT, null]"));

    // When:
    udfIndex.getFunction(Arrays.asList(INT, null));

  }

  @Test
  public void shouldNotChooseSpecificWhenTrickyVarArgLoop() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING, INT),
        function("two", true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[VARCHAR, INT, VARCHAR]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(STRING, INT, STRING));

  }

  @Test
  public void shouldNotMatchWhenNullTypeInArgsIfParamLengthDiffers() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[VARCHAR, null]"));

    // When:
    udfIndex.getFunction(Arrays.asList(STRING, null));

  }

  @Test
  public void shouldNotMatchVarargDifferentStructs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(OTHER, true, SchemaBuilder.array(STRUCT1).build())};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[STRUCT<a VARCHAR>, STRUCT<b INT>]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(STRUCT1, STRUCT2));
  }

  @Test
  public void shouldNotMatchPermutedStructs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(OTHER, false, STRUCT3)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[STRUCT<d INT, c INT>]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(STRUCT3_PERMUTE));
  }

  @Test
  public void shouldNotMatchGenericMethodWithAlreadyReservedTypes() {
    // Given:
    final Schema generic = GenericsUtil.generic("A").build();
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[INT, VARCHAR]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(INT, STRING));
  }

  @Test
  public void shouldNotMatchNestedGenericMethodWithAlreadyReservedTypes() {
    // Given:
    final Schema generic = GenericsUtil.array("A").build();
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(EXPECTED, false, generic, generic)
    };
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(is("Function 'name' does not accept parameters of types:"
        + "[ARRAY<INT>, ARRAY<VARCHAR>]"));

    // When:
    udfIndex.getFunction(ImmutableList.of(INT_LIST, STRING_LIST));
  }


  private static KsqlFunction function(
      final String name,
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

    return KsqlFunction.create(
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