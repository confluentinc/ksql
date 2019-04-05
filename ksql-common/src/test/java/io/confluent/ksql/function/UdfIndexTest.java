package io.confluent.ksql.function;

import static io.confluent.ksql.function.KsqlFunction.INTERNAL_PATH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

public class UdfIndexTest {

  private static final Schema STRING_VARARGS = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA);
  private static final Schema STRING = Schema.OPTIONAL_STRING_SCHEMA;
  private static final Schema INT = Schema.OPTIONAL_INT32_SCHEMA;
  private static final Schema STRUCT1 = SchemaBuilder.struct().field("a", STRING).build();
  private static final Schema STRUCT2 = SchemaBuilder.struct().field("b", INT).build();
  private static final Schema MAP1 = SchemaBuilder.map(STRING, STRING).build();
  private static final Schema MAP2 = SchemaBuilder.map(STRING, INT).build();

  private static final String EXPECTED = "expected";
  private static final String OTHER = "other";

  private UdfIndex udfIndex;

  @Before
  public void setUp() {
    udfIndex = new UdfIndex("name");
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldFindNoArgs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{}));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindOneArg() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoDifferentArgs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING, INT));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindTwoSameArgs() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING, STRING));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING, STRING));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRUCT1));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(MAP1));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsEmpty() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList());

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsOne() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargsTwo() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING, STRING));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRUCT1, STRUCT1));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindVarargWithList() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, true, STRING_VARARGS)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING_VARARGS));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(STRING, STRING));

    // Then:
    assertThat(fun.getFunctionName(), equalTo(EXPECTED));
  }

  @Test
  public void shouldFindNonVarargWithNullValues() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(EXPECTED, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // When:
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{null}));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{null}));

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
    final KsqlFunction fun = udfIndex.getFunction(Arrays.asList(new Schema[]{null}));

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
  public void shouldNotMatchIfParamLengthDiffers() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Function 'name' does not accept parameters");

    // When:
    udfIndex.getFunction(Arrays.asList(STRING, STRING));
  }

  @Test
  public void shouldNotMatchIfNoneFound() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Function 'name' does not accept parameters");

    // When:
    udfIndex.getFunction(Arrays.asList(INT));

  }

  @Test
  public void shouldNotMatchIfNullAndPrimitive() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(OTHER, false, Schema.INT32_SCHEMA)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Function 'name' does not accept parameters");

    // When:
    udfIndex.getFunction(Arrays.asList(new Schema[]{null}));

  }

  @Test
  public void shouldNotMatchIfNullAndPrimitiveVararg() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{
        function(OTHER, true, SchemaBuilder.array(Schema.INT32_SCHEMA))};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Function 'name' does not accept parameters");

    // When:
    udfIndex.getFunction(Arrays.asList(new Schema[]{null}));

  }

  @Test
  public void shouldNotMatchIfNoneFoundWithNull() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING, INT)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Function 'name' does not accept parameters");

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
    expectedException.expectMessage("Function 'name' does not accept parameters");

    // When:
    udfIndex.getFunction(Arrays.asList(STRING, INT, STRING));

  }

  @Test
  public void shouldNotMatchWhenNullTypeInArgsIfParamLengthDiffers() {
    // Given:
    final KsqlFunction[] functions = new KsqlFunction[]{function(OTHER, false, STRING)};
    Arrays.stream(functions).forEach(udfIndex::addFunction);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Function 'name' does not accept parameters");

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
    expectedException.expectMessage("Function 'name' does not accept parameters");

    // When:
    udfIndex.getFunction(Arrays.asList(STRUCT1, STRUCT2));

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