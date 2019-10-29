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

package io.confluent.ksql.function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UdtfLoaderTest {

  private static final ClassLoader PARENT_CLASS_LOADER = UdtfLoaderTest.class.getClassLoader();

  private static final FunctionRegistry FUNC_REG = initializeFunctionRegistry();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();


  @Test
  public void shouldLoadSimpleParams() {

    // Given:
    final List<Schema> args = ImmutableList.of(
        Schema.INT32_SCHEMA,
        Schema.INT64_SCHEMA,
        Schema.FLOAT64_SCHEMA,
        Schema.BOOLEAN_SCHEMA,
        Schema.STRING_SCHEMA,
        DECIMAL_SCHEMA,
        STRUCT_SCHEMA
    );

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldLoadParameterizedListParams() {

    // Given:
    final List<Schema> args = ImmutableList.of(
        SchemaBuilder.array(Schema.INT32_SCHEMA).build(),
        SchemaBuilder.array(Schema.INT64_SCHEMA).build(),
        SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(),
        SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build(),
        SchemaBuilder.array(Schema.STRING_SCHEMA).build(),
        SchemaBuilder.array(DECIMAL_SCHEMA).build(),
        SchemaBuilder.array(STRUCT_SCHEMA).build()
    );

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldLoadParameterizedMapParams() {

    // Given:
    final List<Schema> args = ImmutableList.of(
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build(),
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA).build(),
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build(),
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build(),
        SchemaBuilder.map(Schema.STRING_SCHEMA, DECIMAL_SCHEMA).build(),
        SchemaBuilder.map(
            Schema.STRING_SCHEMA,
            STRUCT_SCHEMA
        ).build()
    );

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldLoadListIntReturn() {

    // Given:
    final List<Schema> args = ImmutableList.of(Schema.INT32_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldLoadListLongReturn() {

    // Given:
    final List<Schema> args = ImmutableList.of(Schema.INT64_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(Schema.OPTIONAL_INT64_SCHEMA));
  }

  @Test
  public void shouldLoadListDoubleReturn() {

    // Given:
    final List<Schema> args = ImmutableList.of(Schema.FLOAT64_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(Schema.OPTIONAL_FLOAT64_SCHEMA));
  }

  @Test
  public void shouldLoadListBooleanReturn() {

    // Given:
    final List<Schema> args = ImmutableList.of(Schema.BOOLEAN_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(Schema.OPTIONAL_BOOLEAN_SCHEMA));
  }

  @Test
  public void shouldLoadListStringReturn() {

    // Given:
    final List<Schema> args = ImmutableList.of(Schema.STRING_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldLoadListBigDecimalReturnWithSchemaProvider() {

    // Given:
    final List<Schema> args = ImmutableList.of(DECIMAL_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(DecimalUtil.builder(30, 10).build()));
  }

  @Test
  public void shouldLoadListStructReturnWithSchemaAnnotation() {

    // Given:
    final List<Schema> args = ImmutableList.of(STRUCT_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(STRUCT_SCHEMA));
  }

  @Test
  public void shouldLoadVarArgsMethod() {

    // Given:
    final List<Schema> args = ImmutableList.of(STRUCT_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG.getTableFunction("test_udtf", args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(STRUCT_SCHEMA));
  }

  @Test
  public void shouldNotLoadUdtfWithWrongReturnValue() {
    // Given:
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
    final UdtfLoader udtfLoader = new UdtfLoader(
        functionRegistry, Optional.empty(), typeParser, true
    );

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage(
            is("UDTF functions must return a List. Class io.confluent.ksql.function.UdtfLoaderTest$UdtfBadReturnValue Method badReturn"));

    // When:
    udtfLoader.loadUdtfFromClass(UdtfBadReturnValue.class, KsqlFunction.INTERNAL_PATH);
  }

  @Test
  public void shouldNotLoadUdtfWithRawListReturn() {
    // Given:
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
    final UdtfLoader udtfLoader = new UdtfLoader(
        functionRegistry, Optional.empty(), typeParser, true
    );

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage(
            is("UDTF functions must return a parameterized List. Class io.confluent.ksql.function.UdtfLoaderTest$RawListReturn Method badReturn"));

    // When:
    udtfLoader.loadUdtfFromClass(RawListReturn.class, KsqlFunction.INTERNAL_PATH);
  }

  @Test
  public void shouldNotLoadUdtfWithBigDecimalReturnAndNoSchemaProvider() {
    // Given:
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
    final UdtfLoader udtfLoader = new UdtfLoader(
        functionRegistry, Optional.empty(), typeParser, true
    );

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage(
            is("Cannot load UDF bigDecimalNoSchemaProvider. BigDecimal return type is not supported without a schema provider method."));

    // When:
    udtfLoader.loadUdtfFromClass(BigDecimalNoSchemaProvider.class, KsqlFunction.INTERNAL_PATH);
  }
  
  @UdtfDescription(name = "badReturnUdtf", description = "whatever")
  static class UdtfBadReturnValue {

    @Udtf
    public Map<String, String> badReturn(int foo) {
      return new HashMap<>();
    }
  }

  @UdtfDescription(name = "rawListReturn", description = "whatever")
  static class RawListReturn {

    @Udtf
    public List badReturn(int foo) {
      return new ArrayList();
    }
  }

  @UdtfDescription(name = "bigDecimalNoSchemaProvider", description = "whatever")
  static class BigDecimalNoSchemaProvider {

    @Udtf
    public List<BigDecimal> badReturn(int foo) {
      return ImmutableList.of(new BigDecimal("123"));
    }
  }

  private static final Schema STRUCT_SCHEMA =
      SchemaBuilder.struct().field("A", Schema.OPTIONAL_STRING_SCHEMA).optional()
          .build();

  private static final Schema DECIMAL_SCHEMA =
      DecimalUtil.builder(2, 1).build();

  private static FunctionRegistry initializeFunctionRegistry() {
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final UserFunctionLoader pluginLoader = createUdfLoader(functionRegistry);
    pluginLoader.load();
    return functionRegistry;
  }

  private static UserFunctionLoader createUdfLoader(
      final MutableFunctionRegistry functionRegistry
  ) {
    return new UserFunctionLoader(
        functionRegistry,
        new File("src/test/resources/udf-example.jar"),
        PARENT_CLASS_LOADER,
        value -> false,
        Optional.empty(),
        true
    );
  }

}