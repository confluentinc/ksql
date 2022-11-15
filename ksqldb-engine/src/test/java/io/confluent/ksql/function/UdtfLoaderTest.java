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

import static io.confluent.ksql.function.KsqlScalarFunction.INTERNAL_PATH;
import static io.confluent.ksql.metastore.TypeRegistry.EMPTY;
import static io.confluent.ksql.schema.ksql.SqlTypeParser.create;
import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class UdtfLoaderTest {

  private static final ClassLoader PARENT_CLASS_LOADER = UdtfLoaderTest.class.getClassLoader();

  private static final FunctionRegistry FUNC_REG = initializeFunctionRegistry();


  @Test
  public void shouldLoadSimpleParams() {

    // Given:
    final List<SqlType> args = ImmutableList.of(
        SqlTypes.INTEGER,
        SqlTypes.BIGINT,
        SqlTypes.DOUBLE,
        SqlTypes.BOOLEAN,
        SqlTypes.STRING,
        DECIMAL_SCHEMA,
        STRUCT_SCHEMA
    );

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.STRING));
  }

  @Test
  public void shouldLoadParameterizedListParams() {

    // Given:
    final List<SqlType> args = ImmutableList.of(
        SqlTypes.array(SqlTypes.INTEGER),
        SqlTypes.array(SqlTypes.BIGINT),
        SqlTypes.array(SqlTypes.DOUBLE),
        SqlTypes.array(SqlTypes.BOOLEAN),
        SqlTypes.array(SqlTypes.STRING),
        SqlTypes.array(DECIMAL_SCHEMA),
        SqlTypes.array(STRUCT_SCHEMA)
    );

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.STRING));
  }

  @Test
  public void shouldLoadParameterizedMapParams() {

    // Given:
    final List<SqlType> args = ImmutableList.of(
        SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER),
        SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT),
        SqlTypes.map(SqlTypes.STRING, SqlTypes.DOUBLE),
        SqlTypes.map(SqlTypes.STRING, SqlTypes.BOOLEAN),
        SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING),
        SqlTypes.map(SqlTypes.STRING, DECIMAL_SCHEMA),
        SqlTypes.map(SqlTypes.STRING, STRUCT_SCHEMA)
    );

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.STRING));
  }

  @Test
  public void shouldLoadListIntReturn() {

    // Given:
    final List<SqlType> args = ImmutableList.of(SqlTypes.INTEGER);

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.INTEGER));
  }

  @Test
  public void shouldLoadListLongReturn() {

    // Given:
    final List<SqlType> args = ImmutableList.of(SqlTypes.BIGINT);

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.BIGINT));
  }

  @Test
  public void shouldLoadListDoubleReturn() {

    // Given:
    final List<SqlType> args = ImmutableList.of(SqlTypes.DOUBLE);

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.DOUBLE));
  }

  @Test
  public void shouldLoadListBooleanReturn() {

    // Given:
    final List<SqlType> args = ImmutableList.of(SqlTypes.BOOLEAN);

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldLoadListStringReturn() {

    // Given:
    final List<SqlType> args = ImmutableList.of(SqlTypes.STRING);

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.STRING));
  }

  @Test
  public void shouldLoadListBigDecimalReturnWithSchemaProvider() {

    // Given:
    final List<SqlType> args = ImmutableList.of(DECIMAL_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.decimal(30, 10)));
  }

  @Test
  public void shouldLoadListStructReturnWithSchemaAnnotation() {

    // Given:
    final List<SqlType> args = ImmutableList.of(STRUCT_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(STRUCT_SCHEMA));
  }

  @Test
  public void shouldLoadVarArgsMethod() {

    // Given:
    final List<SqlType> args = ImmutableList.of(STRUCT_SCHEMA);

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(STRUCT_SCHEMA));
  }

  @Test
  public void shouldNotLoadUdtfWithWrongReturnValue() {
    // Given:
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final SqlTypeParser typeParser = create(EMPTY);
    final UdtfLoader udtfLoader = new UdtfLoader(
        functionRegistry, empty(), typeParser, true
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udtfLoader.loadUdtfFromClass(UdtfBadReturnValue.class, INTERNAL_PATH)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UDTF functions must return a List. Class io.confluent.ksql"
            + ".function.UdtfLoaderTest$UdtfBadReturnValue Method badReturn"));
  }

  @Test
  public void shouldNotLoadUdtfWithRawListReturn() {
    // Given:
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final SqlTypeParser typeParser = create(EMPTY);
    final UdtfLoader udtfLoader = new UdtfLoader(
        functionRegistry, empty(), typeParser, true
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udtfLoader.loadUdtfFromClass(RawListReturn.class, INTERNAL_PATH)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "UDTF functions must return a parameterized List. Class io.confluent.ksql.function.UdtfLoaderTest$RawListReturn Method badReturn"));
  }

  @Test
  public void shouldNotLoadUdtfWithBigDecimalReturnAndNoSchemaProvider() {
    // Given:
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final SqlTypeParser typeParser = create(EMPTY);
    final UdtfLoader udtfLoader = new UdtfLoader(
        functionRegistry, empty(), typeParser, true
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udtfLoader
            .loadUdtfFromClass(BigDecimalNoSchemaProvider.class, INTERNAL_PATH)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot load UDF bigDecimalNoSchemaProvider. DECIMAL return type is not supported without an explicit schema"));
  }

  @UdtfDescription(name = "badReturnUdtf", description = "whatever")
  static class UdtfBadReturnValue {

    @Udtf
    public Map<String, String> badReturn(final int foo) {
      return new HashMap<>();
    }
  }

  @UdtfDescription(name = "rawListReturn", description = "whatever")
  static class RawListReturn {

    @Udtf
    public List badReturn(final int foo) {
      return new ArrayList();
    }
  }

  @UdtfDescription(name = "bigDecimalNoSchemaProvider", description = "whatever")
  static class BigDecimalNoSchemaProvider {

    @Udtf
    public List<BigDecimal> badReturn(final int foo) {
      return ImmutableList.of(new BigDecimal("123"));
    }
  }

  private static final SqlType STRUCT_SCHEMA = SqlTypes.struct().field("A", SqlTypes.STRING).build();
  private static final SqlType DECIMAL_SCHEMA = SqlTypes.decimal(2, 1);

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