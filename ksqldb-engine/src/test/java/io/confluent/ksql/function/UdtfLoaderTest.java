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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
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
    final List<SqlArgument> args = ImmutableList.of(
        SqlArgument.of(SqlTypes.INTEGER),
        SqlArgument.of(SqlTypes.BIGINT),
        SqlArgument.of(SqlTypes.DOUBLE),
        SqlArgument.of(SqlTypes.BOOLEAN),
        SqlArgument.of(SqlTypes.STRING),
        SqlArgument.of(DECIMAL_SCHEMA),
        SqlArgument.of(STRUCT_SCHEMA)
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
    final List<SqlArgument> args = ImmutableList.of(
        SqlArgument.of(SqlTypes.array(SqlTypes.INTEGER)),
        SqlArgument.of(SqlTypes.array(SqlTypes.BIGINT)),
        SqlArgument.of(SqlTypes.array(SqlTypes.DOUBLE)),
        SqlArgument.of(SqlTypes.array(SqlTypes.BOOLEAN)),
        SqlArgument.of(SqlTypes.array(SqlTypes.STRING)),
        SqlArgument.of(SqlTypes.array(DECIMAL_SCHEMA)),
        SqlArgument.of(SqlTypes.array(STRUCT_SCHEMA))
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
    final List<SqlArgument> args = ImmutableList.of(
        SqlArgument.of(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER)),
        SqlArgument.of(SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT)),
        SqlArgument.of( SqlTypes.map(SqlTypes.STRING, SqlTypes.DOUBLE)),
        SqlArgument.of(SqlTypes.map(SqlTypes.STRING, SqlTypes.BOOLEAN)),
        SqlArgument.of(SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING)),
        SqlArgument.of(SqlTypes.map(SqlTypes.STRING, DECIMAL_SCHEMA)),
        SqlArgument.of(SqlTypes.map(SqlTypes.STRING, STRUCT_SCHEMA))
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
    final List<SqlArgument> args = ImmutableList.of(SqlArgument.of(SqlTypes.INTEGER));

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.INTEGER));
  }

  @Test
  public void shouldLoadListLongReturn() {

    // Given:
    final List<SqlArgument> args = ImmutableList.of(SqlArgument.of(SqlTypes.BIGINT));

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.BIGINT));
  }

  @Test
  public void shouldLoadListDoubleReturn() {

    // Given:
    final List<SqlArgument> args = ImmutableList.of(SqlArgument.of(SqlTypes.DOUBLE));

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.DOUBLE));
  }

  @Test
  public void shouldLoadListBooleanReturn() {

    // Given:
    final List<SqlArgument> args = ImmutableList.of(SqlArgument.of(SqlTypes.BOOLEAN));

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldLoadListStringReturn() {

    // Given:
    final List<SqlArgument> args = ImmutableList.of(SqlArgument.of(SqlTypes.STRING));

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.STRING));
  }

  @Test
  public void shouldLoadListBigDecimalReturnWithSchemaProvider() {

    // Given:
    final List<SqlArgument> args = ImmutableList.of(SqlArgument.of(DECIMAL_SCHEMA));

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SqlTypes.decimal(30, 10)));
  }

  @Test
  public void shouldLoadListStructReturnWithSchemaAnnotation() {

    // Given:
    final List<SqlArgument> args = ImmutableList.of(SqlArgument.of(STRUCT_SCHEMA));

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(STRUCT_SCHEMA));
  }

  @Test
  public void shouldLoadVarArgsMethod() {

    // Given:
    final List<SqlArgument> args = ImmutableList.of(SqlArgument.of(STRUCT_SCHEMA));

    // When:
    final KsqlTableFunction function = FUNC_REG
        .getTableFunction(FunctionName.of("test_udtf"), args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(STRUCT_SCHEMA));
  }

  @Test
  public void shouldNotLetBadUdtfsExitViaBadSchemaProvider() {
    // Given:
    // We do need to set up the ExtensionSecurityManager for our test.
    // This is controlled by a feature flag and in this test, we just directly enable it.
    SecurityManager manager = System.getSecurityManager();
    System.setSecurityManager(ExtensionSecurityManager.INSTANCE);

    // When:
    final Exception error = assertThrows(
        KsqlException.class,
        () ->
            FUNC_REG.getTableFunction(
                    FunctionName.of("bad_test_udtf"),
                    Collections.singletonList(SqlArgument.of(SqlTypes.decimal(2,0))))
                .getReturnType(ImmutableList.of(SqlArgument.of(SqlTypes.DOUBLE)))
    );

    // Then:
    assertThat(error.getMessage(), containsString(
        "Cannot invoke the schema provider method provideSchema for UDF bad_test_udtf."));
    System.setSecurityManager(manager);
    assertEquals(System.getSecurityManager(), manager);
  }

  @Test
  public void shouldNotLetBadUdtfsExit() {
    // Given:
    // We do need to set up the ExtensionSecurityManager for our test.
    // This is controlled by a feature flag and in this test, we just directly enable it.
    SecurityManager manager = System.getSecurityManager();
    System.setSecurityManager(ExtensionSecurityManager.INSTANCE);

    // When:
    final Exception error = assertThrows(
        KsqlFunctionException.class,
        () ->
            FUNC_REG.getTableFunction(
                FunctionName.of("bad_test_udtf"),
                Collections.singletonList(SqlArgument.of(SqlTypes.STRING))).apply("foo")
    );

    // Then:
    assertThat(error.getMessage(), containsString(
        "Failed to invoke function public java.util.List "
            + "io.confluent.ksql.function.udf.BadTestUdtf.listStringReturn(java.lang.String)"));
    System.setSecurityManager(manager);
    assertEquals(System.getSecurityManager(), manager);
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