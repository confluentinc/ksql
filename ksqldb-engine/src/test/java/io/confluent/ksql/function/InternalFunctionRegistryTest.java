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

import static io.confluent.ksql.function.UserFunctionLoaderTestUtil.loadAllUserFunctions;
import static io.confluent.ksql.name.FunctionName.of;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InternalFunctionRegistryTest {

  private static final String UDF_NAME = "someFunc";
  private static final String UDAF_NAME = "someAggFunc";

  private static class Func1 implements Kudf {

    @Override
    public Object evaluate(final Object... args) {
      return null;
    }
  }

  private static class Func2 implements Kudf {

    @Override
    public Object evaluate(final Object... args) {
      return null;
    }
  }

  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final KsqlScalarFunction func = KsqlScalarFunction.createLegacyBuiltIn(
      SqlTypes.STRING,
      Collections.emptyList(),
      FunctionName.of("func"),
      Func1.class);

  @Mock(name = "udfFactory")
  private UdfFactory udfFactory;

  @Mock(name = "udfFactory1")
  private UdfFactory udfFactory1;

  @Mock
  private AggregateFunctionFactory udafFactory;

  @Mock
  private KsqlAggregateFunction mockAggFun;

  @Mock
  private KsqlTableFunction tableFunction;

  @Before
  public void setUp() {
    when(udfFactory.getName()).thenReturn(UDF_NAME);
    when(udfFactory1.getName()).thenReturn(UDF_NAME);
    when(udafFactory.getName()).thenReturn(UDAF_NAME);

    when(udfFactory.matches(udfFactory)).thenReturn(true);
  }

  @Test
  public void shouldAddFunction() {
    // Given:
    givenUdfFactoryRegistered();

    // When:
    functionRegistry.addFunction(func);

    // Then:
    final UdfFactory factory = functionRegistry.getUdfFactory(FunctionName.of("func"));
    assertThat(factory.getFunction(Collections.emptyList()), is(this.func));
  }

  @Test
  public void shouldNotAddFunctionWithSameNameAsExistingFunctionAndOnDifferentClass() {
    // Given:
    givenUdfFactoryRegistered();

    final KsqlScalarFunction func2 = KsqlScalarFunction.createLegacyBuiltIn(
        SqlTypes.STRING,
        Collections.emptyList(),
        func.name(),
        Func2.class);

    functionRegistry.addFunction(func);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.addFunction(func2)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "a function with the same name exists in a different class"));
  }

  @Test
  public void shouldNotThrowWhenEnsuringCompatibleUdfFactory() {
    // Given:
    functionRegistry.ensureFunctionFactory(udfFactory);

    // When:
    functionRegistry.ensureFunctionFactory(udfFactory);

    // Then: no exception thrown.
  }

  @Test
  public void shouldThrowOnEnsureUdfFactoryOnDifferentX() {
    // Given:
    functionRegistry.ensureFunctionFactory(udfFactory);

    when(udfFactory.matches(udfFactory1)).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.ensureFunctionFactory(udfFactory1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("UdfFactory not compatible with existing factory"));
  }

  @Test
  public void shouldThrowOnAddUdfIfUadfFactoryAlreadyExists() {
    // Given:
    when(udafFactory.getName()).thenReturn(UDF_NAME);
    functionRegistry.addAggregateFunctionFactory(udafFactory);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.ensureFunctionFactory(udfFactory)
    );

    // Then:
    assertThat(e.getMessage(), containsString("UdfFactory already registered as aggregate: SOMEFUNC"));
  }

  @Test
  public void shouldThrowOnAddUdafIfUdafFactoryAlreadyExists() {
    // Given:
    functionRegistry.addAggregateFunctionFactory(udafFactory);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.addAggregateFunctionFactory(udafFactory)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Aggregate function already registered: SOMEAGGFUNC"));
  }

  @Test
  public void shouldThrowOnAddUdafIfUdfFactoryAlreadyExists() {
    // Given:
    when(udfFactory.getName()).thenReturn(UDAF_NAME);
    functionRegistry.ensureFunctionFactory(udfFactory);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.addAggregateFunctionFactory(udafFactory)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Aggregate function already registered as non-aggregate: SOMEAGGFUNC"));
  }

  @Test
  public void shouldThrowOnInvalidUdfFunctionName() {
    // Given:
    when(udfFactory.getName()).thenReturn("i am invalid");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.ensureFunctionFactory(udfFactory)
    );

    // Then:
    assertThat(e.getMessage(), containsString("is not a valid function name"));
  }

  @Test
  public void shouldThrowOnInvalidUdafFunctionName() {
    // Given:
    when(udafFactory.getName()).thenReturn("i am invalid");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.addAggregateFunctionFactory(udafFactory)
    );

    // Then:
    assertThat(e.getMessage(), containsString("is not a valid function name"));
  }

  @Test
  public void shouldKnowIfFunctionIsAggregate() {
    loadAllUserFunctions(functionRegistry);
    assertFalse(functionRegistry.isAggregate(FunctionName.of("lcase")));
    assertTrue(functionRegistry.isAggregate(FunctionName.of("topk")));
  }

  @Test
  public void shouldAddAggregateFunction() {
    functionRegistry.addAggregateFunctionFactory(createAggregateFunctionFactory());
    assertThat(functionRegistry.getAggregateFactory(FunctionName.of("my_aggregate")),
            not(nullValue()));
  }

  @Test
  public void shouldAddTableFunction() {
    functionRegistry.addTableFunctionFactory(createTableFunctionFactory());
    assertThat(functionRegistry.getTableFunction(FunctionName.of("my_tablefunction"),
        ImmutableList.of(SqlArgument.of(SqlTypes.INTEGER))
    ), not(nullValue()));
  }

  @Test
  public void shouldAddFunctionWithSameNameButDifferentReturnTypes() {
    // Given:
    givenUdfFactoryRegistered();
    functionRegistry.addFunction(func);
    final KsqlScalarFunction func2 = KsqlScalarFunction.createLegacyBuiltIn(
        SqlTypes.BIGINT,
        Collections.singletonList(ParamTypes.LONG),
        FunctionName.of("func"), Func1.class);

    // When:
    functionRegistry.addFunction(func2);

    // Then: no exception thrown.
  }

  @Test
  public void shouldAddFunctionWithSameNameClassButDifferentArguments() {
    // Given:
    givenUdfFactoryRegistered();

    final KsqlScalarFunction func2 = KsqlScalarFunction.createLegacyBuiltIn(
        SqlTypes.STRING,
        Collections.singletonList(ParamTypes.LONG),
        func.name(),
        Func1.class);

    functionRegistry.addFunction(func);

    // When:
    functionRegistry.addFunction(func2);

    // Then:
    assertThat(functionRegistry.getUdfFactory(FunctionName.of("func"))
        .getFunction(Collections.singletonList(SqlArgument.of(SqlTypes.BIGINT))), equalTo(func2));
    assertThat(functionRegistry.getUdfFactory(FunctionName.of("func"))
        .getFunction(Collections.emptyList()), equalTo(func));
  }

  @Test
  public void shouldThrowExceptionIfNoFunctionsWithNameExist() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.getUdfFactory(of("foo_bar"))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "'foo_bar'"));
  }

  @Test
  public void shouldHaveAllInitializedFunctionNamesInUppercase() {
    for (final UdfFactory udfFactory : functionRegistry.listFunctions()) {
      final String actual = udfFactory.getName();
      final String expected = actual.toUpperCase();

      assertThat("UDF name must be registered in uppercase", actual, equalTo(expected));
    }
  }

  @Test
  public void shouldHaveNoBuiltInUDFsRegistered() {
    final Collection<String> names = Collections2.transform(functionRegistry.listFunctions(),
        UdfFactory::getName);

    assertThat("One or more built-in Kudfs are registered in the InternalFunctionRegistry",
        names, hasSize(0));
  }

  @Test
  public void shouldNotAllowModificationViaListFunctions() {
    // Given:
    functionRegistry.ensureFunctionFactory(udfFactory);

    final List<UdfFactory> functions = functionRegistry.listFunctions();

    // When
    functions.clear();

    // Then:
    assertThat(functionRegistry.listFunctions(), hasItem(sameInstance(udfFactory)));
  }

  @Test
  public void shouldKnowIfFunctionIsPresent() {
    loadAllUserFunctions(functionRegistry);

    // Given:
    when(udafFactory.getName()).thenReturn(UDF_NAME);
    functionRegistry.addAggregateFunctionFactory(udafFactory);

    // When / Then:
    assertThat(functionRegistry.isPresent(FunctionName.of("count")), is(true));
    assertThat(functionRegistry.isPresent(FunctionName.of(UDF_NAME)), is(true));
    assertThat(functionRegistry.isPresent(FunctionName.of("not_found")), is(false));
  }

  private void givenUdfFactoryRegistered() {
    functionRegistry.ensureFunctionFactory(UdfLoaderUtil.createTestUdfFactory(func));
  }

  private AggregateFunctionFactory createAggregateFunctionFactory() {
    return new AggregateFunctionFactory("my_aggregate") {

      @Override
      public FunctionSource getFunction(List<SqlType> argTypeList) {
        return new FunctionSource(0, (initArgs) -> mockAggFun);
      }

      @Override
      public List<List<ParamType>> supportedArgs() {
        return ImmutableList.of();
      }
    };
  }

  private TableFunctionFactory createTableFunctionFactory() {
    return new TableFunctionFactory(new UdfMetadata("my_tablefunction",
        "", "", "", FunctionCategory.OTHER, "")) {
      @Override
      public KsqlTableFunction createTableFunction(final List<SqlArgument> argTypeList) {
        return tableFunction;
      }

      @Override
      protected List<List<ParamType>> supportedParams() {
        return null;
      }
    };
  }
}