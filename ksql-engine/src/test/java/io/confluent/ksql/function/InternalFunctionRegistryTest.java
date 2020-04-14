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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;
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
      Schema.OPTIONAL_STRING_SCHEMA,
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
    final UdfFactory factory = functionRegistry.getUdfFactory("func");
    assertThat(factory.getFunction(Collections.emptyList()), is(this.func));
  }

  @Test
  public void shouldNotAddFunctionWithSameNameAsExistingFunctionAndOnDifferentClass() {
    // Given:
    givenUdfFactoryRegistered();

    final KsqlScalarFunction func2 = KsqlScalarFunction.createLegacyBuiltIn(
        Schema.OPTIONAL_STRING_SCHEMA,
        Collections.emptyList(),
        func.getFunctionName(),
        Func2.class);

    functionRegistry.addFunction(func);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.addFunction(func2)
    );

    // Then:
    assertThat(e.getMessage(), containsString("a function with the same name exists in a different class"));
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
    final KsqlException e = assertThrows(
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
    final KsqlException e = assertThrows(
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
    final KsqlException e = assertThrows(
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
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.addAggregateFunctionFactory(udafFactory)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Aggregate function already registered as non-aggregate: SOMEAGGFUNC"));

  }

  @Test
  public void shouldThrowOnInvalidUdfFunctionName() {
    // Given:
    when(udfFactory.getName()).thenReturn("i am invalid");

    // When:
    final KsqlException e = assertThrows(
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
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.addAggregateFunctionFactory(udafFactory)
    );

    // Then:
    assertThat(e.getMessage(), containsString("is not a valid function name"));
  }

  @Test
  public void shouldKnowIfFunctionIsAggregate() {
    assertFalse(functionRegistry.isAggregate("lcase"));
    assertTrue(functionRegistry.isAggregate("topk"));
  }

  @Test
  public void shouldAddAggregateFunction() {
    functionRegistry.addAggregateFunctionFactory(createAggregateFunctionFactory());
    assertThat(functionRegistry.getAggregateFunction("my_aggregate",
        Schema.OPTIONAL_INT32_SCHEMA,
        AggregateFunctionInitArguments.EMPTY_ARGS), not(nullValue()));
  }

  @Test
  public void shouldAddTableFunction() {
    functionRegistry.addTableFunctionFactory(createTableFunctionFactory());
    assertThat(functionRegistry.getTableFunction("my_tablefunction",
        ImmutableList.of(Schema.OPTIONAL_INT32_SCHEMA)
    ), not(nullValue()));
  }

  @Test
  public void shouldAddFunctionWithSameNameButDifferentReturnTypes() {
    // Given:
    givenUdfFactoryRegistered();
    functionRegistry.addFunction(func);
    final KsqlScalarFunction func2 = KsqlScalarFunction.createLegacyBuiltIn(
        Schema.OPTIONAL_INT64_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA), FunctionName.of("func"), Func1.class);

    // When:
    functionRegistry.addFunction(func2);

    // Then: no exception thrown.
  }

  @Test
  public void shouldAddFunctionWithSameNameClassButDifferentArguments() {
    // Given:
    givenUdfFactoryRegistered();

    final KsqlScalarFunction func2 = KsqlScalarFunction.createLegacyBuiltIn(
        Schema.OPTIONAL_STRING_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
        func.getFunctionName(),
        Func1.class);

    functionRegistry.addFunction(func);

    // When:
    functionRegistry.addFunction(func2);

    // Then:
    assertThat(functionRegistry.getUdfFactory("func")
        .getFunction(Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA)), equalTo(func2));
    assertThat(functionRegistry.getUdfFactory("func")
        .getFunction(Collections.emptyList()), equalTo(func));
  }

  @Test
  public void shouldThrowExceptionIfNoFunctionsWithNameExist() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> functionRegistry.getUdfFactory("foo_bar")
    );

    // Then:
    assertThat(e.getMessage(), containsString("'foo_bar'"));
  }

  @Test
  public void shouldHaveAllInitializedFunctionNamesInUppercase() {
    for (UdfFactory udfFactory : functionRegistry.listFunctions()) {
      String actual = udfFactory.getName();
      String expected = actual.toUpperCase();

      assertThat("UDF name must be registered in uppercase", actual, equalTo(expected));
    }
  }

  @Test
  public void shouldHaveBuiltInUDFRegistered() {

    // Verify that all built-in UDF are correctly registered in the InternalFunctionRegistry
    List<String> buildtInUDF = Arrays.asList(
        // String UDF
        "LCASE", "UCASE", "CONCAT", "TRIM", "IFNULL", "LEN",
        // Math UDF
        "CEIL", "RANDOM",
        // JSON UDF
        "EXTRACTJSONFIELD", "ARRAYCONTAINS",
        // Struct UDF
        "FETCH_FIELD_FROM_STRUCT"
    );

    Collection<String> names = Collections2.transform(functionRegistry.listFunctions(),
        UdfFactory::getName);

    assertThat("More or less UDF are registered in the InternalFunctionRegistry",
        names, containsInAnyOrder(buildtInUDF.toArray()));
  }

  @Test
  public void shouldHaveBuiltInUDAFRegistered() {
    Collection<String> builtInUDAF = Arrays.asList(
        "COUNT", "SUM", "MAX", "MIN", "TOPK", "TOPKDISTINCT"
    );

    Collection<String> names = Collections2.transform(functionRegistry.listAggregateFunctions(),
        AggregateFunctionFactory::getName);

    assertThat("More or less UDAF are registered in the InternalFunctionRegistry",
        names, containsInAnyOrder(builtInUDAF.toArray()));

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

  private void givenUdfFactoryRegistered() {
    functionRegistry.ensureFunctionFactory(UdfLoaderUtil.createTestUdfFactory(func));
  }

  private static AggregateFunctionFactory createAggregateFunctionFactory() {
    return new AggregateFunctionFactory("my_aggregate") {
      @Override
      public KsqlAggregateFunction createAggregateFunction(final List<Schema> argTypeList,
          final AggregateFunctionInitArguments initArgs) {
        return new KsqlAggregateFunction() {

          @Override
          public FunctionName getFunctionName() {
            return FunctionName.of("my_aggregate");
          }

          @Override
          public Supplier getInitialValueSupplier() {
            return null;
          }

          @Override
          public int getArgIndexInValue() {
            return 0;
          }

          @Override
          public Schema getAggregateType() {
            return null;
          }

          @Override
          public SqlType aggregateType() {
            return null;
          }

          @Override
          public Schema getReturnType() {
            return null;
          }

          @Override
          public SqlType returnType() {
            return null;
          }

          @Override
          public Object aggregate(final Object currentValue, final Object aggregateValue) {
            return null;
          }

          @Override
          public Merger getMerger() {
            return null;
          }

          @Override
          public Function getResultMapper() {
            return null;
          }

          @Override
          public List<Schema> getArguments() {
            return argTypeList;
          }

          @Override
          public String getDescription() {
            return null;
          }
        };
      }

      @Override
      public List<List<Schema>> supportedArgs() {
        return ImmutableList.of();
      }
    };
  }

  private TableFunctionFactory createTableFunctionFactory() {
    return new TableFunctionFactory(new UdfMetadata("my_tablefunction",
        "", "", "", "", false)) {
      @Override
      public KsqlTableFunction createTableFunction(List<Schema> argTypeList) {
        return tableFunction;
      }

      @Override
      protected List<List<Schema>> supportedArgs() {
        return null;
      }
    };
  }
}