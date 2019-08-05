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

import static io.confluent.ksql.util.KsqlConfig.KSQL_FUNCTIONS_PROPERTY_PREFIX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

/**
 * This uses ksql-engine/src/test/resource/udf-example.jar to load the custom jars.
 * You can find the classes it is loading in the same directory
 */
public class UdfLoaderTest {

  private static final ClassLoader PARENT_CLASS_LOADER = UdfLoaderTest.class.getClassLoader();
  private static final UdfCompiler COMPILER = new UdfCompiler(Optional.empty());
  private static final Metrics METRICS = new Metrics();

  private static final FunctionRegistry FUNC_REG =
      initializeFunctionRegistry(true, Optional.empty());

  private static final FunctionRegistry FUNC_REG_WITH_METRICS =
      initializeFunctionRegistry(true, Optional.of(METRICS));

  private static final FunctionRegistry FUNC_REG_WITHOUT_CUSTOM =
      initializeFunctionRegistry(false, Optional.empty());

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

  @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  @Before
  public void before() {
    PASSED_CONFIG = null;
  }

  @Test
  public void shouldLoadFunctionsInKsqlEngine() {
    final UdfFactory function = FUNC_REG.getUdfFactory("substring");
    assertThat(function, not(nullValue()));

    final Kudf substring1 = function.getFunction(
        Arrays.asList(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)).newInstance(ksqlConfig);
    assertThat(substring1.evaluate("foo", 2), equalTo("oo"));

    final Kudf substring2 = function.getFunction(
        Arrays.asList(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)).newInstance(ksqlConfig);
    assertThat(substring2.evaluate("foo", 2, 1), equalTo("o"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldLoadUdafs() {
    final KsqlAggregateFunction aggregate = FUNC_REG
        .getAggregate("test_udaf", Schema.OPTIONAL_INT64_SCHEMA);
    final KsqlAggregateFunction<Long, Long> instance = aggregate.getInstance(
        new AggregateFunctionArguments(0, Collections.singletonList("udfIndex")));
    assertThat(instance.getInitialValueSupplier().get(), equalTo(0L));
    assertThat(instance.aggregate(1L, 1L), equalTo(2L));
    assertThat(instance.getMerger().apply(null, 2L, 3L), equalTo(5L));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldLoadStructUdafs() {
    final Schema schema = SchemaBuilder.struct()
        .field("A", Schema.OPTIONAL_INT32_SCHEMA)
        .field("B", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final KsqlAggregateFunction aggregate = FUNC_REG
        .getAggregate("test_udaf", schema);
    final KsqlAggregateFunction<Struct, Struct> instance = aggregate.getInstance(
        new AggregateFunctionArguments(0, Collections.singletonList("udfIndex")));

    assertThat(instance.getInitialValueSupplier().get(),
        equalTo(new Struct(schema).put("A", 0).put("B", 0)));
    assertThat(instance.aggregate(
        new Struct(schema).put("A", 0).put("B", 0),
        new Struct(schema).put("A", 1).put("B", 2)
        ),
        equalTo(new Struct(schema).put("A", 1).put("B", 2)));
    assertThat(instance.getMerger().apply(null,
        new Struct(schema).put("A", 0).put("B", 0),
        new Struct(schema).put("A", 1).put("B", 2)
        ),
        equalTo(new Struct(schema).put("A", 1).put("B", 2)));
  }

  @Test
  public void shouldLoadFunctionsFromJarsInPluginDir() {
    final UdfFactory toString = FUNC_REG.getUdfFactory("tostring");
    final UdfFactory multi = FUNC_REG.getUdfFactory("multiply");
    assertThat(toString, not(nullValue()));
    assertThat(multi, not(nullValue()));
  }

  @Test
  public void shouldLoadFunctionWithListReturnType() {
    // Given:
    final UdfFactory toList = FUNC_REG.getUdfFactory("tolist");

    // When:
    final List<Schema> args = Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA);
    final KsqlFunction function
        = toList.getFunction(args);

    assertThat(function.getReturnType(args),
        is(SchemaBuilder
            .array(Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build())
    );
  }

  @Test
  public void shouldLoadFunctionWithMapReturnType() {
    // Given:
    final UdfFactory toMap = FUNC_REG.getUdfFactory("tomap");

    // When:
    final List<Schema> args = Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA);
    final KsqlFunction function
        = toMap.getFunction(args);

    // Then:
    assertThat(
        function.getReturnType(args),
        equalTo(SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
        )
    );
  }

  @Test
  public void shouldLoadFunctionWithStructReturnType() {
    // Given:
    final UdfFactory toStruct = FUNC_REG.getUdfFactory("tostruct");

    // When:
    final List<Schema> args = Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA);
    final KsqlFunction function
        = toStruct.getFunction(args);

    // Then:
    assertThat(function.getReturnType(args), equalTo(SchemaBuilder.struct()
        .field("A", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build())
    );
  }

  @Test
  public void shouldPutJarUdfsInClassLoaderForJar() throws Exception {
    final UdfFactory toString = FUNC_REG.getUdfFactory("tostring");
    final UdfFactory multiply = FUNC_REG.getUdfFactory("multiply");

    final Kudf toStringUdf = toString.getFunction(ImmutableList.of(Schema.STRING_SCHEMA))
        .newInstance(ksqlConfig);

    final Kudf multiplyUdf = multiply.getFunction(
        Arrays.asList(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA))
        .newInstance(ksqlConfig);

    final ClassLoader multiplyLoader = getActualUdfClassLoader(multiplyUdf);
    assertThat(multiplyLoader, equalTo(getActualUdfClassLoader(toStringUdf)));
    assertThat(multiplyLoader, not(equalTo(PARENT_CLASS_LOADER)));
  }

  @Test
  public void shouldCreateUdfFactoryWithJarPathWhenExternal() {
    final UdfFactory tostring = FUNC_REG.getUdfFactory("tostring");
    assertThat(tostring.getPath(), equalTo("src/test/resources/udf-example.jar"));
  }

  @Test
  public void shouldCreateUdfFactoryWithInternalPathWhenInternal() {
    final UdfFactory substring = FUNC_REG.getUdfFactory("substring");
    assertThat(substring.getPath(), equalTo(KsqlFunction.INTERNAL_PATH));
  }

  @Test
  public void shouldSupportUdfParameterAnnotation() {
    final UdfFactory substring = FUNC_REG.getUdfFactory("somefunction");
    final KsqlFunction function = substring.getFunction(
        ImmutableList.of(
            Schema.OPTIONAL_STRING_SCHEMA,
            Schema.OPTIONAL_STRING_SCHEMA,
            Schema.OPTIONAL_STRING_SCHEMA));

    final List<Schema> arguments = function.getArguments();

    assertThat(arguments.get(0).name(), is("justValue"));
    assertThat(arguments.get(0).doc(), is(""));
    assertThat(arguments.get(1).name(), is("valueAndDescription"));
    assertThat(arguments.get(1).doc(), is("Some description"));
    // NB: Is the below failing?
    // Then you need to add `-parameters` to your IDE's java compiler settings.
    assertThat(arguments.get(2).name(), is("noValue"));
    assertThat(arguments.get(2).doc(), is(""));
  }

  @Test
  public void shouldPutKsqlFunctionsInParentClassLoader() throws Exception {
    final UdfFactory substring = FUNC_REG.getUdfFactory("substring");
    final Kudf kudf = substring.getFunction(
        Arrays.asList(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
        .newInstance(ksqlConfig);
    assertThat(getActualUdfClassLoader(kudf), equalTo(PARENT_CLASS_LOADER));
  }

  @Test
  public void shouldLoadUdfsInKSQLIfLoadCustomerUdfsFalse() {
    // udf in ksql-engine will throw if not found
    FUNC_REG_WITHOUT_CUSTOM.getUdfFactory("substring");
  }

  @Test
  public void shouldNotLoadCustomUDfsIfLoadCustomUdfsFalse() {
    // udf in udf-example.jar
    try {
      FUNC_REG_WITHOUT_CUSTOM.getUdfFactory("tostring");
      fail("Should have thrown as function doesn't exist");
    } catch (final KsqlException e) {
      // pass
    }
  }

  @Test
  public void shouldCollectMetricsWhenMetricCollectionEnabled() {
    // Given:
    final UdfFactory substring = FUNC_REG_WITH_METRICS.getUdfFactory("substring");
    final KsqlFunction function = substring
        .getFunction(Arrays.asList(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA));

    // When:
    final Kudf kudf = function.newInstance(ksqlConfig);

    // Then:
    assertThat(kudf, instanceOf(UdfMetricProducer.class));
    final Sensor sensor = METRICS.getSensor("ksql-udf-substring");
    assertThat(sensor, not(nullValue()));
    assertThat(METRICS.metric(METRICS.metricName("ksql-udf-substring-count", "ksql-udf")),
        not(nullValue()));
    assertThat(METRICS.metric(METRICS.metricName("ksql-udf-substring-max", "ksql-udf")),
        not(nullValue()));
    assertThat(METRICS.metric(METRICS.metricName("ksql-udf-substring-avg", "ksql-udf")),
        not(nullValue()));
    assertThat(METRICS.metric(METRICS.metricName("ksql-udf-substring-rate", "ksql-udf")),
        not(nullValue()));
  }

  @Test
  public void shouldUseConfigForExtDir() {
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    // The tostring function is in the udf-example.jar that is found in src/test/resources
    final ImmutableMap<Object, Object> configMap
        = ImmutableMap.builder().put(KsqlConfig.KSQL_EXT_DIR, "src/test/resources")
        .put(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED, false)
        .build();
    final KsqlConfig config
        = new KsqlConfig(configMap);
    UdfLoader.newInstance(config, functionRegistry, "").load();
    // will throw if it doesn't exist
    functionRegistry.getUdfFactory("tostring");
  }

  @Test
  public void shouldNotThrowWhenExtDirDoesntExist() {
    final ImmutableMap<Object, Object> configMap
        = ImmutableMap.builder().put(KsqlConfig.KSQL_EXT_DIR, "foo/bar")
        .put(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED, false)
        .build();
    final KsqlConfig config
        = new KsqlConfig(configMap);
    UdfLoader.newInstance(config, new InternalFunctionRegistry(), "").load();
  }

  @Test
  public void shouldConfigureConfigurableUdfsOnInstantiation() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG, "should not be passed",
        KSQL_FUNCTIONS_PROPERTY_PREFIX + "configurableudf.some.setting", "foo-bar",
        KSQL_FUNCTIONS_PROPERTY_PREFIX + "_global_.expected-param", "expected-value"
    ));

    final KsqlFunction udf = FUNC_REG.getUdfFactory("ConfigurableUdf")
        .getFunction(ImmutableList.of(Schema.INT32_SCHEMA));

    // When:
    udf.newInstance(ksqlConfig);

    // Then:
    assertThat(PASSED_CONFIG, is(notNullValue()));
    assertThat(PASSED_CONFIG.keySet(), not(hasItem(KsqlConfig.KSQL_SERVICE_ID_CONFIG)));
    assertThat(PASSED_CONFIG.get(KSQL_FUNCTIONS_PROPERTY_PREFIX + "configurableudf.some.setting"),
        is("foo-bar"));
    assertThat(PASSED_CONFIG.get(KSQL_FUNCTIONS_PROPERTY_PREFIX + "_global_.expected-param"),
        is("expected-value"));
  }

  @Test
  public void shouldEnsureFunctionReturnTypeIsOptional() throws Exception {
    // Given:
    assertThat("Invalid test: return type must be primitive",
        SomeFunctionUdf.class
            .getDeclaredMethod("foo", String.class, String.class, String.class)
            .getReturnType(),
        equalTo(int.class));
    final List<Schema> args = ImmutableList.of(
        Schema.STRING_SCHEMA,
        Schema.STRING_SCHEMA,
        Schema.STRING_SCHEMA);

    // Then:
    final KsqlFunction someFunction = FUNC_REG
        .getUdfFactory("SomeFunction")
        .getFunction(args);

    assertThat(someFunction.getReturnType(args).isOptional(), is(true));
  }

  @Test
  public void shouldEnsureFunctionReturnTypeIsDeepOptional() {
    final List<Schema> args = Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA);
    final KsqlFunction complexFunction = FUNC_REG
        .getUdfFactory("ComplexFunction")
        .getFunction(args);

    assertThat(complexFunction.getReturnType(args), is(
        SchemaBuilder
            .struct()
            .field("F0", SchemaBuilder
                .struct()
                .field("F1", Schema.OPTIONAL_INT32_SCHEMA)
                .optional()
                .build())
            .optional()
            .build()));
  }

  private static FunctionRegistry initializeFunctionRegistry(
      final boolean loadCustomUdfs,
      final Optional<Metrics> metrics
  ) {
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final UdfLoader pluginLoader = createUdfLoader(functionRegistry, loadCustomUdfs, metrics);
    pluginLoader.load();
    return functionRegistry;
  }

  private static UdfLoader createUdfLoader(
      final MutableFunctionRegistry functionRegistry,
      final boolean loadCustomerUdfs,
      final Optional<Metrics> metrics
  ) {
    return new UdfLoader(functionRegistry,
        new File("src/test/resources"),
        PARENT_CLASS_LOADER,
        value -> false,
        COMPILER,
        metrics,
        loadCustomerUdfs
    );
  }

  private static ClassLoader getActualUdfClassLoader(final Kudf udf) throws Exception {
    final Field actualUdf = PluggableUdf.class.getDeclaredField("actualUdf");
    actualUdf.setAccessible(true);
    return actualUdf.get(udf).getClass().getClassLoader();
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"}) // Invoked via reflection in test.
  public static class UdfWithMissingDescriptionAnnotation {
    @Udf(description = "This invalid UDF is here to test that the loader does not blow up if badly"
        + " formed UDFs are in the class path.")
    public String something(final String value) {
      return null;
    }
  }

  private static Map<String, ?> PASSED_CONFIG = null;

  @SuppressWarnings({"unused", "MethodMayBeStatic"}) // Invoked via reflection in test.
  @UdfDescription(
      name = "ConfigurableUdf",
      description = "A test-only UDF for testing configure() is called")
  public static class ConfigurableUdf implements Configurable {
    @Override
    public void configure(final Map<String, ?> map) {
      PASSED_CONFIG = map;
    }

    @Udf
    public int foo(final int bar) {
      return bar;
    }
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"}) // Invoked via reflection in test.
  @UdfDescription(
      name = "SomeFunction",
      description = "A test-only UDF for testing 'UdfParameter'")
  public static class SomeFunctionUdf {
    @Udf
    public int foo(
        @UdfParameter("justValue") final String v0,
        @UdfParameter(value = "valueAndDescription", description = "Some description") final String v1,
        @UdfParameter final String noValue) {
      return 0;
    }
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"}) // Invoked via reflection in test.
  @UdfDescription(
      name = "ComplexFunction",
      description = "A test-only UDF that uses the 'schema' parameter")
  public static class ComplexUdf {

    @Udf(schema = "STRUCT<f0 STRUCT<f1 INT>>")
    public Object foo(final String noValue) {
      return 0;
    }
  }
}