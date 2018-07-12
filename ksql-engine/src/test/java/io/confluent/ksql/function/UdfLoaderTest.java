/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * This uses ksql-engine/src/test/resource/udf-example.jar to load the custom jars.
 * You can find the classes it is loading in the same directory
 */
public class UdfLoaderTest {

  private final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
  private final UdfCompiler compiler = new UdfCompiler(Optional.empty());
  private final ClassLoader parentClassLoader = UdfLoaderTest.class.getClassLoader();
  private final Metrics metrics = new Metrics();
  private final UdfLoader pluginLoader = createUdfLoader(metaStore, true, false);

  @Before
  public void before() {
    pluginLoader.load();
  }

  @Test
  public void shouldLoadFunctionsInKsqlEngine() {
    final UdfFactory function = metaStore.getUdfFactory("substring");
    assertThat(function, not(nullValue()));

    final Kudf substring1 = function.getFunction(
        Arrays.asList(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)).newInstance();
    assertThat(substring1.evaluate("foo", 1), equalTo("oo"));

    final Kudf substring2 = function.getFunction(
        Arrays.asList(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)).newInstance();
    assertThat(substring2.evaluate("foo", 1,2), equalTo("o"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldLoadUdafs() {
    final KsqlAggregateFunction aggregate
        = metaStore.getAggregate("test_udaf", Schema.OPTIONAL_INT64_SCHEMA);
    final KsqlAggregateFunction<Long, Long> instance = aggregate.getInstance(
        new AggregateFunctionArguments(Collections.singletonMap("udfIndex", 0),
            Collections.singletonList("udfIndex")));
    assertThat(instance.getInitialValueSupplier().get(), equalTo(0L));
    assertThat(instance.aggregate(1L, 1L), equalTo(2L));
    assertThat(instance.getMerger().apply("k", 2L, 3L), equalTo(5L));
  }

  @Test
  public void shouldLoadFunctionsFromJarsInPluginDir() {
    final UdfFactory toString = metaStore.getUdfFactory("tostring");
    final UdfFactory multi = metaStore.getUdfFactory("multiply");
    assertThat(toString, not(nullValue()));
    assertThat(multi, not(nullValue()));
  }

  @Test
  public void shouldPutJarUdfsInClassLoaderForJar()
      throws NoSuchFieldException, IllegalAccessException {
    final UdfFactory toString = metaStore.getUdfFactory("tostring");
    final UdfFactory multiply = metaStore.getUdfFactory("multiply");


    final Kudf toStringUdf = toString.getFunction(Collections.singletonList(Schema.STRING_SCHEMA))
        .newInstance();
    final Kudf multiplyUdf = multiply.getFunction(
        Arrays.asList(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA))
        .newInstance();

    final ClassLoader multiplyLoader = getActualUdfClassLoader(multiplyUdf);
    assertThat(multiplyLoader, equalTo(getActualUdfClassLoader(toStringUdf)));
    assertThat(multiplyLoader, not(equalTo(parentClassLoader)));
  }

  @Test
  public void shouldCreateUdfFactoryWithJarPathWhenExternal() {
    final UdfFactory tostring = metaStore.getUdfFactory("tostring");
    assertThat(tostring.getPath(), equalTo("src/test/resources/udf-example.jar"));
  }

  @Test
  public void shouldCreateUdfFactoryWithInternalPathWhenInternal() {
    final UdfFactory substring = metaStore.getUdfFactory("substring");
    assertThat(substring.getPath(), equalTo(KsqlFunction.INTERNAL_PATH));
  }

  @Test
  public void shouldPutKsqlFunctionsInParentClassLoader()
      throws NoSuchFieldException, IllegalAccessException {
    final UdfFactory substring = metaStore.getUdfFactory("substring");
    final Kudf kudf = substring.getFunction(
        Arrays.asList(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
        .newInstance();
    assertThat(getActualUdfClassLoader(kudf), equalTo(parentClassLoader));
  }

  private ClassLoader getActualUdfClassLoader(final Kudf udf)
      throws NoSuchFieldException, IllegalAccessException {
    final Field actualUdf = PluggableUdf.class.getDeclaredField("actualUdf");
    actualUdf.setAccessible(true);
    try {
      return actualUdf.get(udf).getClass().getClassLoader();
    } finally{
      actualUdf.setAccessible(false);
    }
  }

  @Test
  public void shouldLoadUdfsInKSQLIfLoadCustomerUdfsFalse() {
    final MetaStore metaStore = loadKsqlUdfsOnly();
    // udf in ksql-engine will throw if not found
    metaStore.getUdfFactory("substring");
  }

  @Test
  public void shouldNotLoadCustomUDfsIfLoadCustomUdfsFalse() {
    final MetaStore metaStore = loadKsqlUdfsOnly();
    // udf in udf-example.jar
    try {
      metaStore.getUdfFactory("tostring");
      fail("Should have thrown as function doesn't exist");
    } catch (final KsqlException e) {
      // pass
    }
  }

  private MetaStore loadKsqlUdfsOnly() {
    final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    final UdfLoader pluginLoader = createUdfLoader(metaStore, false, false);
    pluginLoader.load();
    return metaStore;
  }

  @Test
  public void shouldCollectMetricsWhenMetricCollectionEnabled() {
    final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    final UdfLoader pluginLoader = createUdfLoader(metaStore, true, true);

    pluginLoader.load();
    final UdfFactory substring = metaStore.getUdfFactory("substring");
    final KsqlFunction function
        = substring.getFunction(Arrays.asList(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA));
    final Kudf kudf = function.newInstance();
    assertThat(kudf, instanceOf(UdfMetricProducer.class));
    final Sensor sensor = metrics.getSensor("ksql-udf-substring");
    assertThat(sensor, not(nullValue()));
    assertThat(metrics.metric(metrics.metricName("ksql-udf-substring-count", "ksql-udf")),
        not(nullValue()));
    assertThat(metrics.metric(metrics.metricName("ksql-udf-substring-max", "ksql-udf")),
        not(nullValue()));
    assertThat(metrics.metric(metrics.metricName("ksql-udf-substring-avg", "ksql-udf")),
        not(nullValue()));
    assertThat(metrics.metric(metrics.metricName("ksql-udf-substring-rate", "ksql-udf")),
        not(nullValue()));
  }

  @Test
  public void shouldUseConfigForExtDir() {
    final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    // The tostring function is in the udf-example.jar that is found in src/test/resources
    final ImmutableMap<Object, Object> configMap
        = ImmutableMap.builder().put(KsqlConfig.KSQL_EXT_DIR, "src/test/resources")
        .put(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED, false)
        .build();
    final KsqlConfig config
        = new KsqlConfig(configMap);
    UdfLoader.newInstance(config, metaStore, "").load();
    // will throw if it doesn't exist
    metaStore.getUdfFactory("tostring");
  }

  @Test
  public void shouldNotThrowWhenExtDirDoesntExist() {
    final ImmutableMap<Object, Object> configMap
        = ImmutableMap.builder().put(KsqlConfig.KSQL_EXT_DIR, "foo/bar")
        .put(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED, false)
        .build();
    final KsqlConfig config
        = new KsqlConfig(configMap);
    UdfLoader.newInstance(config, new MetaStoreImpl(new InternalFunctionRegistry()), "").load();
  }

  private UdfLoader createUdfLoader(final MetaStore metaStore,
                                    final boolean loadCustomerUdfs,
                                    final boolean collectMetrics) {
    final Optional<Metrics> optionalMetrics = collectMetrics
        ? Optional.of(metrics)
        : Optional.empty();
    return new UdfLoader(metaStore,
        new File("src/test/resources"),
        parentClassLoader,
        value -> false,
        compiler,
        optionalMetrics,
        loadCustomerUdfs);
  }

}
