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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * This uses ksql-engine/src/test/resource/udf-example.jar to load the custom jars.
 * You can find the classes it is loading in the same directory
 */
public class UdfLoaderTest {

  private final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
  private final UdfCompiler compiler = new UdfCompiler();
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
        Arrays.asList(Schema.Type.STRING, Schema.Type.INT32)).newInstance();
    assertThat(substring1.evaluate("foo", 1), equalTo("oo"));

    final Kudf substring2 = function.getFunction(
        Arrays.asList(Schema.Type.STRING, Schema.Type.INT32, Schema.Type.INT32)).newInstance();
    assertThat(substring2.evaluate("foo", 1,2), equalTo("o"));
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


    final Kudf toStringUdf = toString.getFunction(Collections.singletonList(Schema.Type.STRING))
        .newInstance();
    final Kudf multiplyUdf = multiply.getFunction(Arrays.asList(Schema.Type.INT32, Schema.Type.INT32))
        .newInstance();

    final ClassLoader multiplyLoader = getActualUdfClassLoader(multiplyUdf);
    assertThat(multiplyLoader, equalTo(getActualUdfClassLoader(toStringUdf)));
    assertThat(multiplyLoader, not(equalTo(parentClassLoader)));
  }

  @Test
  public void shouldPutKsqlFunctionsInParentClassLoader()
      throws NoSuchFieldException, IllegalAccessException {
    final UdfFactory substring = metaStore.getUdfFactory("substring");
    final Kudf kudf = substring.getFunction(
        Arrays.asList(Schema.Type.STRING, Schema.Type.INT32))
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
  public void shouldNotLoadUdfsInJarDirectoryIfLoadCustomerUdfsFalse() {
    final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    final UdfLoader pluginLoader = createUdfLoader(metaStore, false, false);
    pluginLoader.load();
    // udf in ksql-engine
    final UdfFactory function = metaStore.getUdfFactory("substring");
    // udf in udf-example.jar
    final UdfFactory toString = metaStore.getUdfFactory("tostring");

    assertThat(function, not(nullValue()));
    assertThat(toString, nullValue());
    assertThat(toString, nullValue());
  }

  @Test
  public void shouldCollectMetricsWhenMetricCollectionEnabled() {
    final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    final UdfLoader pluginLoader = createUdfLoader(metaStore, true, true);

    pluginLoader.load();
    final UdfFactory substring = metaStore.getUdfFactory("substring");
    final KsqlFunction function
        = substring.getFunction(Arrays.asList(Schema.Type.STRING, Schema.Type.INT32));
    final Kudf kudf = function.newInstance();
    assertThat(kudf, instanceOf(UdfMetricProducer.class));
    final Sensor sensor = metrics.getSensor("ksql-udf-substring");
    assertThat(sensor, not(nullValue()));
    assertThat(metrics.metric(metrics.metricName("ksql-udf-substring-count", "ksql-udf-substring")),
        not(nullValue()));
    assertThat(metrics.metric(metrics.metricName("ksql-udf-substring-max", "ksql-udf-substring")),
        not(nullValue()));
    assertThat(metrics.metric(metrics.metricName("ksql-udf-substring-avg", "ksql-udf-substring")),
        not(nullValue()));
    assertThat(metrics.metric(metrics.metricName("ksql-udf-substring-rate", "ksql-udf-substring")),
        not(nullValue()));
  }

  private UdfLoader createUdfLoader(final MetaStore metaStore,
                                    final boolean loadCustomerUdfs,
                                    final boolean collectMetrics) {
    return new UdfLoader(metaStore,
        new File("src/test/resources"),
        parentClassLoader,
        value -> false,
        compiler,
        metrics, loadCustomerUdfs, collectMetrics);
  }

}