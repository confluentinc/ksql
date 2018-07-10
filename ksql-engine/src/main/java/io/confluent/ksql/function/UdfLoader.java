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
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.confluent.ksql.function.udaf.UdafAggregateFunctionFactory;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.matchprocessor.ClassAnnotationMatchProcessor;
import io.github.lukehutch.fastclasspathscanner.matchprocessor.MethodAnnotationMatchProcessor;


public class UdfLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(UdfLoader.class);
  private static final String UDF_METRIC_GROUP = "ksql-udf";

  private final MetaStore metaStore;
  private final File pluginDir;
  private final ClassLoader parentClassLoader;
  private final Predicate<String> blacklist;
  private final UdfCompiler compiler;
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;
  private final boolean loadCustomerUdfs;


  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public UdfLoader(final MetaStore metaStore,
                   final File pluginDir,
                   final ClassLoader parentClassLoader,
                   final Predicate<String> blacklist,
                   final UdfCompiler compiler,
                   final Optional<Metrics> metrics,
                   final boolean loadCustomerUdfs) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore can't be null");
    this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir can't be null");
    this.parentClassLoader = Objects.requireNonNull(parentClassLoader,
        "parentClassLoader can't be null");
    this.blacklist = Objects.requireNonNull(blacklist, "blacklist can't be null");
    this.compiler = Objects.requireNonNull(compiler, "compiler can't be null");
    this.metrics = Objects.requireNonNull(metrics, "metrics can't be null");
    this.loadCustomerUdfs = loadCustomerUdfs;
  }

  public void load() {
    // load udfs packaged as part of ksql first
    loadUdfs(parentClassLoader, Optional.empty());
    if (loadCustomerUdfs) {
      try {
        if (!pluginDir.exists() && !pluginDir.isDirectory()) {
          LOGGER.info("UDFs can't be loaded as as dir {} doesn't exist or is not a directory",
              pluginDir);
          return;
        }
        Files.find(pluginDir.toPath(), 1, (path, attributes) -> path.toString().endsWith(".jar"))
            .map(path -> UdfClassLoader.newClassLoader(path, parentClassLoader, blacklist))
            .forEach(classLoader -> loadUdfs(classLoader, Optional.of(classLoader.getJarPath())));
      } catch (final IOException e) {
        LOGGER.error("Failed to load UDFs from location {}", pluginDir, e);
      }
    }
  }


  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private void loadUdfs(final ClassLoader loader, final Optional<Path> path) {
    final String pathLoadedFrom
        = path.map(Path::toString).orElse(KsqlFunction.INTERNAL_PATH);
    final FastClasspathScanner fastClasspathScanner = new FastClasspathScanner();
    if (loader != parentClassLoader) {
      fastClasspathScanner.overrideClassLoaders(loader);
    }
    fastClasspathScanner
        .ignoreParentClassLoaders()
        // if we are loading from the parent classloader then restrict the name space to only
        // jars/dirs containing "ksql-engine". This is so we don't end up scanning every jar
        .filterClasspathElements(
            name -> {
              if (parentClassLoader != loader) {
                return true;
              }
              return name.contains("ksql-engine");
            })
        .matchClassesWithMethodAnnotation(Udf.class, handleUdfAnnotation(loader, pathLoadedFrom))
        .matchClassesWithAnnotation(UdafDescription.class,
            handleUdafAnnotation(loader, pathLoadedFrom))
        .scan();
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private ClassAnnotationMatchProcessor handleUdafAnnotation(final ClassLoader loader,
                                                             final String path
  ) {
    return (theClass) ->  {
      final UdafDescription udafAnnotation = theClass.getAnnotation(UdafDescription.class);
      final List<KsqlAggregateFunction<?, ?>> aggregateFunctions
          = Arrays.stream(theClass.getMethods())
          .filter(method -> method.getAnnotation(UdafFactory.class) != null)
          .filter(method -> {
            if (!Modifier.isStatic(method.getModifiers())) {
              LOGGER.warn("Trying to create a UDAF from a non-static factory method. Udaf factory"
                      + " methods must be static. class={}, method={}, name={}",
                  method.getDeclaringClass(),
                  method.getName(),
                  udafAnnotation.name());
              return false;
            }
            return true;
          })
          .map(method -> {
            final UdafFactory annotation = method.getAnnotation(UdafFactory.class);
            try {
              LOGGER.info("Adding UDAF name={} from path={} class={}",
                  udafAnnotation.name(),
                  path,
                  method.getDeclaringClass());
              return Optional.of(compiler.compileAggregate(method,
                  loader,
                  udafAnnotation.name(),
                  annotation.description()
              ));
            } catch (final Exception e) {
              LOGGER.warn("Failed to create UDAF name={}, method={}, class={}, path={}",
                  udafAnnotation.name(),
                  method.getName(),
                  method.getDeclaringClass(),
                  path,
                  e);
            }
            return Optional.<KsqlAggregateFunction<?, ?>>empty();
          }).filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toList());

      metaStore.addAggregateFunctionFactory(new UdafAggregateFunctionFactory(
          new UdfMetadata(udafAnnotation.name(),
              udafAnnotation.description(),
              udafAnnotation.author(),
              udafAnnotation.version(),
              path),
              aggregateFunctions));
    };
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private MethodAnnotationMatchProcessor handleUdfAnnotation(final ClassLoader loader,
                                                             final String path) {
    return (theClass, executable) ->  {
      final UdfDescription annotation = theClass.getAnnotation(UdfDescription.class);
      if (annotation != null) {
        LOGGER.info("Adding UDF name='{}' from path={}",
            annotation.name(),
            path);
        final Method method = (Method) executable;
        try {
          final UdfInvoker udf = compiler.compile(method, loader);
          addFunction(annotation, method, udf, path);
        } catch (final KsqlException e) {
          if (parentClassLoader == loader) {
            throw e;
          } else {
            LOGGER.warn("Failed to add UDF to the MetaStore. name={} method={}",
                annotation.name(),
                method,
                e);
          }
        }
      }
    };
  }

  private void addFunction(final UdfDescription classLevelAnnotaion,
                           final Method method,
                           final UdfInvoker udf,
                           final String path) {
    // sanity check
    instantiateUdfClass(method, classLevelAnnotaion);
    final Udf udfAnnotation = method.getAnnotation(Udf.class);
    final String sensorName = "ksql-udf-" + classLevelAnnotaion.name();

    @SuppressWarnings("unchecked")
    final Class<? extends Kudf> udfClass = metrics
        .map(m -> (Class)UdfMetricProducer.class)
        .orElse(PluggableUdf.class);
    addSensor(sensorName, classLevelAnnotaion.name());

    LOGGER.info("Adding function " + classLevelAnnotaion.name() + " for method " + method);
    metaStore.addFunctionFactory(new UdfFactory(udfClass,
        new UdfMetadata(classLevelAnnotaion.name(),
            classLevelAnnotaion.description(),
            classLevelAnnotaion.author(),
            classLevelAnnotaion.version(),
            path)));

    metaStore.addFunction(new KsqlFunction(
        SchemaUtil.getSchemaFromType(method.getReturnType()),
        Arrays.stream(method.getGenericParameterTypes())
            .map(SchemaUtil::getSchemaFromType).collect(Collectors.toList()),
        classLevelAnnotaion.name(),
        udfClass,
        () -> {
          final PluggableUdf theUdf
              = new PluggableUdf(udf, instantiateUdfClass(method, classLevelAnnotaion));
          return metrics.<Kudf>map(m -> new UdfMetricProducer(m.getSensor(sensorName),
              theUdf,
              Time.SYSTEM)).orElse(theUdf);
        }, udfAnnotation.description(),
        path));
  }

  private static Object instantiateUdfClass(final Method method,
                                     final UdfDescription annotation) {
    try {
      return method.getDeclaringClass().newInstance();
    } catch (final Exception e) {
      throw new KsqlException("Failed to create instance for UDF="
          + annotation.name()
          + ", method=" + method,
          e);
    }
  }

  private void addSensor(final String sensorName, final String udfName) {
    metrics.ifPresent(metrics -> {
      if (metrics.getSensor(sensorName) == null) {
        final Sensor sensor = metrics.sensor(sensorName);
        sensor.add(metrics.metricName(sensorName + "-avg", UDF_METRIC_GROUP,
            "Average time for an invocation of " + udfName + " udf"),
            new Avg());
        sensor.add(metrics.metricName(sensorName + "-max", UDF_METRIC_GROUP,
            "Max time for an invocation of " + udfName + " udf"),
            new Max());
        sensor.add(metrics.metricName(sensorName + "-count", UDF_METRIC_GROUP,
            "Total number of invocations of " + udfName + " udf"),
            new Count());
        sensor.add(metrics.metricName(sensorName + "-rate", UDF_METRIC_GROUP,
            "The average number of occurrence of " + udfName + " operation per second "
                + udfName + " udf"),
            new Rate(TimeUnit.SECONDS, new Count()));
      }
    });
  }

  public static UdfLoader newInstance(final KsqlConfig config,
                                      final MetaStore metaStore,
                                      final String ksqlInstallDir
  ) {
    final Boolean loadCustomerUdfs = config.getBoolean(KsqlConfig.KSQL_ENABLE_UDFS);
    final Boolean collectMetrics = config.getBoolean(KsqlConfig.KSQL_COLLECT_UDF_METRICS);
    final String extDirName = config.getString(KsqlConfig.KSQL_EXT_DIR);
    final File pluginDir = KsqlConfig.DEFAULT_EXT_DIR.equals(extDirName)
        ? new File(ksqlInstallDir, extDirName)
        : new File(extDirName);

    final Optional<Metrics> metrics = collectMetrics
        ? Optional.of(MetricCollectors.getMetrics())
        : Optional.empty();
    
    if (config.getBoolean(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED)) {
      System.setSecurityManager(ExtensionSecurityManager.INSTANCE);
    }
    return new UdfLoader(metaStore,
        pluginDir,
        Thread.currentThread().getContextClassLoader(),
        new Blacklist(new File(pluginDir, "resource-blacklist.txt")),
        new UdfCompiler(metrics),
        metrics,
        loadCustomerUdfs
    );
  }

}
