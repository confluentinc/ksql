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

import io.confluent.ksql.execution.function.UdfUtil;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.matchprocessor.ClassAnnotationMatchProcessor;
import io.github.lukehutch.fastclasspathscanner.matchprocessor.MethodAnnotationMatchProcessor;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class UdfLoader {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOGGER = LoggerFactory.getLogger(UdfLoader.class);
  private static final String UDF_METRIC_GROUP = "ksql-udf";

  private final MutableFunctionRegistry functionRegistry;
  private final File pluginDir;
  private final ClassLoader parentClassLoader;
  private final Predicate<String> blacklist;
  private final UdfCompiler udfCompiler;
  private final UdafCompiler udafCompiler;
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;
  private final boolean loadCustomerUdfs;
  private final SqlTypeParser typeParser;


  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  UdfLoader(
      final MutableFunctionRegistry functionRegistry,
      final File pluginDir,
      final ClassLoader parentClassLoader,
      final Predicate<String> blacklist,
      final UdfCompiler udfCompiler,
      final UdafCompiler udafCompiler,
      final Optional<Metrics> metrics,
      final boolean loadCustomerUdfs
  ) {
    this.functionRegistry = Objects
        .requireNonNull(functionRegistry, "functionRegistry can't be null");
    this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir can't be null");
    this.parentClassLoader = Objects.requireNonNull(parentClassLoader,
        "parentClassLoader can't be null");
    this.blacklist = Objects.requireNonNull(blacklist, "blacklist can't be null");
    this.udfCompiler = Objects.requireNonNull(udfCompiler, "compiler can't be null");
    this.udafCompiler = Objects.requireNonNull(udafCompiler, "compiler can't be null");
    this.metrics = Objects.requireNonNull(metrics, "metrics can't be null");
    this.loadCustomerUdfs = loadCustomerUdfs;
    this.typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
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

  // Does not handle customer udfs, i.e the loader is the ParentClassLoader and path is internal
  void loadUdfFromClass(final Class<?> ... udfClass) {
    for (final Class<?> theClass: udfClass) {
      //classes must be annotated with @UdfDescription
      final UdfDescription udfDescription = theClass.getAnnotation(UdfDescription.class);
      if (udfDescription == null) {
        throw new KsqlException(String.format("Cannot load class %s. Classes containing UDFs must"
            + "be annotated with @UdfDescription.", theClass.getName()));
      }
      //method must be public and annotated with @Udf
      for (Method m: theClass.getDeclaredMethods()) {
        if (m.isAnnotationPresent(Udf.class) && Modifier.isPublic(m.getModifiers())) {
          handleUdfAnnotation(theClass,
                              udfDescription,
                              m,
                              parentClassLoader,
                              KsqlFunction.INTERNAL_PATH) ;
        }
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
        .matchClassesWithMethodAnnotation(Udf.class,
                                          processMethodAnnotation(loader, pathLoadedFrom))
        .matchClassesWithAnnotation(UdafDescription.class,
            handleUdafAnnotation(loader, pathLoadedFrom))
        .scan();
  }

  private ClassAnnotationMatchProcessor handleUdafAnnotation(final ClassLoader loader,
                                                             final String path
  ) {
    return (theClass) ->  {
      final UdafDescription udafAnnotation = theClass.getAnnotation(UdafDescription.class);
      final List<KsqlAggregateFunction<?, ?, ?>> aggregateFunctions
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
              return Optional.of(udafCompiler.compileAggregate(method,
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
            return Optional.<KsqlAggregateFunction<?, ?, ?>>empty();
          }).filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toList());

      functionRegistry.addAggregateFunctionFactory(new UdafAggregateFunctionFactory(
          new UdfMetadata(udafAnnotation.name(),
              udafAnnotation.description(),
              udafAnnotation.author(),
              udafAnnotation.version(),
              path,
              false),
              aggregateFunctions));
    };
  }

  private MethodAnnotationMatchProcessor processMethodAnnotation(final ClassLoader loader,
                                                                 final String path) {
    return (theClass, executable) ->  {
      final UdfDescription annotation = theClass.getAnnotation(UdfDescription.class);
      if (annotation == null) {
        LOGGER.warn("Ignoring method annotated with @Udf but missing @UdfDescription. "
            + "method='{}' from path={}", executable.getName(), path);
        return;
      }

      LOGGER.info("Adding UDF name='{}' from path={}", annotation.name(), path);
      final Method method = (Method) executable;
      try {
        handleUdfAnnotation(theClass, annotation, method, loader, path);
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
    };
  }

  private void handleUdfAnnotation(final Class<?> theClass,
                                   final UdfDescription classLevelAnnotation,
                                   final Method method,
                                   final ClassLoader classLoader,
                                   final String path) {

    LOGGER.info("Adding UDF name='{}' from class={}", classLevelAnnotation.name(), theClass);

    // sanity check
    instantiateUdfClass(method, classLevelAnnotation);
    final Udf udfAnnotation = method.getAnnotation(Udf.class);
    final String functionName = classLevelAnnotation.name();
    final String sensorName = "ksql-udf-" + functionName;

    @SuppressWarnings("unchecked")
    final Class<? extends Kudf> udfClass = metrics
        .map(m -> (Class)UdfMetricProducer.class)
        .orElse(PluggableUdf.class);

    addSensor(sensorName, functionName);

    LOGGER.info("Adding function " + functionName + " for method " + method);
    functionRegistry.ensureFunctionFactory(new UdfFactory(
        udfClass,
        new UdfMetadata(functionName,
                        classLevelAnnotation.description(),
                        classLevelAnnotation.author(),
                        classLevelAnnotation.version(),
                        path,
                        false)));

    final Schema javaReturnSchema = getReturnType(method, udfAnnotation);

    final Function<List<Schema>, Schema> returnSchema = handleUdfReturnSchema(
        theClass,
        javaReturnSchema,
        udfAnnotation,
        classLevelAnnotation);

    functionRegistry.addFunction(udfCompiler.compileFunction(
        method,
        classLevelAnnotation.name(),
        classLoader,
        udfClass,
        path,
        sensorName,
        returnSchema));
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

  private static Object instantiateUdfClass(final Class udfClass,
                                            final UdfDescription annotation) {
    try {
      return udfClass.newInstance();
    } catch (final Exception e) {
      throw new KsqlException("Failed to create instance for UDF="
          + annotation.name(), e);
    }
  }

  private static Function<List<Schema>, Schema> handleUdfReturnSchema(
      final Class theClass,
      final Schema javaReturnSchema,
      final Udf udfAnnotation,
      final UdfDescription descAnnotation
  ) {
    final String schemaProviderName = udfAnnotation.schemaProvider();

    if (!schemaProviderName.equals("")) {
      return handleUdfSchemaProviderAnnotation(schemaProviderName, theClass, descAnnotation);
    } else if (DecimalUtil.isDecimal(javaReturnSchema)) {
      throw new KsqlException(String.format("Cannot load UDF %s. BigDecimal return type "
          + "is not supported without a schema provider method.", descAnnotation.name()));
    }

    return ignored -> javaReturnSchema;
  }

  private static Function<List<Schema>, Schema> handleUdfSchemaProviderAnnotation(
      final String schemaProviderName,
      final Class theClass,
      final UdfDescription annotation
  ) {
    // throws exception if cannot find method
    final Method m = findSchemaProvider(theClass, schemaProviderName);
    final Object instance = instantiateUdfClass(theClass, annotation);

    return parameterSchemas -> {
      final List<SqlType> parameterTypes = parameterSchemas.stream()
          .map(p -> SchemaConverters.connectToSqlConverter().toSqlType(p))
          .collect(Collectors.toList());
      return SchemaConverters.sqlToConnectConverter().toConnectSchema(invokeSchemaProviderMethod(
          instance, m, parameterTypes, annotation));
    };
  }

  private static Method findSchemaProvider(
      final Class<?> theClass,
      final String schemaProviderName
  ) {
    try {
      final Method m = theClass.getDeclaredMethod(schemaProviderName, List.class);
      if (!m.isAnnotationPresent(UdfSchemaProvider.class)) {
        throw new KsqlException(String.format(
            "Method %s should be annotated with @UdfSchemaProvider.",
            schemaProviderName));
      }
      return m;
    } catch (NoSuchMethodException e) {
      throw new KsqlException(String.format(
          "Cannot find schema provider method with name %s and parameter List<SqlType> in class "
              + "%s.", schemaProviderName,theClass.getName()),e);
    }
  }

  private static SqlType invokeSchemaProviderMethod(
      final Object instance,
      final Method m,
      final List<SqlType> args,
      final UdfDescription annotation
  ) {
    try {
      return (SqlType) m.invoke(instance, args);
    } catch (IllegalAccessException
        | InvocationTargetException e) {
      throw new KsqlException(String.format("Cannot invoke the schema provider "
              + "method %s for UDF %s. ",
          m.getName(), annotation.name()), e);
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
            new WindowedCount());
        sensor.add(metrics.metricName(sensorName + "-rate", UDF_METRIC_GROUP,
            "The average number of occurrence of " + udfName + " operation per second "
                + udfName + " udf"),
            new Rate(TimeUnit.SECONDS, new WindowedCount()));
      }
    });
  }

  public static UdfLoader newInstance(final KsqlConfig config,
                                      final MutableFunctionRegistry metaStore,
                                      final String ksqlInstallDir) {
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
                         new UdafCompiler(metrics),
                         metrics,
                         loadCustomerUdfs);
  }

  private Schema getReturnType(final Method method, final Udf udfAnnotation) {
    try {
      final Schema returnType = udfAnnotation.schema().isEmpty()
          ? UdfUtil.getSchemaFromType(method.getGenericReturnType())
          : SchemaConverters
              .sqlToConnectConverter()
              .toConnectSchema(
                  typeParser.parse(udfAnnotation.schema()).getSqlType());

      return SchemaUtil.ensureOptional(returnType);
    } catch (final KsqlException e) {
      throw new KsqlException("Could not load UDF method with signature: " + method, e);
    }
  }
}
