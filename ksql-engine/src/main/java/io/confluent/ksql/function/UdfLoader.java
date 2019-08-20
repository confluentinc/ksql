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

import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.TypeContextUtil;
import io.confluent.ksql.security.ExtensionSecurityManager;
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
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.Time;
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
  private final UdfCompiler compiler;
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;
  private final boolean loadCustomerUdfs;


  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  UdfLoader(
      final MutableFunctionRegistry functionRegistry,
      final File pluginDir,
      final ClassLoader parentClassLoader,
      final Predicate<String> blacklist,
      final UdfCompiler compiler,
      final Optional<Metrics> metrics,
      final boolean loadCustomerUdfs
  ) {
    this.functionRegistry = Objects
        .requireNonNull(functionRegistry, "functionRegistry can't be null");
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

  // Does not handle customer udfs, i.e the loader is the ParentClassLoader and path is internal
  public void loadUdfFromClass(final Class<?> ... udfClass) {
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
                  annotation.description(),
                  annotation.paramSchema(),
                  annotation.returnSchema()
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
                                   final UdfDescription annotation,
                                   final Method method,
                                   final ClassLoader classLoader,
                                   final String path) {

    LOGGER.info("Adding UDF name='{}' from class={}", annotation.name(), theClass);
    final UdfInvoker udf = UdfCompiler.compile(method, classLoader);
    addFunction(theClass, annotation, method, udf, path);
  }


  private void addFunction(final Class theClass,
      final UdfDescription classLevelAnnotation,
      final Method method,
      final UdfInvoker udf,
      final String path) {
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
    functionRegistry.ensureFunctionFactory(new UdfFactory(udfClass,
        new UdfMetadata(functionName,
            classLevelAnnotation.description(),
            classLevelAnnotation.author(),
            classLevelAnnotation.version(),
            path,
            false)));

    final List<Schema> parameters = IntStream.range(0, method.getParameterCount()).mapToObj(idx -> {
      final Type type = method.getGenericParameterTypes()[idx];
      final Optional<UdfParameter> annotation = Arrays.stream(method.getParameterAnnotations()[idx])
          .filter(UdfParameter.class::isInstance)
          .map(UdfParameter.class::cast)
          .findAny();

      final Parameter param = method.getParameters()[idx];
      final String name = annotation.map(UdfParameter::value)
          .filter(val -> !val.isEmpty())
          .orElse(param.isNamePresent() ? param.getName() : "");

      if (name.trim().isEmpty()) {
        throw new KsqlFunctionException(
            String.format("Cannot resolve parameter name for param at index %d for udf %s:%s. "
                    + "Please specify a name in @UdfParameter or compile your JAR with -parameters "
                    + "to infer the name from the parameter name.",
                idx, classLevelAnnotation.name(), method.getName()));
      }

      final String doc = annotation.map(UdfParameter::description).orElse("");
      if (annotation.isPresent() && !annotation.get().schema().isEmpty()) {
        return SchemaConverters.sqlToConnectConverter()
            .toConnectSchema(
                TypeContextUtil.getType(annotation.get().schema()).getSqlType(),
                name,
                doc);
      }

      return UdfUtil.getSchemaFromType(type, name, doc);
    }).collect(Collectors.toList());


    functionRegistry.addFunction(KsqlFunction.create(
        handleUdfReturnSchema(theClass, method, udfAnnotation, classLevelAnnotation),
        parameters,
        functionName,
        udfClass,
        ksqlConfig -> {
          final Object actualUdf = instantiateUdfClass(method, classLevelAnnotation);
          if (actualUdf instanceof Configurable) {
            ((Configurable)actualUdf)
                .configure(ksqlConfig.getKsqlFunctionsConfigProps(functionName));
          }
          final PluggableUdf theUdf = new PluggableUdf(udf, actualUdf, method);
          return metrics.<Kudf>map(m -> new UdfMetricProducer(m.getSensor(sensorName),
              theUdf,
              Time.SYSTEM)).orElse(theUdf);
        }, udfAnnotation.description(),
        path,
        method.isVarArgs()));
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

  private Function<List<Schema>,Schema> handleUdfReturnSchema(
      final Class theClass,
      final Method method,
      final Udf udfAnnotation,
      final UdfDescription descAnnotation) {

    final String schemaProviderName = method.getAnnotation(Udf.class).schemaProvider();

    if (!schemaProviderName.equals("")) {
      return handleUdfSchemaProviderAnnotation(schemaProviderName, theClass, method,
          descAnnotation);
    }

    final Schema returnSchema = getReturnType(method, udfAnnotation);
    if (returnSchema.name() != null
        && returnSchema.name().equals("org.apache.kafka.connect.data.Decimal")
        && schemaProviderName.equals("")) {
      throw new KsqlFunctionException("BigDecimal return type is not supported "
          + "without a schema provider method.");
    }

    return ignored -> returnSchema;

  }

  private Function<List<Schema>,Schema> handleUdfSchemaProviderAnnotation(
      final String schemaProviderName,
      final Class theClass,
      final Method method,
      final UdfDescription annotation) {

    Function<List<Schema>, Schema> schemaProviderFunction = null;

    final List<Method> allMethods = new ArrayList<Method>(
        Arrays.asList(theClass.getDeclaredMethods()));

    // throws exception if cannot find method
    final Method m = findSchemaProvider(allMethods, schemaProviderName);
    final Object instance = instantiateUdfClass(theClass, annotation);

    schemaProviderFunction = list -> {
      return invokeSchemaProviderMethod(instance, m, list, annotation);
    };

    return schemaProviderFunction;
  }

  private Method findSchemaProvider(final List<Method> allMethods,
      final String schemaProviderName) {
    for (final Method m : allMethods) {
      if (m.isAnnotationPresent(UdfSchemaProvider.class)) {
        if (m.getName().equals(schemaProviderName)) {
          return m;
        }
      }
    }

    throw new KsqlFunctionException(String.format(
        "Cannot find schema provider method with name %s. Please provide a method "
            + "declaration when using the annotation schemaProvider for a udf.",
        schemaProviderName));

  }

  private Schema invokeSchemaProviderMethod(final Object instance, final Method m,
      final List<Schema> args, final UdfDescription annotation) {
    try {
      return (Schema) m.invoke(instance,args);
    } catch (IllegalAccessException
        | InvocationTargetException e) {
      throw new KsqlFunctionException(String.format("Cannot invoke the schema provider "
              + "method %s for udf %s. ",
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

  private static Schema getReturnType(final Method method, final Udf udfAnnotation) {
    try {
      final Schema returnType = udfAnnotation.schema().isEmpty()
          ? UdfUtil.getSchemaFromType(method.getGenericReturnType())
          : SchemaConverters
              .sqlToConnectConverter()
              .toConnectSchema(TypeContextUtil.getType(udfAnnotation.schema()).getSqlType());

      return SchemaUtil.ensureOptional(returnType);
    } catch (final KsqlException e) {
      throw new KsqlException("Could not load UDF method with signature: " + method, e);
    }
  }
}
