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

import static java.util.Optional.empty;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.execution.function.UdfUtil;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;
  private final boolean loadCustomerUdfs;
  private final SqlTypeParser typeParser;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public UdfLoader(
      final MutableFunctionRegistry functionRegistry,
      final File pluginDir,
      final ClassLoader parentClassLoader,
      final Predicate<String> blacklist,
      final Optional<Metrics> metrics,
      final boolean loadCustomerUdfs
  ) {
    this.functionRegistry = Objects
        .requireNonNull(functionRegistry, "functionRegistry can't be null");
    this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir can't be null");
    this.parentClassLoader = Objects.requireNonNull(
        parentClassLoader,
        "parentClassLoader can't be null"
    );
    this.blacklist = Objects.requireNonNull(blacklist, "blacklist can't be null");
    this.metrics = Objects.requireNonNull(metrics, "metrics can't be null");
    this.loadCustomerUdfs = loadCustomerUdfs;
    this.typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
  }

  public void load() {
    // load functions packaged as part of ksql first
    loadFunctions(parentClassLoader, empty());
    if (loadCustomerUdfs) {
      try {
        if (!pluginDir.exists() && !pluginDir.isDirectory()) {
          LOGGER.info(
              "UDFs can't be loaded as as dir {} doesn't exist or is not a directory",
              pluginDir
          );
          return;
        }
        Files.find(pluginDir.toPath(), 1,
            (path, attributes) -> path.toString().endsWith(".jar")
        )
            .map(path -> UdfClassLoader.newClassLoader(path, parentClassLoader, blacklist))
            .forEach(classLoader ->
                loadFunctions(classLoader, Optional.of(classLoader.getJarPath())));
      } catch (final IOException e) {
        LOGGER.error("Failed to load UDFs from location {}", pluginDir, e);
      }
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private void loadFunctions(final ClassLoader loader, final Optional<Path> path) {
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
        .matchClassesWithAnnotation(
            UdfDescription.class, theClass -> loadUdfFromClass(theClass, pathLoadedFrom, loader))
        .matchClassesWithAnnotation(
            UdafDescription.class, theClass -> loadUdafFromClass(theClass, pathLoadedFrom))
        .matchClassesWithAnnotation(
            UdtfDescription.class, theClass -> loadUdtfFromClass(theClass, pathLoadedFrom))
        .scan();
  }

  // Does not handle customer udfs, i.e the loader is the ParentClassLoader and path is internal
  // This method is only used from tests
  @VisibleForTesting
  void loadUdfFromClass(final Class<?>... udfClasses) {
    for (final Class<?> theClass : udfClasses) {
      loadUdfFromClass(
          theClass, KsqlFunction.INTERNAL_PATH, theClass.getClassLoader());
    }
  }

  private void loadUdfFromClass(
      final Class<?> theClass,
      final String path,
      final ClassLoader loader
  ) {
    final UdfDescription udfDescriptionAnnotation = theClass.getAnnotation(UdfDescription.class);
    if (udfDescriptionAnnotation == null) {
      throw new KsqlException(String.format("Cannot load class %s. Classes containing UDFs must"
          + "be annotated with @UdfDescription.", theClass.getName()));
    }
    final String functionName = udfDescriptionAnnotation.name();
    final String sensorName = "ksql-udf-" + functionName;
    @SuppressWarnings("unchecked") final Class<? extends Kudf> udfClass = metrics
        .map(m -> (Class) UdfMetricProducer.class)
        .orElse(PluggableUdf.class);
    addSensor(sensorName, functionName);

    final UdfFactory factory = new UdfFactory(
        udfClass,
        new UdfMetadata(
            udfDescriptionAnnotation.name(),
            udfDescriptionAnnotation.description(),
            udfDescriptionAnnotation.author(),
            udfDescriptionAnnotation.version(),
            path,
            false
        )
    );

    functionRegistry.ensureFunctionFactory(factory);

    Arrays.stream(theClass.getMethods())
        .filter(method -> method.getAnnotation(Udf.class) != null)
        .map(method -> {
          try {
            return Optional.of(createFunction(theClass, udfDescriptionAnnotation, method, path,
                sensorName, udfClass
            ));
          } catch (final KsqlException e) {
            if (parentClassLoader == loader) {
              // This only seems to be done for tests, dubious code
              throw e;
            } else {
              LOGGER.warn(
                  "Failed to add UDF to the MetaStore. name={} method={}",
                  udfDescriptionAnnotation.name(),
                  method,
                  e
              );
            }
          }
          return Optional.<KsqlFunction>empty();
        })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(factory::addFunction);
  }

  private KsqlFunction createFunction(
      final Class theClass,
      final UdfDescription udfDescriptionAnnotation,
      final Method method,
      final String path,
      final String sensorName,
      final Class<? extends Kudf> udfClass
  ) {
    // sanity check
    instantiateUdfClass(method, udfDescriptionAnnotation);
    final FunctionInvoker invoker = createFunctionInvoker(method);
    final Udf udfAnnotation = method.getAnnotation(Udf.class);
    final String functionName = udfDescriptionAnnotation.name();

    LOGGER.info("Adding function " + functionName + " for method " + method);

    final List<Schema> parameters = createParameters(method, functionName);

    final Schema javaReturnSchema = getReturnType(method, udfAnnotation.schema());

    return KsqlFunction.create(
        handleUdfReturnSchema(
            theClass,
            javaReturnSchema,
            udfAnnotation,
            udfDescriptionAnnotation
        ),
        javaReturnSchema,
        parameters,
        FunctionName.of(functionName.toUpperCase()),
        udfClass,
        ksqlConfig -> {
          final Object actualUdf = instantiateUdfClass(method, udfDescriptionAnnotation);
          if (actualUdf instanceof Configurable) {
            ((Configurable) actualUdf)
                .configure(ksqlConfig.getKsqlFunctionsConfigProps(functionName));
          }
          final PluggableUdf theUdf = new PluggableUdf(invoker, actualUdf);
          return metrics.<Kudf>map(m -> new UdfMetricProducer(
              m.getSensor(sensorName),
              theUdf,
              Time.SYSTEM
          )).orElse(theUdf);
        }, udfAnnotation.description(),
        path,
        method.isVarArgs()
    );
  }

  private List<Schema> createParameters(final Method method, final String functionName) {
    return IntStream.range(0, method.getParameterCount()).mapToObj(idx -> {
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
            String.format("Cannot resolve parameter name for param at index %d for UDF %s:%s. "
                    + "Please specify a name in @UdfParameter or compile your JAR with -parameters "
                    + "to infer the name from the parameter name.",
                idx, functionName, method.getName()
            ));
      }

      final String doc = annotation.map(UdfParameter::description).orElse("");
      if (annotation.isPresent() && !annotation.get().schema().isEmpty()) {
        return SchemaConverters.sqlToConnectConverter()
            .toConnectSchema(
                typeParser.parse(annotation.get().schema()).getSqlType(),
                name,
                doc
            );
      }

      return UdfUtil.getSchemaFromType(type, name, doc);
    }).collect(Collectors.toList());
  }

  @VisibleForTesting
  public static FunctionInvoker createFunctionInvoker(final Method method) {
    return new DynamicFunctionInvoker(method);
  }

  private void loadUdafFromClass(final Class<?> theClass, final String path) {
    final UdafDescription udafAnnotation = theClass.getAnnotation(UdafDescription.class);
    final List<UdafFactoryInvoker> argCreators
        = Arrays.stream(theClass.getMethods())
        .filter(method -> method.getAnnotation(UdafFactory.class) != null)
        .filter(method -> {
          if (!Modifier.isStatic(method.getModifiers())) {
            LOGGER.warn(
                "Trying to create a UDAF from a non-static factory method. Udaf factory"
                    + " methods must be static. class={}, method={}, name={}",
                method.getDeclaringClass(),
                method.getName(),
                udafAnnotation.name()
            );
            return false;
          }
          return true;
        })
        .map(method -> {
          final UdafFactory annotation = method.getAnnotation(UdafFactory.class);
          try {
            LOGGER.info(
                "Adding UDAF name={} from path={} class={}",
                udafAnnotation.name(),
                path,
                method.getDeclaringClass()
            );
            return Optional.of(createUdafFactoryInvoker(
                method,
                FunctionName.of(udafAnnotation.name()),
                annotation.description(),
                annotation.paramSchema(),
                annotation.aggregateSchema(),
                annotation.returnSchema()
            ));
          } catch (final Exception e) {
            LOGGER.warn(
                "Failed to create UDAF name={}, method={}, class={}, path={}",
                udafAnnotation.name(),
                method.getName(),
                method.getDeclaringClass(),
                path,
                e
            );
          }
          return Optional.<UdafFactoryInvoker>empty();
        }).filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());

    functionRegistry.addAggregateFunctionFactory(new UdafAggregateFunctionFactory(
        new UdfMetadata(
            udafAnnotation.name(),
            udafAnnotation.description(),
            udafAnnotation.author(),
            udafAnnotation.version(),
            path,
            false
        ),
        argCreators
    ));
  }


  UdafFactoryInvoker createUdafFactoryInvoker(
      final Method method,
      final FunctionName functionName,
      final String description,
      final String inputSchema,
      final String aggregateSchema,
      final String outputSchema
  ) {
    return new UdafFactoryInvoker(method, functionName, description, inputSchema,
        aggregateSchema, outputSchema, typeParser, metrics
    );
  }

  private void loadUdtfFromClass(
      final Class<?> theClass,
      final String path
  ) {
    final UdtfDescription udtfDescriptionAnnotation = theClass.getAnnotation(UdtfDescription.class);
    if (udtfDescriptionAnnotation == null) {
      throw new KsqlException(String.format("Cannot load class %s. Classes containing UDTFs must"
          + "be annotated with @UdtfDescription.", theClass.getName()));
    }
    final String functionName = udtfDescriptionAnnotation.name();
    final String sensorName = "ksql-udtf-" + functionName;
    addSensor(sensorName, functionName);

    final UdfMetadata metadata = new UdfMetadata(
        udtfDescriptionAnnotation.name(),
        udtfDescriptionAnnotation.description(),
        udtfDescriptionAnnotation.author(),
        udtfDescriptionAnnotation.version(),
        path,
        false
    );

    final UdtfTableFunctionFactory udtfFactory = new UdtfTableFunctionFactory(metadata);

    Arrays.stream(theClass.getMethods())
        .filter(method -> method.getAnnotation(Udtf.class) != null)
        .map(method -> {
          try {
            final Udtf annotation = method.getAnnotation(Udtf.class);
            if (method.getReturnType() != List.class) {
              throw new KsqlException(String
                  .format("UDTF functions must return a List. Class %s Method %s",
                      theClass.getName(), method.getName()
                  ));
            }
            final Type ret = method.getGenericReturnType();
            if (!(ret instanceof ParameterizedType)) {
              throw new KsqlException(String
                  .format("UDTF functions must return a generic List. Class %s Method %s",
                      theClass.getName(), method.getName()
                  ));
            }
            final Type typeArg = ((ParameterizedType) ret).getActualTypeArguments()[0];
            final Schema returnType = getReturnType(method, typeArg, annotation.schema());
            final List<Schema> parameters = createParameters(method, functionName);
            return Optional
                .of(createTableFunction(method, FunctionName.of(functionName), returnType,
                    parameters,
                    udtfDescriptionAnnotation.description()
                ));
          } catch (final KsqlException e) {
            LOGGER.warn(
                "Failed to add UDF to the MetaStore. name={} method={}",
                udtfDescriptionAnnotation.name(),
                method,
                e
            );
          }
          return Optional.<KsqlTableFunction>empty();
        })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(udtfFactory::addFunction);

    functionRegistry.addTableFunctionFactory(udtfFactory);
  }

  private KsqlTableFunction createTableFunction(
      final Method method,
      final FunctionName functionName,
      final Schema outputType,
      final List<Schema> arguments,
      final String description
  ) {
    final FunctionInvoker invoker = createFunctionInvoker(method);
    final Object instance = instantiateUdtfClass(method, description);
    @SuppressWarnings("unchecked") final KsqlTableFunction tableFunction = new BaseTableFunction(
        functionName, outputType, arguments, description) {
      @Override
      public List<?> apply(final Object... args) {
        return (List) invoker.eval(instance, args);
      }
    };
    return tableFunction;
  }

  private static Object instantiateUdfClass(
      final Method method,
      final UdfDescription annotation
  ) {
    try {
      return method.getDeclaringClass().newInstance();
    } catch (final Exception e) {
      throw new KsqlException(
          "Failed to create instance for UDF="
              + annotation.name()
              + ", method=" + method,
          e
      );
    }
  }

  private static Object instantiateUdfClass(
      final Class udfClass,
      final UdfDescription annotation
  ) {
    try {
      return udfClass.newInstance();
    } catch (final Exception e) {
      throw new KsqlException("Failed to create instance for UDF="
          + annotation.name(), e);
    }
  }

  private static Object instantiateUdtfClass(
      final Method method,
      final String udtfName
  ) {
    try {
      return method.getDeclaringClass().newInstance();
    } catch (final Exception e) {
      throw new KsqlException(
          "Failed to create instance for UDTF="
              + udtfName
              + ", method=" + method,
          e
      );
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
            schemaProviderName
        ));
      }
      return m;
    } catch (NoSuchMethodException e) {
      throw new KsqlException(String.format(
          "Cannot find schema provider method with name %s and parameter List<SqlType> in class "
              + "%s.", schemaProviderName, theClass.getName()), e);
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
          m.getName(), annotation.name()
      ), e);
    }
  }

  private void addSensor(final String sensorName, final String udfName) {
    metrics.ifPresent(metrics -> {
      if (metrics.getSensor(sensorName) == null) {
        final Sensor sensor = metrics.sensor(sensorName);
        sensor.add(
            metrics.metricName(sensorName + "-avg", UDF_METRIC_GROUP,
                "Average time for an invocation of " + udfName + " udf"
            ),
            new Avg()
        );
        sensor.add(
            metrics.metricName(sensorName + "-max", UDF_METRIC_GROUP,
                "Max time for an invocation of " + udfName + " udf"
            ),
            new Max()
        );
        sensor.add(
            metrics.metricName(sensorName + "-count", UDF_METRIC_GROUP,
                "Total number of invocations of " + udfName + " udf"
            ),
            new WindowedCount()
        );
        sensor.add(
            metrics.metricName(sensorName + "-rate", UDF_METRIC_GROUP,
                "The average number of occurrence of " + udfName + " operation per second "
                    + udfName + " udf"
            ),
            new Rate(TimeUnit.SECONDS, new WindowedCount())
        );
      }
    });
  }

  public static UdfLoader newInstance(
      final KsqlConfig config,
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
        : empty();

    if (config.getBoolean(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED)) {
      System.setSecurityManager(ExtensionSecurityManager.INSTANCE);
    }
    return new UdfLoader(
        metaStore,
        pluginDir,
        Thread.currentThread().getContextClassLoader(),
        new Blacklist(new File(pluginDir, "resource-blacklist.txt")),
        metrics,
        loadCustomerUdfs
    );
  }

  private Schema getReturnType(final Method method, final String annotationSchema) {
    return getReturnType(method, method.getGenericReturnType(), annotationSchema);
  }

  private Schema getReturnType(
      final Method method, final Type type, final String annotationSchema
  ) {
    try {
      final Schema returnType = annotationSchema.isEmpty()
          ? UdfUtil.getSchemaFromType(type)
          : SchemaConverters
              .sqlToConnectConverter()
              .toConnectSchema(
                  typeParser.parse(annotationSchema).getSqlType());

      return SchemaUtil.ensureOptional(returnType);
    } catch (final KsqlException e) {
      throw new KsqlException("Could not load UDF method with signature: " + method, e);
    }
  }

}
