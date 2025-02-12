/*
 * Copyright 2019 Confluent Inc.
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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads user defined functions (UDFs)
 */
public class UdfLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(UdfLoader.class);

  private final MutableFunctionRegistry functionRegistry;
  private final Optional<Metrics> metrics;
  private final SqlTypeParser typeParser;
  private final boolean throwExceptionOnLoadFailure;

  public UdfLoader(
      final MutableFunctionRegistry functionRegistry,
      final Optional<Metrics> metrics,
      final SqlTypeParser typeParser,
      final boolean throwExceptionOnLoadFailure
  ) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.typeParser = Objects.requireNonNull(typeParser, "typeParser");
    this.throwExceptionOnLoadFailure = throwExceptionOnLoadFailure;
  }

  // This method is only used from tests
  @VisibleForTesting
  void loadUdfFromClass(final Class<?>... udfClasses) {
    for (final Class<?> theClass : udfClasses) {
      loadUdfFromClass(
          theClass, KsqlScalarFunction.INTERNAL_PATH);
    }
  }

  @VisibleForTesting
  public void loadUdfFromClass(
      final Class<?> theClass,
      final String path
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

    FunctionMetrics.initInvocationSensor(metrics, sensorName, "ksql-udf", functionName + " udf");

    final UdfFactory factory = new UdfFactory(
        udfClass,
        new UdfMetadata(
            udfDescriptionAnnotation.name(),
            udfDescriptionAnnotation.description(),
            udfDescriptionAnnotation.author(),
            udfDescriptionAnnotation.version(),
            udfDescriptionAnnotation.category(),
            path
        )
    );

    functionRegistry.ensureFunctionFactory(factory);

    for (final Method method : theClass.getMethods()) {
      final Udf udfAnnotation = method.getAnnotation(Udf.class);
      if (udfAnnotation != null) {
        final KsqlScalarFunction function;
        try {
          function = createFunction(theClass, udfDescriptionAnnotation, udfAnnotation, method, path,
              sensorName, udfClass
          );
        } catch (final KsqlException e) {
          if (throwExceptionOnLoadFailure) {
            throw e;
          } else {
            LOGGER.warn(
                "Failed to add UDF to the MetaStore. name={} method={}",
                udfDescriptionAnnotation.name(),
                method,
                e
            );
            continue;
          }
        }
        factory.addFunction(function);
      }
    }
  }

  private KsqlScalarFunction createFunction(
      final Class theClass,
      final UdfDescription udfDescriptionAnnotation,
      final Udf udfAnnotation,
      final Method method,
      final String path,
      final String sensorName,
      final Class<? extends Kudf> udfClass
  ) {
    // sanity check
    FunctionLoaderUtils
        .instantiateFunctionInstance(method.getDeclaringClass(), udfDescriptionAnnotation.name());
    final FunctionInvoker invoker = FunctionLoaderUtils.createFunctionInvoker(method);
    final String functionName = udfDescriptionAnnotation.name();

    LOGGER.info("Adding function " + functionName + " for method " + method);

    final List<ParameterInfo> parameters = FunctionLoaderUtils
        .createParameters(method, functionName, typeParser);

    final ParamType javaReturnSchema = FunctionLoaderUtils
        .getReturnType(method, udfAnnotation.schema(), typeParser);

    final SchemaProvider schemaProviderFunction = FunctionLoaderUtils
        .handleUdfReturnSchema(
            theClass,
            javaReturnSchema,
            udfAnnotation.schema(),
            typeParser,
            udfAnnotation.schemaProvider(),
            udfDescriptionAnnotation.name(),
            method.isVarArgs()
        );

    return KsqlScalarFunction.create(
        schemaProviderFunction,
        javaReturnSchema,
        parameters,
        FunctionName.of(functionName.toUpperCase()),
        udfClass,
        getUdfFactory(method, udfDescriptionAnnotation, functionName, invoker, sensorName),
        udfAnnotation.description(),
        path,
        method.isVarArgs()
    );
  }

  private Function<KsqlConfig, Kudf> getUdfFactory(
      final Method method,
      final UdfDescription udfDescriptionAnnotation,
      final String functionName,
      final FunctionInvoker invoker,
      final String sensorName
  ) {
    return ksqlConfig -> {
      final Object actualUdf = FunctionLoaderUtils.instantiateFunctionInstance(
          method.getDeclaringClass(), udfDescriptionAnnotation.name());
      if (actualUdf instanceof Configurable) {
        ExtensionSecurityManager.INSTANCE.pushInUdf();
        try {
          ((Configurable) actualUdf)
              .configure(ksqlConfig.getKsqlFunctionsConfigProps(functionName));
        } finally {
          ExtensionSecurityManager.INSTANCE.popOutUdf();
        }
      }
      final PluggableUdf theUdf = new PluggableUdf(invoker, actualUdf);
      return metrics.<Kudf>map(m -> new UdfMetricProducer(
          m.getSensor(sensorName),
          theUdf,
          Time.SYSTEM
      )).orElse(theUdf);
    };
  }
}
