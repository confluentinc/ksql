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

import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads user defined table functions (UDTFs)
 */
public class UdtfLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(UdtfLoader.class);

  private final MutableFunctionRegistry functionRegistry;
  private final Optional<Metrics> metrics;
  private final SqlTypeParser typeParser;
  private final boolean throwExceptionOnLoadFailure;

  public UdtfLoader(
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

  public void loadUdtfFromClass(
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

    FunctionMetrics.initInvocationSensor(metrics, sensorName, "ksql-udtf", functionName + " udtf");

    final UdfMetadata metadata = new UdfMetadata(
        udtfDescriptionAnnotation.name(),
        udtfDescriptionAnnotation.description(),
        udtfDescriptionAnnotation.author(),
        udtfDescriptionAnnotation.version(),
        udtfDescriptionAnnotation.category(),
        path
    );

    final TableFunctionFactory factory = new TableFunctionFactory(metadata);

    for (final Method method : theClass.getMethods()) {
      if (method.getAnnotation(Udtf.class) != null) {
        final Udtf annotation = method.getAnnotation(Udtf.class);
        try {
          if (method.getReturnType() != List.class) {
            throw new KsqlException(String
                .format("UDTF functions must return a List. Class %s Method %s",
                    theClass.getName(), method.getName()
                ));
          }
          final Type ret = method.getGenericReturnType();
          if (!(ret instanceof ParameterizedType)) {
            throw new KsqlException(String
                .format(
                    "UDTF functions must return a parameterized List. Class %s Method %s",
                    theClass.getName(), method.getName()
                ));
          }
          final Type typeArg = ((ParameterizedType) ret).getActualTypeArguments()[0];
          final ParamType returnType = FunctionLoaderUtils
              .getReturnType(method, typeArg, annotation.schema(), typeParser);
          final List<ParameterInfo> parameters = FunctionLoaderUtils
              .createParameters(method, functionName, typeParser);
          final KsqlTableFunction tableFunction =
              createTableFunction(method, FunctionName.of(functionName), returnType,
                  parameters,
                  annotation.description(),
                  annotation
              );
          factory.addFunction(tableFunction);
        } catch (final KsqlException e) {
          if (throwExceptionOnLoadFailure) {
            throw e;
          } else {
            LOGGER.warn(
                "Failed to add UDTF to the MetaStore. name={} method={}",
                udtfDescriptionAnnotation.name(),
                method,
                e
            );
          }
        }
      }
    }

    functionRegistry.addTableFunctionFactory(factory);
  }

  private KsqlTableFunction createTableFunction(
      final Method method,
      final FunctionName functionName,
      final ParamType outputType,
      final List<ParameterInfo> parameters,
      final String description,
      final Udtf udtfAnnotation
  ) {
    final FunctionInvoker invoker = FunctionLoaderUtils.createFunctionInvoker(method);
    final Object instance = FunctionLoaderUtils
        .instantiateFunctionInstance(method.getDeclaringClass(), description);
    final SchemaProvider schemaProviderFunction = FunctionLoaderUtils
        .handleUdfReturnSchema(
            method.getDeclaringClass(),
            outputType,
            udtfAnnotation.schema(),
            typeParser,
            udtfAnnotation.schemaProvider(),
            functionName.text(),
            method.isVarArgs());
    return new KsqlTableFunction(
        schemaProviderFunction,
        functionName, outputType, parameters, description,
        new PluggableUdf(invoker, instance)
    );
  }
}
