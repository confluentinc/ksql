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

import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads user defined table functions (UDTFs)
 */
class UdtfLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(UdtfLoader.class);

  private final MutableFunctionRegistry functionRegistry;
  private final Optional<Metrics> metrics;
  private final SqlTypeParser typeParser;

  UdtfLoader(
      final MutableFunctionRegistry functionRegistry,
      final Optional<Metrics> metrics,
      final SqlTypeParser typeParser
  ) {
    this.functionRegistry = functionRegistry;
    this.metrics = metrics;
    this.typeParser = typeParser;
  }

  void loadUdtfFromClass(
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
    FunctionLoaderUtils.addSensor(sensorName, functionName, metrics);

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
            final Schema returnType = FunctionLoaderUtils
                .getReturnType(method, typeArg, annotation.schema(), typeParser);
            final List<Schema> parameters = FunctionLoaderUtils
                .createParameters(method, functionName, typeParser);
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
    final FunctionInvoker invoker = FunctionLoaderUtils.createFunctionInvoker(method);
    final Object instance = FunctionLoaderUtils
        .instantiateFunctionInstance(method.getDeclaringClass(), description);
    @SuppressWarnings("unchecked") final KsqlTableFunction tableFunction = new BaseTableFunction(
        functionName, outputType, arguments, description) {
      @Override
      public List<?> apply(final Object... args) {
        return (List) invoker.eval(instance, args);
      }
    };
    return tableFunction;
  }
}
