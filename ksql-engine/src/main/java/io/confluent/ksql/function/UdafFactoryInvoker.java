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

import com.google.common.primitives.Primitives;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

class UdafFactoryInvoker implements FunctionSignature {

  private final FunctionName functionName;
  private final Schema aggregateArgType;
  private final Schema aggregateReturnType;
  private final Optional<Metrics> metrics;
  private final List<Schema> argTypes;
  private final Method method;
  private final String description;

  UdafFactoryInvoker(
      final Method method,
      final FunctionName functionName,
      final String description,
      final String inputSchema,
      final String aggregateSchema,
      final String outputSchema,
      final SqlTypeParser typeParser,
      final Optional<Metrics> metrics
  ) {
    if (!(Udaf.class.equals(method.getReturnType())
        || TableUdaf.class.equals(method.getReturnType()))) {
      final String functionInfo = String.format("method='%s', functionName='%s', UDFClass='%s'",
          method.getName(), functionName, method.getDeclaringClass());
      throw new KsqlException("UDAFs must implement " + Udaf.class.getName() + " or "
          + TableUdaf.class.getName() + ". "
          + functionInfo);
    }
    if (!Modifier.isStatic(method.getModifiers())) {
      throw new KsqlException("UDAF factory methods must be static " + method);
    }
    final UdafTypes types = new UdafTypes(method, functionName.name(), typeParser);
    this.functionName = Objects.requireNonNull(functionName);
    this.aggregateArgType = Objects.requireNonNull(types.getAggregateSchema(aggregateSchema));
    this.aggregateReturnType = Objects.requireNonNull(types.getOutputSchema(outputSchema));
    this.metrics = Objects.requireNonNull(metrics);
    this.argTypes =
        Collections.singletonList(types.getInputSchema(Objects.requireNonNull(inputSchema)));
    this.method = Objects.requireNonNull(method);
    this.description = Objects.requireNonNull(description);
  }

  @SuppressWarnings("unchecked")
  KsqlAggregateFunction createFunction(final AggregateFunctionInitArguments initArgs) {
    final Object[] factoryArgs = new Object[initArgs.argsSize()];
    for (int i = 0; i < factoryArgs.length; i++) {
      final Class<?> argType = method.getParameterTypes()[i];
      final Object arg = coerce(argType, initArgs.arg(i));
      factoryArgs[i] = arg;
    }

    try {
      final Udaf udaf = (Udaf)method.invoke(null, factoryArgs);
      final KsqlAggregateFunction function;
      if (TableUdaf.class.isAssignableFrom(method.getReturnType())) {
        function = new UdafTableAggregateFunction(functionName.name(), initArgs.udafIndex(),
            udaf, aggregateArgType, aggregateReturnType, argTypes, description, metrics,
            method.getName());
      } else {
        function = new UdafAggregateFunction(functionName.name(), initArgs.udafIndex(),
            udaf, aggregateArgType, aggregateReturnType, argTypes, description, metrics,
            method.getName());
      }
      return function;
    } catch (Exception e) {
      throw new KsqlException("Failed to invoke UDAF factory method", e);
    }
  }

  @Override
  public FunctionName getFunctionName() {
    return functionName;
  }

  @Override
  public List<Schema> getArguments() {
    return argTypes;
  }

  @Override
  public boolean isVariadic() {
    return false;
  }

  private static Object coerce(
      final Class<?> clazz,
      final String arg) {
    if (Integer.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return Integer.valueOf(arg);
    } else if (Long.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return Long.valueOf(arg);
    } else if (Double.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return Double.valueOf(arg);
    } else if (Float.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return Float.valueOf(arg);
    } else if (Byte.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return Byte.valueOf(arg);
    } else if (Short.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return Short.valueOf(arg);
    } else if (Boolean.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return Boolean.valueOf(arg);
    } else if (String.class.isAssignableFrom(clazz)) {
      return arg;
    } else if (Struct.class.isAssignableFrom(clazz)) {
      return arg;
    }
    throw new KsqlFunctionException("Unsupported udaf argument type: " + clazz);
  }
}
