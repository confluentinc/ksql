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

import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udaf.DynamicUdaf;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UdafFactoryInvoker implements FunctionSignature {

  private static final Logger LOG = LoggerFactory.getLogger(UdafFactoryInvoker.class);

  private final FunctionName functionName;
  private final ParamType aggregateArgType;
  private final ParamType aggregateReturnType;
  private final Optional<Metrics> metrics;
  private final List<ParamType> paramTypes;
  private final List<ParameterInfo> params;
  private final Method method;
  private final String description;

  UdafFactoryInvoker(
      final Method method,
      final FunctionName functionName,
      final SqlTypeParser typeParser,
      final Optional<Metrics> metrics
  ) {
    final UdafFactory annotation = method.getAnnotation(UdafFactory.class);
    final String description = annotation == null ? "" : annotation.description();

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
    final UdafTypes types = new UdafTypes(method, functionName, typeParser);
    this.functionName = Objects.requireNonNull(functionName);
    this.aggregateArgType = Objects.requireNonNull(types.getAggregateSchema());
    this.aggregateReturnType = Objects.requireNonNull(types.getOutputSchema());
    this.metrics = Objects.requireNonNull(metrics);
    this.params = Objects.requireNonNull(types.getInputSchema());
    this.paramTypes = params.stream().map(ParameterInfo::type).collect(Collectors.toList());
    this.method = Objects.requireNonNull(method);
    this.description = Objects.requireNonNull(description);
  }

  @SuppressWarnings("unchecked")
  KsqlAggregateFunction createFunction(
      final List<SqlArgument> argTypeList,
      final AggregateFunctionInitArguments initArgs
  ) {
    final Object[] factoryArgs = initArgs.args().toArray();
    try {
      final Udaf udaf = (Udaf)method.invoke(null, factoryArgs);

      // JNH: Make UDAFs Configurable
      if (udaf instanceof Configurable) {
        ((Configurable) udaf).configure(initArgs.config());
      }

      // Use argTypeList to compute aggregate and return types!
      // JNH: Compute these with annotations
      SqlType sqlAggregateType = null;
      SqlType sqlReturnType = null;

      // JNH: Clean up if the idea of a DynamicUdaf is ok!
      if (udaf instanceof DynamicUdaf) {
        final DynamicUdaf dynamicUdaf = (DynamicUdaf) udaf;
        dynamicUdaf.setInputArgumentTypes(argTypeList);

        if (dynamicUdaf.getAggregateType() != null || dynamicUdaf.getAggregateType() != null) {
          /*
          if (!dynamicUdaf.getAggregateType().equals(sqlAggregateType)) {
            System.out.println("Aggregate types do not match for " + argTypeList.get(0));
            System.out.println("Annotation: " + aggregateArgType + " computed: "
            + dynamicUdaf.getAggregateType());
          }
          if (!dynamicUdaf.getReturnType().equals(sqlReturnType)) {
            System.out.println("Return types do not match for " + argTypeList.get(0));
          }
           */
          sqlAggregateType = dynamicUdaf.getAggregateType();
          sqlReturnType    = dynamicUdaf.getReturnType();
          System.out.println("Setting agg and return types to "
              + sqlAggregateType + " " + sqlReturnType);
        } else {
          throw new Exception("DynamicUdafs must implement `getAggregateType` and `getReturnType`.");
        }
      } else {
        sqlAggregateType =
            SchemaConverters.functionToSqlConverter().toSqlType(aggregateArgType);
        sqlReturnType =
            SchemaConverters.functionToSqlConverter().toSqlType(aggregateReturnType);
      }

      final KsqlAggregateFunction function;
      if (TableUdaf.class.isAssignableFrom(method.getReturnType())) {
        function = new UdafTableAggregateFunction(
            functionName.text(),
            initArgs.udafIndex(),
            udaf,
            sqlAggregateType,
            sqlReturnType,
            params,
            description,
            metrics,
            method.getName());
      } else {
        function = new UdafAggregateFunction(
            functionName.text(),
            initArgs.udafIndex(),
            udaf,
            sqlAggregateType,
            sqlReturnType,
            params,
            description,
            metrics,
            method.getName());
      }
      return function;
    } catch (final Exception e) {
      LOG.error("Failed to invoke UDAF factory method", e);
      throw new KsqlException("Failed to invoke UDAF factory method", e);
    }
  }

  @Override
  public FunctionName name() {
    return functionName;
  }

  @Override
  public ParamType declaredReturnType() {
    return aggregateReturnType;
  }

  @Override
  public List<ParamType> parameters() {
    return paramTypes;
  }

  @Override
  public List<ParameterInfo> parameterInfo() {
    return params;
  }

  @Override
  public boolean isVariadic() {
    return false;
  }

}
