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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.security.ExtensionSecurityManager;
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
  private final Optional<Metrics> metrics;
  private final List<ParamType> paramTypes;
  private final List<ParameterInfo> params;
  private final Method method;
  private final String description;
  private final UdafTypes types;
  private final String aggregateSchema;
  private final String outputSchema;
  private ParamType aggregateReturnType;

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
    this.types = new UdafTypes(method, functionName, typeParser);
    this.functionName = Objects.requireNonNull(functionName);
    this.aggregateSchema = aggregateSchema; // This can be null if the annotation is not used.
    this.outputSchema = outputSchema;       // This can be null if the annotation is not used.
    this.metrics = Objects.requireNonNull(metrics);
    this.params = types.getInputSchema(Objects.requireNonNull(inputSchema));
    this.paramTypes = params.stream().map(ParameterInfo::type).collect(Collectors.toList());
    this.method = Objects.requireNonNull(method);
    this.description = Objects.requireNonNull(description);
  }

  @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "REC_CATCH_EXCEPTION"})
  @SuppressWarnings("unchecked")
  KsqlAggregateFunction createFunction(final AggregateFunctionInitArguments initArgs,
      final List<SqlArgument> argTypeList) {
    final Object[] factoryArgs = initArgs.args().toArray();
    try {
      ExtensionSecurityManager.INSTANCE.pushInUdf();
      final Udaf udaf = (Udaf)method.invoke(null, factoryArgs);
      udaf.initializeTypeArguments(argTypeList);
      if (udaf instanceof Configurable) {
        ((Configurable) udaf).configure(initArgs.config());
      }

      final SqlType aggregateSqlType = (SqlType) udaf.getAggregateSqlType()
          .orElseGet(() -> SchemaConverters.functionToSqlConverter()
              .toSqlType(types.getAggregateSchema(aggregateSchema)));
      final SqlType returnSqlType = (SqlType) udaf.getReturnSqlType()
          .orElseGet(() ->
              SchemaConverters.functionToSqlConverter()
                  .toSqlType(types.getOutputSchema(outputSchema)));
      this.aggregateReturnType =
          SchemaConverters.sqlToFunctionConverter().toFunctionType(returnSqlType);

      final KsqlAggregateFunction function;
      if (TableUdaf.class.isAssignableFrom(method.getReturnType())) {
        function = new UdafTableAggregateFunction(
            functionName.text(),
            initArgs.udafIndex(),
            udaf,
            aggregateSqlType,
            returnSqlType,
            params,
            description,
            metrics,
            method.getName());
      } else {
        function = new UdafAggregateFunction(
            functionName.text(),
            initArgs.udafIndex(),
            udaf,
            aggregateSqlType,
            returnSqlType,
            params,
            description,
            metrics,
            method.getName());
      }
      return function;
    } catch (final Exception e) {
      LOG.error("Failed to invoke UDAF factory method", e);
      throw new KsqlException("Failed to invoke UDAF factory method", e);
    } finally {
      ExtensionSecurityManager.INSTANCE.popOutUdf();
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
