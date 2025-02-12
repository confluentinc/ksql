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

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;

public abstract class BaseAggregateFunction<I, A, O> implements KsqlAggregateFunction<I, A, O> {

  /** An index of the function argument in the row that is used for computing the aggregate.
   * For instance, in the example SELECT key, SUM(ROWTIME), aggregation will be done on a row
   * with two columns (key, ROWTIME) and the `argIndexInValue` will be 1.
   **/
  private final int argIndexInValue;
  private final Supplier<A> initialValueSupplier;
  private final SqlType aggregateSchema;
  private final SqlType outputSchema;
  private final ImmutableList<ParameterInfo> params;
  private final ImmutableList<ParamType> paramTypes;

  protected final String functionName;
  private final String description;

  public BaseAggregateFunction(
      final String functionName,
      final int argIndexInValue,
      final Supplier<A> initialValueSupplier,
      final SqlType aggregateType,
      final SqlType outputType,
      final List<ParameterInfo> parameters,
      final String description
  ) {
    this.argIndexInValue = argIndexInValue;
    this.initialValueSupplier = () -> {
      ExtensionSecurityManager.INSTANCE.pushInUdf();
      try {
        final A val = initialValueSupplier.get();
        if (val instanceof Struct && !((Struct) val).schema().isOptional()) {
          throw new KsqlException("Initialize function for " + functionName
              + " must return struct with optional schema");
        }
        return val;
      } finally {
        ExtensionSecurityManager.INSTANCE.popOutUdf();
      }
    };
    this.aggregateSchema = Objects.requireNonNull(aggregateType, "aggregateType");
    this.outputSchema = Objects.requireNonNull(outputType, "outputType");
    this.params = ImmutableList.copyOf(Objects.requireNonNull(parameters, "parameters"));
    this.paramTypes = ImmutableList.copyOf(
        parameters.stream().map(ParameterInfo::type).collect(Collectors.toList())
    );
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.description = Objects.requireNonNull(description, "description");
  }

  public FunctionName name() {
    return FunctionName.of(functionName);
  }

  public Supplier<A> getInitialValueSupplier() {
    return initialValueSupplier;
  }

  public int getArgIndexInValue() {
    return argIndexInValue;
  }

  public SqlType getAggregateType() {
    return aggregateSchema;
  }

  @Override
  public SqlType returnType() {
    return outputSchema;
  }

  @Override
  public ParamType declaredReturnType() {
    return SchemaConverters.sqlToFunctionConverter().toFunctionType(outputSchema);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "paramTypes is ImmutableList")
  public List<ParamType> parameters() {
    return paramTypes;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "params is ImmutableList")
  public List<ParameterInfo> parameterInfo() {
    return params;
  }

  @Override
  public String getDescription() {
    return description;
  }
}
