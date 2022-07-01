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
import io.confluent.ksql.function.udaf.VariadicArgs;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.Triple;
import org.apache.kafka.connect.data.Struct;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class BaseAggregateFunction<I, A, O> implements KsqlAggregateFunction<I, A, O> {

  /** An index of the function argument in the row that is used for computing the aggregate.
   * For instance, in the example SELECT key, SUM(ROWTIME), aggregation will be done on a row
   * with two columns (key, ROWTIME) and the `argIndexInValue` will be 1.
   **/
  private final List<Integer> argIndicesInValue;
  private final Supplier<A> initialValueSupplier;
  private final SqlType aggregateSchema;
  private final SqlType outputSchema;
  private final ImmutableList<ParameterInfo> params;
  private final ImmutableList<ParamType> paramTypes;
  private final Function<List<Object>, Object> inputConverter;

  protected final String functionName;
  private final String description;

  public BaseAggregateFunction(
      final String functionName,
      final List<Integer> argIndicesInValue,
      final Supplier<A> initialValueSupplier,
      final SqlType aggregateType,
      final SqlType outputType,
      final List<ParameterInfo> parameters,
      final String description
  ) {
    this.argIndicesInValue = argIndicesInValue;
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
    this.inputConverter = determineInputConverter();
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.description = Objects.requireNonNull(description, "description");
  }

  public FunctionName name() {
    return FunctionName.of(functionName);
  }

  public Supplier<A> getInitialValueSupplier() {
    return initialValueSupplier;
  }

  public List<Integer> getArgIndicesInValue() {
    return argIndicesInValue;
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
  public Object convertToInput(List<Object> arguments) {
    return inputConverter.apply(arguments);
  }

  @Override
  public String getDescription() {
    return description;
  }

  private Function<List<Object>, Object> determineInputConverter() {
    if (isVariadic()) {
      switch (parameters().size()) {
        case 1:
          return VariadicArgs::new;
        case 2:
          return (objects) -> Pair.of(
                  objects.get(0),
                  new VariadicArgs<>(objects.subList(1, objects.size()))
          );
        case 3:
          return (objects) -> Triple.of(
                  objects.get(0),
                  objects.get(1),
                  new VariadicArgs<>(objects.subList(2, objects.size()))
          );
        default:
          throw new KsqlException("Unsupported number of aggregation function parameters: "
                  + parameters().size());
      }
    } else {
      switch (parameters().size()) {
        case 1:
          return (objects) -> objects.get(0);
        case 2:
          return (objects) -> Pair.of(
                  objects.get(0),
                  objects.get(1)
          );
        case 3:
          return (objects) -> Triple.of(
                  objects.get(0),
                  objects.get(1),
                  objects.get(2)
          );
        default:
          throw new KsqlException("Unsupported number of aggregation function parameters: "
                  + parameters().size());
      }
    }
  }
}
