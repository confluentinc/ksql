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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Immutable
public class KsqlFunction implements FunctionSignature {

  @EffectivelyImmutable
  private final SchemaProvider returnSchemaProvider;
  private final ParamType declaredReturnType;
  private final ImmutableList<ParameterInfo> parameters;
  private final ImmutableList<ParamType> paramTypes;
  private final FunctionName functionName;
  private final String description;
  private final String pathLoadedFrom;
  private final boolean isVariadic;

  KsqlFunction(
      final SchemaProvider returnSchemaProvider,
      final ParamType declaredReturnType,
      final List<ParameterInfo> parameters,
      final FunctionName functionName,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic
  ) {

    this.returnSchemaProvider = Objects.requireNonNull(returnSchemaProvider, "schemaProvider");
    this.declaredReturnType = Objects.requireNonNull(declaredReturnType, "javaReturnType");
    this.parameters = ImmutableList.copyOf(Objects.requireNonNull(parameters, "parameters"));
    this.paramTypes = ImmutableList
        .copyOf(parameters.stream().map(ParameterInfo::type).collect(Collectors.toList()));
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.description = Objects.requireNonNull(description, "description");
    this.pathLoadedFrom = Objects.requireNonNull(pathLoadedFrom, "pathLoadedFrom");
    this.isVariadic = isVariadic;

    if (parameters.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("KSQL Function can't have null argument types");
    }
    if (isVariadic) {
      if (parameters.isEmpty()) {
        throw new IllegalArgumentException(
            "KSQL variadic functions must have at least one parameter");
      }
      if (parameterInfo().stream().noneMatch(
              (info) -> info.type() instanceof ArrayType && info.isVariadic()
      )) {
        throw new IllegalArgumentException(
            "KSQL variadic functions must have ARRAY type as their variadic parameter");
      }
      if (parameterInfo().stream().filter(ParameterInfo::isVariadic).count() != 1) {
        throw new IllegalArgumentException(
                "KSQL variadic functions must have exactly one variadic argument"
        );
      }
    }
  }

  public SqlType getReturnType(final List<SqlArgument> arguments) {
    return returnSchemaProvider.resolve(parameters(), arguments);
  }

  @Override
  public ParamType declaredReturnType() {
    return declaredReturnType;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "paramTypes is ImmutableList")
  public List<ParamType> parameters() {
    return paramTypes;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "parameters is ImmutableList")
  public List<ParameterInfo> parameterInfo() {
    return parameters;
  }

  @Override
  public FunctionName name() {
    return functionName;
  }

  public String getDescription() {
    return description;
  }

  public String getPathLoadedFrom() {
    return pathLoadedFrom;
  }

  public boolean isVariadic() {
    return isVariadic;
  }

  @Override
  public String toString() {
    return "KsqlFunction{"
        + "returnType=" + declaredReturnType
        + ", arguments=" + parameters
        + ", functionName='" + functionName + '\''
        + ", description='" + description + "'"
        + ", pathLoadedFrom='" + pathLoadedFrom + "'"
        + ", isVariadic=" + isVariadic
        + '}';
  }

}