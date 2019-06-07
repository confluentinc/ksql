/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Schema;

public class UdfFactory {
  private final UdfMetadata metadata;
  private final Class<? extends Kudf> udfClass;
  private final Map<List<FunctionParameter>, KsqlFunction> functions = new LinkedHashMap<>();


  UdfFactory(final Class<? extends Kudf> udfClass,
             final UdfMetadata metadata) {
    this.udfClass = Objects.requireNonNull(udfClass, "udfClass can't be null");
    this.metadata = Objects.requireNonNull(metadata, "metadata can't be null");
  }

  public UdfFactory copy() {
    final UdfFactory udf = new UdfFactory(udfClass, metadata);
    udf.functions.putAll(functions);
    return udf;
  }

  void addFunction(final KsqlFunction ksqlFunction) {
    final List<FunctionParameter> paramTypes
        = mapToFunctionParameter(ksqlFunction.getArguments());

    checkCompatible(ksqlFunction, paramTypes);
    functions.put(paramTypes, ksqlFunction);
  }

  private void checkCompatible(final KsqlFunction ksqlFunction,
                               final List<FunctionParameter> paramTypes) {
    if (udfClass != ksqlFunction.getKudfClass()) {
      throw new KsqlException("Can't add function " + ksqlFunction
          + " as a function with the same name exists on a different class " + udfClass);
    }
    if (functions.containsKey(paramTypes)) {
      throw new KsqlException("Can't add function " + ksqlFunction
          + " as a function with the same name and argument types already exists "
          + functions.get(paramTypes));
    }
    if (!ksqlFunction.getPathLoadedFrom().equals(metadata.getPath())) {
      throw new KsqlException("Can't add function " + ksqlFunction
          + "as a function with the same name has been loaded from a different jar "
          + metadata.getPath());
    }
  }

  public String getName() {
    return metadata.getName();
  }

  public String getAuthor() {
    return metadata.getAuthor();
  }

  public String getVersion() {
    return metadata.getVersion();
  }

  public String getDescription() {
    return metadata.getDescription();
  }

  public void eachFunction(final Consumer<KsqlFunction> consumer) {
    functions.values().forEach(consumer);
  }

  public String getPath() {
    return metadata.getPath();
  }

  @Override
  public String toString() {
    return "UdfFactory{"
        + "metadata=" + metadata
        + ", udfClass=" + udfClass
        + ", functions=" + functions
        + '}';
  }

  public KsqlFunction getFunction(final List<Schema> paramTypes) {
    final List<FunctionParameter> params = mapToFunctionParameter(paramTypes);
    final KsqlFunction function = functions.get(params);
    if (function != null) {
      return function;
    }

    if (paramTypes.stream().anyMatch(Objects::isNull)) {
      return functions.entrySet()
          .stream()
          .filter(entry -> checkParamsMatch(entry.getKey(), paramTypes))
          .map(Map.Entry::getValue)
          .findFirst()
          .orElseThrow(() -> createNoMatchingFunctionException(paramTypes));
    }

    throw createNoMatchingFunctionException(paramTypes);
  }

  private KsqlException createNoMatchingFunctionException(final List<Schema> paramTypes) {
    final String sqlParamTypes = paramTypes.stream()
        .map(schema -> schema == null
            ? null
            : SchemaUtil.getSchemaTypeAsSqlType(schema.type()))
        .collect(Collectors.joining(", ", "[", "]"));

    return new KsqlException("Function '" + metadata.getName()
                            + "' does not accept parameters of types:" + sqlParamTypes);
  }

  private static boolean checkParamsMatch(final List<FunctionParameter> functionArgTypes,
                                          final List<Schema> suppliedParamTypes) {
    if (functionArgTypes.size() != suppliedParamTypes.size()) {
      return false;
    }

    return IntStream.range(0, suppliedParamTypes.size())
        .boxed()
        .allMatch(idx -> functionArgTypes.get(idx).matches(suppliedParamTypes.get(idx)));
  }

  private List<FunctionParameter> mapToFunctionParameter(final List<Schema> params) {
    return params
        .stream()
        .map(schema -> schema == null
            ? new FunctionParameter(null, false)
            : new FunctionParameter(schema.type(), schema.isOptional()))
        .collect(Collectors.toList());
  }

  private class FunctionParameter {
    private final Schema.Type type;
    private final boolean isOptional;

    private FunctionParameter(final Schema.Type type, final boolean isOptional) {
      this.type = type;
      this.isOptional = isOptional;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final FunctionParameter that = (FunctionParameter) o;
      // isOptional is excluded from equals and hashCode so that
      // primitive types will match their boxed counterparts. i.e,
      // primitive types are not optional, i.e., they don't accept null.
      return type == that.type;
    }

    @Override
    public int hashCode() {
      // isOptional is excluded from equals and hashCode so that
      // primitive types will match their boxed counterparts. i.e,
      // primitive types are not optional, i.e., they don't accept null.
      return Objects.hash(type);
    }

    boolean isOptional() {
      return isOptional;
    }

    public boolean matches(final Schema schema) {
      if (schema == null) {
        return isOptional;
      }
      return type.equals(schema.type());
    }
  }
}
