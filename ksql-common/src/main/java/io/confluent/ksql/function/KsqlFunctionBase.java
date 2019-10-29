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
import com.google.common.collect.Iterables;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

@Immutable
public class KsqlFunctionBase implements FunctionSignature {

  private final Function<List<Schema>, Schema> returnSchemaProvider;
  private final Schema javaReturnType;
  private final List<Schema> parameters;
  private final FunctionName functionName;
  private final String description;
  private final String pathLoadedFrom;
  private final boolean isVariadic;

  KsqlFunctionBase(
      final Function<List<Schema>, Schema> returnSchemaProvider,
      final Schema javaReturnType,
      final List<Schema> arguments,
      final FunctionName functionName,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic
  ) {

    this.returnSchemaProvider = Objects.requireNonNull(returnSchemaProvider, "schemaProvider");
    this.javaReturnType = Objects.requireNonNull(javaReturnType, "javaReturnType");
    this.parameters = ImmutableList.copyOf(Objects.requireNonNull(arguments, "arguments"));
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.description = Objects.requireNonNull(description, "description");
    this.pathLoadedFrom = Objects.requireNonNull(pathLoadedFrom, "pathLoadedFrom");
    this.isVariadic = isVariadic;

    if (arguments.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("KSQL Function can't have null argument types");
    }
    if (isVariadic) {
      if (arguments.isEmpty()) {
        throw new IllegalArgumentException(
            "KSQL variadic functions must have at least one parameter");
      }
      if (!Iterables.getLast(arguments).type().equals(Type.ARRAY)) {
        throw new IllegalArgumentException(
            "KSQL variadic functions must have ARRAY type as their last parameter");
      }
    }
  }

  public Schema getReturnType(final List<Schema> arguments) {

    final Schema returnType = returnSchemaProvider.apply(arguments);

    if (returnType == null) {
      throw new KsqlException(String.format("Return type of UDF %s cannot be null.", functionName));
    }

    if (!returnType.isOptional()) {
      throw new IllegalArgumentException("KSQL only supports optional field types");
    }

    if (!GenericsUtil.hasGenerics(returnType)) {
      checkMatchingReturnTypes(returnType, javaReturnType);
      return returnType;
    }

    final Map<Schema, Schema> genericMapping = new HashMap<>();
    for (int i = 0; i < Math.min(parameters.size(), arguments.size()); i++) {
      final Schema schema = parameters.get(i);

      // we resolve any variadic as if it were an array so that the type
      // structure matches the input type
      final Schema instance = isVariadic && i == parameters.size() - 1
          ? SchemaBuilder.array(arguments.get(i)).build()
          : arguments.get(i);

      genericMapping.putAll(GenericsUtil.resolveGenerics(schema, instance));
    }

    final Schema genericSchema = GenericsUtil.applyResolved(returnType, genericMapping);
    final Schema genericJavaSchema = GenericsUtil.applyResolved(javaReturnType, genericMapping);
    checkMatchingReturnTypes(genericSchema, genericJavaSchema);

    return genericSchema;
  }

  private void checkMatchingReturnTypes(final Schema s1, final Schema s2) {
    if (!SchemaUtil.areCompatible(s1, s2)) {
      throw new KsqlException(String.format(
          "Return type %s of UDF %s does not match the declared "
              + "return type %s.",
          SchemaConverters.connectToSqlConverter().toSqlType(
              s1).toString(),
          functionName.toString(FormatOptions.noEscape()),
          SchemaConverters.connectToSqlConverter().toSqlType(
              s2).toString()
      ));
    }
  }

  public List<Schema> getArguments() {
    return parameters;
  }

  public FunctionName getFunctionName() {
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
        + "returnType=" + javaReturnType
        + ", arguments=" + parameters.stream().map(Schema::type).collect(Collectors.toList())
        + ", functionName='" + functionName + '\''
        + ", description='" + description + "'"
        + ", pathLoadedFrom='" + pathLoadedFrom + "'"
        + ", isVariadic=" + isVariadic
        + '}';
  }

}