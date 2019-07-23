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
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
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
public final class KsqlFunction implements IndexedFunction {

  static final String INTERNAL_PATH = "internal";

  private final Schema returnType;
  private final List<Schema> parameters;
  private final String functionName;
  private final Class<? extends Kudf> kudfClass;
  private final Function<KsqlConfig, Kudf> udfFactory;
  private final String description;
  private final String pathLoadedFrom;
  private final boolean isVariadic;
  private final boolean hasGenerics;

  /**
   * Create built in / legacy function.
   */
  @SuppressWarnings("deprecation")  // Solution only available in Java9.
  public static KsqlFunction createLegacyBuiltIn(
      final Schema returnType,
      final List<Schema> arguments,
      final String functionName,
      final Class<? extends Kudf> kudfClass
  ) {
    final Function<KsqlConfig, Kudf> udfFactory = ksqlConfig -> {
      try {
        return kudfClass.newInstance();
      } catch (final Exception e) {
        throw new KsqlException("Failed to create instance of kudfClass "
            + kudfClass + " for function " + functionName, e);
      }
    };

    return create(
        returnType, arguments, functionName, kudfClass, udfFactory, "", INTERNAL_PATH, false);
  }

  /**
   * Create udf.
   *
   * <p>Can be either built-in UDF or true user-supplied.
   */
  static KsqlFunction create(
      final Schema returnType,
      final List<Schema> arguments,
      final String functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KsqlConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic
  ) {
    return new KsqlFunction(
        returnType,
        arguments,
        functionName,
        kudfClass,
        udfFactory,
        description,
        pathLoadedFrom,
        isVariadic);
  }

  private KsqlFunction(
      final Schema returnType,
      final List<Schema> arguments,
      final String functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KsqlConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic) {
    this.returnType = Objects.requireNonNull(returnType, "returnType");
    this.parameters = ImmutableList.copyOf(Objects.requireNonNull(arguments, "arguments"));
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.kudfClass = Objects.requireNonNull(kudfClass, "kudfClass");
    this.udfFactory = Objects.requireNonNull(udfFactory, "udfFactory");
    this.description = Objects.requireNonNull(description, "description");
    this.pathLoadedFrom  = Objects.requireNonNull(pathLoadedFrom, "pathLoadedFrom");
    this.isVariadic = isVariadic;
    this.hasGenerics = GenericsUtil.hasGenerics(returnType);

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

    if (!returnType.isOptional()) {
      throw new IllegalArgumentException("KSQL only supports optional field types");
    }
  }


  public Schema getReturnType(final List<Schema> arguments) {
    if (!hasGenerics) {
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

    return GenericsUtil.applyResolved(returnType, genericMapping);
  }

  public List<Schema> getArguments() {
    return parameters;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getDescription() {
    return description;
  }

  public Class<? extends Kudf> getKudfClass() {
    return kudfClass;
  }

  public String getPathLoadedFrom() {
    return pathLoadedFrom;
  }

  public boolean isVariadic() {
    return isVariadic;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlFunction that = (KsqlFunction) o;
    return Objects.equals(returnType, that.returnType)
        && Objects.equals(parameters, that.parameters)
        && Objects.equals(functionName, that.functionName)
        && Objects.equals(kudfClass, that.kudfClass)
        && Objects.equals(pathLoadedFrom, that.pathLoadedFrom)
        && (isVariadic == that.isVariadic);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        returnType, parameters, functionName, kudfClass, pathLoadedFrom, isVariadic);
  }

  @Override
  public String toString() {
    return "KsqlFunction{"
        + "returnType=" + returnType
        + ", arguments=" + parameters.stream().map(Schema::type).collect(Collectors.toList())
        + ", functionName='" + functionName + '\''
        + ", kudfClass=" + kudfClass
        + ", description='" + description + "'"
        + ", pathLoadedFrom='" + pathLoadedFrom + "'"
        + ", isVariadic=" + isVariadic
        + '}';
  }

  public Kudf newInstance(final KsqlConfig ksqlConfig) {
    return udfFactory.apply(ksqlConfig);
  }
}