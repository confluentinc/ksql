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
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Schema;

@Immutable
public final class KsqlFunction {

  static final String INTERNAL_PATH = "internal";

  private final Schema returnType;
  private final List<Schema> arguments;
  private final String functionName;
  private final Class<? extends Kudf> kudfClass;
  private final Function<KsqlConfig, Kudf> udfFactory;
  private final String description;
  private final String pathLoadedFrom;

  /**
   * Create built in / legacy function.
   */
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
        returnType, arguments, functionName, kudfClass, udfFactory, "", INTERNAL_PATH);
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
      final String pathLoadedFrom
  ) {
    return new KsqlFunction(
        returnType, arguments, functionName, kudfClass, udfFactory, description, pathLoadedFrom);
  }

  private KsqlFunction(
      final Schema returnType,
      final List<Schema> arguments,
      final String functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KsqlConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom
  ) {
    this.returnType = Objects.requireNonNull(returnType, "returnType");
    this.arguments = ImmutableList.copyOf(Objects.requireNonNull(arguments, "arguments"));
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.kudfClass = Objects.requireNonNull(kudfClass, "kudfClass");
    this.udfFactory = Objects.requireNonNull(udfFactory, "udfFactory");
    this.description = Objects.requireNonNull(description, "description");
    this.pathLoadedFrom  = Objects.requireNonNull(pathLoadedFrom, "pathLoadedFrom");

    if (arguments.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("KSQL Function can't have null argument types");
    }
  }

  public Schema getReturnType() {
    return returnType;
  }

  public List<Schema> getArguments() {
    return arguments;
  }

  String getFunctionName() {
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
        && Objects.equals(arguments, that.arguments)
        && Objects.equals(functionName, that.functionName)
        && Objects.equals(kudfClass, that.kudfClass)
        && Objects.equals(pathLoadedFrom, that.pathLoadedFrom);
  }

  @Override
  public int hashCode() {
    return Objects.hash(returnType, arguments, functionName, kudfClass, pathLoadedFrom);
  }

  @Override
  public String toString() {
    return "KsqlFunction{"
        + "returnType=" + returnType
        + ", arguments=" + arguments.stream().map(Schema::type).collect(Collectors.toList())
        + ", functionName='" + functionName + '\''
        + ", kudfClass=" + kudfClass
        + ", description='" + description + "'"
        + ", pathLoadedFrom='" + pathLoadedFrom + "'"
        + '}';
  }

  public Kudf newInstance(final KsqlConfig ksqlConfig) {
    return udfFactory.apply(ksqlConfig);
  }
}