/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.function;

import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlException;

public class KsqlFunction {

  static final String INTERNAL_PATH = "internal";
  private final Schema returnType;
  private final List<Schema> arguments;
  private final String functionName;
  private final Class<? extends Kudf> kudfClass;
  private final Supplier<Kudf> udfSupplier;
  private final String description;
  private final String pathLoadedFrom;

  public KsqlFunction(final Schema returnType,
                      final List<Schema> arguments,
                      final String functionName,
                      final Class<? extends Kudf> kudfClass) {
    this(returnType, arguments, functionName, kudfClass, () -> {
      try {
        return kudfClass.newInstance();
      } catch (Exception e) {
        throw new KsqlException("Failed to create instance of kudfClass "
             + kudfClass
             + " for function "  + functionName, e);
      }
    }, "", INTERNAL_PATH);

  }

  KsqlFunction(final Schema returnType,
               final List<Schema> arguments,
               final String functionName,
               final Class<? extends Kudf> kudfClass,
               final Supplier<Kudf> udfSupplier,
               final String description,
               final String pathLoadedFrom) {
    this.returnType = Objects.requireNonNull(returnType, "returnType can't be null");
    this.arguments = Objects.requireNonNull(arguments, "arguments can't be null");
    this.functionName = Objects.requireNonNull(functionName, "functionName can't be null");
    this.kudfClass = Objects.requireNonNull(kudfClass, "kudfClass can't be null");
    this.udfSupplier = Objects.requireNonNull(udfSupplier, "udfSupplier can't be null");
    this.description = Objects.requireNonNull(description, "description can't be null");
    if (arguments.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("KSQL Function can't have null argument types");
    }
    this.pathLoadedFrom  = Objects.requireNonNull(pathLoadedFrom, "pathLoadedFrom can't be null");
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KsqlFunction that = (KsqlFunction) o;
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

  public Kudf newInstance() {
    return udfSupplier.get();
  }
}