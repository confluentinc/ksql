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

  private final Schema returnType;
  private final List<Schema> arguments;
  private final String functionName;
  private final Class<? extends Kudf> kudfClass;
  private final Supplier<Kudf> udfSupplier;

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
    });

  }

  KsqlFunction(final Schema returnType,
               final List<Schema> arguments,
               final String functionName,
               final Class<? extends Kudf> kudfClass,
               final Supplier<Kudf> udfSupplier) {
    Objects.requireNonNull(returnType, "returnType can't be null");
    Objects.requireNonNull(arguments, "arguments can't be null");
    Objects.requireNonNull(functionName, "functionName can't be null");
    Objects.requireNonNull(kudfClass, "kudfClass can't be null");
    this.returnType = returnType;
    this.arguments = arguments;
    this.functionName = functionName;
    this.kudfClass = kudfClass;
    this.udfSupplier = udfSupplier;
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


  public Class<? extends Kudf> getKudfClass() {
    return kudfClass;
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
        && Objects.equals(kudfClass, that.kudfClass);
  }

  @Override
  public int hashCode() {
    return Objects.hash(returnType, arguments, functionName, kudfClass);
  }

  @Override
  public String toString() {
    return "KsqlFunction{"
        + "returnType=" + returnType
        + ", arguments=" + arguments.stream().map(Schema::type).collect(Collectors.toList())
        + ", functionName='" + functionName + '\''
        + ", kudfClass=" + kudfClass
        + '}';
  }

  public Kudf newInstance() {
    return udfSupplier.get();
  }
}