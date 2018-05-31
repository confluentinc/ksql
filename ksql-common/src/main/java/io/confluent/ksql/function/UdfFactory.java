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

import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlException;

public class UdfFactory {
  private final String name;
  private final Class<? extends Kudf> udfClass;
  private final Map<List<Schema.Type>, KsqlFunction> functions = new HashMap<>();

  UdfFactory(final String name, final Class<? extends Kudf> udfClass) {
    this.name = name;
    this.udfClass = udfClass;
  }

  public UdfFactory copy() {
    final UdfFactory udf = new UdfFactory(name, udfClass);
    udf.functions.putAll(functions);
    return udf;
  }

  void addFunction(final KsqlFunction ksqlFunction) {
    final List<Schema.Type> paramTypes = ksqlFunction.getArguments()
        .stream()
        .map(Schema::type).collect(Collectors.toList());

    checkCompatible(ksqlFunction, paramTypes);
    functions.put(paramTypes, ksqlFunction);
  }

  private void checkCompatible(final KsqlFunction ksqlFunction,
                               final List<Schema.Type> paramTypes) {
    if (udfClass != ksqlFunction.getKudfClass()) {
      throw new KsqlException("Can't add function " + ksqlFunction
          + " as a function with the same name exists on a different class " + udfClass);
    }
    if (functions.containsKey(paramTypes)) {
      throw new KsqlException("Can't add function " + ksqlFunction
          + " as a function with the same name and argument types already exists "
          + functions.get(paramTypes));
    }
  }

  @Override
  public String toString() {
    return "UdfFactory{"
        + "name='" + name + '\''
        + ", udfClass=" + udfClass
        + ", functions=" + functions
        + '}';
  }

  public KsqlFunction getFunction(final List<Schema.Type> paramTypes) {
    return functions.get(paramTypes);
  }
}
