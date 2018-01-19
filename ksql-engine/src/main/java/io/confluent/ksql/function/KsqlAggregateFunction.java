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
import org.apache.kafka.streams.kstream.Merger;

import java.util.List;
import java.util.Map;

import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.KsqlException;

public abstract class KsqlAggregateFunction<V, A> {

  private final int argIndexInValue;
  private final A intialValue;
  private final Schema returnType;
  private final List<Schema> arguments;
  private final String functionName;
  private final Class kudafClass;

  public KsqlAggregateFunction(Integer argIndexInValue) {
    this.argIndexInValue = argIndexInValue;
    this.intialValue = null;
    this.returnType = null;
    this.arguments = null;
    this.functionName = null;
    this.kudafClass = null;
  }

  public KsqlAggregateFunction(
      int argIndexInValue,
      A intialValue,
      Schema returnType,
      List<Schema> arguments,
      String functionName,
      Class kudafClass
  ) {
    this.argIndexInValue = argIndexInValue;
    this.intialValue = intialValue;
    this.returnType = returnType;
    this.arguments = arguments;
    this.functionName = functionName;
    this.kudafClass = kudafClass;
  }

  public abstract KsqlAggregateFunction<V, A> getInstance(
      final Map<String, Integer> expressionNames,
      final List<Expression> functionArguments
  );

  public boolean hasSameArgTypes(List<Schema> argTypeList) {
    if (argTypeList == null) {
      throw new KsqlException("Argument type list is null.");
    }
    return this.arguments.equals(argTypeList);
  }

  public abstract A aggregate(V currentVal, A currentAggVal);

  public A getIntialValue() {
    return intialValue;
  }

  public int getArgIndexInValue() {
    return argIndexInValue;
  }

  public Schema getReturnType() {
    return returnType;
  }

  public List<Schema> getArguments() {
    return arguments;
  }

  public abstract Merger<String, A> getMerger();
}
