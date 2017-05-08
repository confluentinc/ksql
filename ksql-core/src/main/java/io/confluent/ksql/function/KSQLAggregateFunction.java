/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function;

import org.apache.kafka.connect.data.Schema;

import java.util.List;

public abstract class KQLAggregateFunction<V, A> {

  final int argIndexInValue;
  public final A intialValue;
  final Schema returnType;
  final List<Schema> arguments;
  final String functionName;
  final Class kudafClass;

  public KQLAggregateFunction(Integer argIndexInValue) {
    this.argIndexInValue = argIndexInValue;
    this.intialValue = null;
    this.returnType = null;
    this.arguments = null;
    this.functionName = null;
    this.kudafClass = null;
  };

  public KQLAggregateFunction(int argIndexInValue, A intialValue, Schema returnType, List<Schema> arguments, String functionName,
                              Class kudafClass) {
    this.argIndexInValue = argIndexInValue;
    this.intialValue = intialValue;
    this.returnType = returnType;
    this.arguments = arguments;
    this.functionName = functionName;
    this.kudafClass = kudafClass;
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

  public String getFunctionName() {
    return functionName;
  }

  public Class getKudafClass() {
    return kudafClass;
  }
}
