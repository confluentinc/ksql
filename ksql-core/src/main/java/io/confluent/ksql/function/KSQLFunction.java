/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function;

import org.apache.kafka.connect.data.Schema;

import java.util.List;

public class KQLFunction {

  final Schema returnType;
  final List<Schema> arguments;
  final String functionName;
  final Class kudfClass;

  public KQLFunction(Schema returnType, List<Schema> arguments, String functionName,
                     Class kudfClass) {
    this.returnType = returnType;
    this.arguments = arguments;
    this.functionName = functionName;
    this.kudfClass = kudfClass;
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


  public Class getKudfClass() {
    return kudfClass;
  }
}
