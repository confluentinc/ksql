package io.confluent.kql.function;


import org.apache.kafka.connect.data.Schema;

import java.util.List;

public class KQLFunction {

  final Schema.Type returnType;
  final List<Schema.Type> arguments;
  final String functionName;
  final Class kudfClass;

  public  KQLFunction(Schema.Type returnType, List<Schema.Type> arguments, String functionName,
                       Class kudfClass) {
    this.returnType = returnType;
    this.arguments = arguments;
    this.functionName = functionName;
    this.kudfClass = kudfClass;
  }

  public Schema.Type getReturnType() {
    return returnType;
  }

  public List<Schema.Type> getArguments() {
    return arguments;
  }

  public String getFunctionName() {
    return functionName;
  }


  public Class getKudfClass() {
    return kudfClass;
  }
}
