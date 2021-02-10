package io.confluent.ksql.execution.codegen;

import io.confluent.ksql.schema.ksql.types.SqlType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeContext {
  private SqlType sqlType;
  private final List<SqlType> inputTypes = new ArrayList<>();
  private final Map<String, SqlType> lambdaTypeMapping = new HashMap<>();

  public SqlType getSqlType() {
    return sqlType;
  }

  public void setSqlType(final SqlType sqlType) {
    this.sqlType = sqlType;
  }

  public List<SqlType> getInputTypes() {
    if (inputTypes.size() == 0) {
      return null;
    }
    return inputTypes;
  }

  public void addInputType(final SqlType inputType) {
    this.inputTypes.add(inputType);
  }

  public void mapInputTypes(final List<String> argumentList) {
    if (inputTypes.size() != argumentList.size()) {
      throw new IllegalArgumentException("Was expecting " +
          inputTypes.size() +
          " arguments but found " +
          argumentList.size() + "," + argumentList +
          ". Check your lambda statement.");
    }
    for (int i = 0; i < argumentList.size(); i++) {
      this.lambdaTypeMapping.putIfAbsent(argumentList.get(i), inputTypes.get(i));
    }
  }

  public SqlType getLambdaType(final String name) {
    return lambdaTypeMapping.get(name);
  }

  Map<String, SqlType> getLambdaTypeMapping() {
    return this.lambdaTypeMapping;
  }

  public Boolean notAllInputsSeen() {
    return lambdaTypeMapping.size() != inputTypes.size() || inputTypes.size() == 0;
  }
}
