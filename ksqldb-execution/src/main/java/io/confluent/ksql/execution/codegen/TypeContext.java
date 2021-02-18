/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.codegen;

import io.confluent.ksql.schema.ksql.types.SqlType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeContext {
  private SqlType sqlType;
  private final List<SqlType> lambdaInputTypes = new ArrayList<>();
  private final Map<String, SqlType> lambdaInputTypeMapping = new HashMap<>();

  public SqlType getSqlType() {
    return sqlType;
  }

  public void setSqlType(final SqlType sqlType) {
    this.sqlType = sqlType;
  }

  public List<SqlType> getLambdaInputTypes() {
    return lambdaInputTypes;
  }

  public void addLambdaInputType(final SqlType inputType) {
    this.lambdaInputTypes.add(inputType);
  }

  public void mapLambdaInputTypes(final List<String> argumentList) {
    if (lambdaInputTypes.size() != argumentList.size()) {
      throw new IllegalArgumentException("Was expecting "
          + lambdaInputTypes.size()
          + " arguments but found "
          + argumentList.size() + ", "
          + argumentList
          + ". Check your lambda statement.");
    }
    for (int i = 0; i < argumentList.size(); i++) {
      this.lambdaInputTypeMapping.putIfAbsent(argumentList.get(i), lambdaInputTypes.get(i));
    }
  }

  public SqlType getLambdaType(final String name) {
    return lambdaInputTypeMapping.get(name);
  }

  public boolean notAllInputsSeen() {
    return lambdaInputTypeMapping.size() != lambdaInputTypes.size() || lambdaInputTypes.size() == 0;
  }
}
