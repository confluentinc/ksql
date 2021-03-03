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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.types.SqlType;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class TypeContext {
  private SqlType sqlType;
  private final Map<String, SqlType> lambdaInputTypeMapping;

  public TypeContext() {
    this(new HashMap<>());
  }

  public TypeContext(
      final Map<String, SqlType> lambdaInputTypeMapping) {
    this.lambdaInputTypeMapping = lambdaInputTypeMapping;
  }

  public SqlType getSqlType() {
    return sqlType;
  }

  public void setSqlType(final SqlType sqlType) {
    this.sqlType = sqlType;
  }

  public SqlType getLambdaType(final String name) {
    return lambdaInputTypeMapping.get(name);
  }

  public TypeContext getCopy() {
    return new TypeContext(
        new HashMap<>(this.lambdaInputTypeMapping)
    );
  }

  public TypeContext copyWithLambdaVariableTypeMapping(final Map<String, SqlType> variableToType) {
    final HashMap<String, SqlType> mapping = new HashMap<>(lambdaInputTypeMapping);
    for (final Entry<String, SqlType> entry : variableToType.entrySet()) {
      final String key = entry.getKey();
      if (mapping.containsKey(key) && !mapping.get(key).equals(variableToType.get(key))) {
        throw new IllegalStateException(String.format(
            "Could not resolve type for lambda variable %s, "
                + "cannot be both %s and %s",
            key, mapping.get(key).toString(), variableToType.get(key).toString()));
      }
      mapping.put(key, entry.getValue());
    }
    return new TypeContext(ImmutableMap.copyOf(mapping));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TypeContext context = (TypeContext) o;
    return lambdaInputTypeMapping.equals(context.lambdaInputTypeMapping);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lambdaInputTypeMapping);
  }
}
