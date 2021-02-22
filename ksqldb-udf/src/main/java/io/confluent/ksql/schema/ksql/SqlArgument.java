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

package io.confluent.ksql.schema.ksql;

import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Objects;
import java.util.Optional;

/**
 * A wrapper class to bundle SqlTypes and SqlLambdas for UDF functions that contain
 * lambdas as an argument. This class allows us to properly find the matching UDF and
 * resolve the return type for lambda UDFs based on the given sqlLambda.
 */
public class SqlArgument {

  private final Optional<SqlType> sqlType;
  private final Optional<SqlLambda> sqlLambda;

  public SqlArgument(final SqlType type, final SqlLambda lambda) {
    if (type != null && lambda != null) {
      throw new RuntimeException(
          "A function argument was assigned to be both a type and a lambda");
    }
    sqlType = Optional.ofNullable(type);
    sqlLambda = Optional.ofNullable(lambda);
  }

  public static SqlArgument of(final SqlType type) {
    return new SqlArgument(type, null);
  }

  public static SqlArgument of(final SqlLambda type) {
    return new SqlArgument(null, type);
  }

  public static SqlArgument of(final SqlType sqlType, final SqlLambda lambdaType) {
    return new SqlArgument(sqlType, lambdaType);
  }

  public Optional<SqlType> getSqlType() {
    return sqlType;
  }

  public SqlType getSqlTypeOrThrow() {
    if (sqlLambda.isPresent()) {
      throw new RuntimeException("Was expecting type as a function argument");
    }
    // we represent the null type with a null SqlType
    return sqlType.orElse(null);
  }

  public Optional<SqlLambda> getSqlLambda() {
    return sqlLambda;
  }

  public SqlLambda getSqlLambdaOrThrow() {
    if (sqlType.isPresent()) {
      throw new RuntimeException("Was expecting lambda as a function argument");
    }
    if (sqlLambda.isPresent()) {
      return sqlLambda.get();
    }
    throw new RuntimeException("Was expecting lambda as a function argument");
  }

  @Override
  public int hashCode() {
    return Objects.hash(sqlType, sqlLambda);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlArgument that = (SqlArgument) o;
    return Objects.equals(sqlType, that.sqlType)
        && Objects.equals(sqlLambda, that.sqlLambda);
  }

  @Override
  public String toString() {
    if (sqlType.isPresent()) {
      return sqlType.get().toString();
    }
    return sqlLambda.map(SqlLambda::toString).orElse("null");
  }
}
