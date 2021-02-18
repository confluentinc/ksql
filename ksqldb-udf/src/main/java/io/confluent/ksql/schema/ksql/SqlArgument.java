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

/**
 * A wrapper class to bundle SqlTypes and SqlLambdas for UDF functions that contain
 * lambdas as an argument. This class allows us to properly find the matching UDF and
 * resolve the return type for lambda UDFs based on the given sqlLambda.
 */
public class SqlArgument {

  private final SqlType sqlType;
  private final SqlLambda sqlLambda;

  public SqlArgument(final SqlType type, final SqlLambda lambda) {
    sqlType = type;
    sqlLambda = lambda;
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

  public SqlType getSqlType() {
    return sqlType;
  }

  public SqlLambda getSqlLambda() {
    return sqlLambda;
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
    if (sqlType != null) {
      return sqlType.toString();
    } else {
      return sqlLambda.toString();
    }
  }
}
