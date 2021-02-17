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

import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Objects;

/**
 * A wrapper class to bundle SqlTypes for UDF functions.
 */
public class SqlArgument {

  private final SqlType sqlType;

  public SqlArgument(final SqlType type) {
    sqlType = type;
  }

  public static SqlArgument of(final SqlType type) {
    return new SqlArgument(type);
  }

  public SqlType getSqlType() {
    return sqlType;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sqlType);
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
    return that.sqlType == this.sqlType;
  }
}
