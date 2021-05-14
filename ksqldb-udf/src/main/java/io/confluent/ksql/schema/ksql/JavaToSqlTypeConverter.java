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

import io.confluent.ksql.schema.ksql.types.SqlBaseType;

public interface JavaToSqlTypeConverter {

  /**
   * Convert the supplied {@code javaType} to its corresponding SQL type.
   *
   * <p/>Structured types are not supported
   *
   * @param javaType the java type to convert.
   * @return the sql type.
   */
  SqlBaseType toSqlType(Class<?> javaType);

  static JavaToSqlTypeConverter instance() {
    return new JavaToSqlConverter();
  }
}
