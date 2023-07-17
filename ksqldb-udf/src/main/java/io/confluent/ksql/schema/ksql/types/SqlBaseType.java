/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql.types;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * The SQL types supported by KSQL.
 */
public enum SqlBaseType {
  BOOLEAN, INTEGER, BIGINT, DECIMAL, DOUBLE, STRING, ARRAY, MAP, STRUCT, TIME, DATE, TIMESTAMP,
  BYTES;

  /**
   * @return {@code true} if numeric type.
   */
  public boolean isNumber() {
    return this == INTEGER || this == BIGINT || this == DECIMAL || this == DOUBLE;
  }

  /**
   * @return {@code true} if time type.
   */
  public boolean isTime() {
    return this == TIME || this == DATE || this == TIMESTAMP;
  }

  /**
   * Test to see if this type can be <i>implicitly</i> cast to another.
   *
   * <p>Types can always be cast to themselves. Only numeric types can be implicitly cast to other
   * numeric types. Note: STRING to DECIMAL handling is not seen as casting: it's parsing.
   *
   * @param to the target type.
   * @return true if this type can be implicitly cast to the supplied type.
   */
  public boolean canImplicitlyCast(final SqlBaseType to) {
    if (to == null) {
      return false;
    }
    final boolean canCastNumber = (isNumber() && to.isNumber() && this.ordinal() <= to.ordinal());
    final boolean canCastTime = this.equals(STRING) && to.isTime();
    final boolean canCastBytes = this.equals(STRING) && to == BYTES;
    return this.equals(to) || canCastNumber || canCastTime || canCastBytes;
  }

  public static Stream<SqlBaseType> numbers() {
    return Arrays.stream(values())
        .filter(SqlBaseType::isNumber);
  }
}
