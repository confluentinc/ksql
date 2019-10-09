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

package io.confluent.ksql.schema.ksql;

/**
 * The SQL types supported by KSQL.
 */
public enum SqlBaseType {
  BOOLEAN, INTEGER, BIGINT, DECIMAL, DOUBLE, STRING, ARRAY, MAP, STRUCT;

  /**
   * @return {@code true} if numeric type.
   */
  public boolean isNumber() {
    return this == INTEGER || this == BIGINT || this == DECIMAL || this == DOUBLE;
  }

  /**
   * Test to see if this type can be up-cast to another.
   *
   * <p>This defines if KSQL supports <i>implicitly</i> converting one numeric type to another.
   *
   * <p>Types can always be upcast to themselves. Only numeric types can be upcast to different
   * numeric types. Note: STRING to DECIMAL handling is not seen as up-casting, it's parsing.
   *
   * @param to the target type.
   * @return true if this type can be upcast to the supplied type.
   */
  public boolean canUpCast(final SqlBaseType to) {
    return this.equals(to)
        || (isNumber() && to.isNumber() && this.ordinal() <= to.ordinal());
  }
}
