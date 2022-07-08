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

import com.google.common.collect.ImmutableList;
import java.util.Collection;

public final class SqlTypes {

  private SqlTypes() {
  }

  public static final SqlPrimitiveType BOOLEAN = SqlPrimitiveType.of(SqlBaseType.BOOLEAN);
  public static final SqlPrimitiveType INTEGER = SqlPrimitiveType.of(SqlBaseType.INTEGER);
  public static final SqlPrimitiveType BIGINT = SqlPrimitiveType.of(SqlBaseType.BIGINT);
  public static final SqlPrimitiveType DOUBLE = SqlPrimitiveType.of(SqlBaseType.DOUBLE);
  public static final SqlPrimitiveType STRING = SqlPrimitiveType.of(SqlBaseType.STRING);
  public static final SqlPrimitiveType TIME = SqlPrimitiveType.of(SqlBaseType.TIME);
  public static final SqlPrimitiveType DATE = SqlPrimitiveType.of(SqlBaseType.DATE);
  public static final SqlPrimitiveType TIMESTAMP = SqlPrimitiveType.of(SqlBaseType.TIMESTAMP);
  public static final SqlPrimitiveType BYTES = SqlPrimitiveType.of(SqlBaseType.BYTES);

  public static final Collection<SqlPrimitiveType> ALL = ImmutableList.of(
      BOOLEAN,
      INTEGER,
      BIGINT,
      DOUBLE,
      STRING,
      TIME,
      DATE,
      TIMESTAMP,
      BYTES);

  public static SqlDecimal decimal(final int precision, final int scale) {
    return SqlDecimal.of(precision, scale);
  }

  public static SqlArray array(final SqlType elementType) {
    return SqlArray.of(elementType);
  }

  public static SqlMap map(final SqlType keyType, final SqlType valueType) {
    return SqlMap.of(keyType, valueType);
  }

  public static SqlStruct.Builder struct() {
    return SqlStruct.builder();
  }

  /**
   * Schema of an INT up-cast to a DECIMAL
   */
  public static final SqlDecimal INT_UPCAST_TO_DECIMAL = SqlDecimal.of(10, 0);

  /**
   * Schema of an BIGINT up-cast to a DECIMAL
   */
  public static final SqlDecimal BIGINT_UPCAST_TO_DECIMAL = SqlDecimal.of(19, 0);
}
