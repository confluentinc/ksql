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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public final class DefaultSqlValueCoercer implements SqlValueCoercer {

  private static final Map<SqlType, Function<Number, Number>> UPCASTER =
      ImmutableMap.<SqlType, Function<Number, Number>>builder()
          .put(SqlType.INTEGER, Number::intValue)
          .put(SqlType.BIGINT, Number::longValue)
          .put(SqlType.DOUBLE, Number::doubleValue)
          .build();

  public Optional<Object> coerce(final Object value, final SqlType targetSqlType) {
    final SqlType valueSqlType = SchemaConverters.javaToSqlConverter()
        .toSqlType(value.getClass());

    if (valueSqlType.equals(targetSqlType)) {
      return Optional.of(value);
    }

    if (!(value instanceof Number) || !valueSqlType.canUpCast(targetSqlType)) {
      return Optional.empty();
    }

    return Optional.of(UPCASTER.get(targetSqlType).apply((Number) value));
  }
}
