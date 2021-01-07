/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.util;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.function.Function;

/**
 * Literal helpers
 */
public final class Literals {

  private static final ImmutableMap<SqlBaseType, Function<Object, ? extends Literal>> FACTORIES =
      ImmutableMap.<SqlBaseType, Function<Object, ? extends Literal>>builder()
          .put(SqlBaseType.BOOLEAN, v -> new BooleanLiteral((Boolean) v))
          .put(SqlBaseType.INTEGER, v -> new IntegerLiteral((Integer) v))
          .put(SqlBaseType.BIGINT, v -> new LongLiteral((Long) v))
          .put(SqlBaseType.DECIMAL, v -> new DecimalLiteral((BigDecimal) v))
          .put(SqlBaseType.DOUBLE, v -> new DoubleLiteral((Double) v))
          .put(SqlBaseType.STRING, v -> new StringLiteral((String) v))
          .put(SqlBaseType.TIMESTAMP, v -> new TimestampLiteral((Timestamp) v))
          .build();

  private Literals() {
  }

  /**
   * Get a factory method to create literals for the supplied {@code sqlType}.
   *
   * <p>Not all sql types have a literal class. Invoking with those without a literal class will
   * result in an exception.
   *
   * @param sqlType the base type to retrieve a factory for.
   * @return the factory method.
   */
  public static Function<Object, ? extends Literal> getFactory(final SqlBaseType sqlType) {
    final Function<Object, ? extends Literal> factory = FACTORIES.get(sqlType);
    if (factory == null) {
      throw new IllegalArgumentException("No literal type registered for " + sqlType);
    }
    return factory;
  }
}
