/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Optional;

public class DecimalSumKudaf implements TableUdaf<BigDecimal, BigDecimal, BigDecimal> {
  private SqlDecimal resultSchema;
  private MathContext context;
  private BigDecimal maxValue;
  private int precision;
  private int scale;
  private int digits;

  @Override
  public void initializeTypeArguments(final List<SqlArgument> argTypeList) {
    resultSchema = (SqlDecimal) argTypeList.get(0).getSqlTypeOrThrow();
    context = new MathContext(resultSchema.getPrecision());
    precision = resultSchema.getPrecision();
    scale = resultSchema.getScale();
    digits = resultSchema.getPrecision() - resultSchema.getScale();
    maxValue = BigDecimal.valueOf(Math.pow(10, digits));
  }

  @Override
  public BigDecimal initialize() {
    return BigDecimal.ZERO;
  }

  @Override
  public Optional<SqlType> getAggregateSqlType() {
    return Optional.of(resultSchema);
  }

  @Override
  public Optional<SqlType> getReturnSqlType() {
    return Optional.of(resultSchema);
  }

  @Override
  public BigDecimal aggregate(final BigDecimal currentValue, final BigDecimal aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }

    final BigDecimal value = aggregateValue.add(currentValue, context);

    if (maxValue.compareTo(value.abs()) < 1) {
      throw new KsqlFunctionException(
          String.format("Numeric field overflow: A field with precision %d and scale %d "
                  + "must round to an absolute value less than 10^%d. Got %s",
              precision,
              scale,
              digits,
              value.toPlainString()));
    }

    return value;
  }

  @Override
  public BigDecimal merge(final BigDecimal aggOne, final BigDecimal aggTwo) {
    return aggOne.add(aggTwo, context);
  }

  @Override
  public BigDecimal map(final BigDecimal agg) {
    return agg;
  }

  @Override
  public BigDecimal undo(final BigDecimal valueToUndo, final BigDecimal aggregateValue) {
    if (valueToUndo == null) {
      return aggregateValue;
    }
    return aggregateValue.subtract(valueToUndo, context);
  }
}
