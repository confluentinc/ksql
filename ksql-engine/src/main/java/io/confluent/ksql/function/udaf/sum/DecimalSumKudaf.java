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

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.TableAggregationFunction;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public class DecimalSumKudaf
    extends BaseAggregateFunction<BigDecimal, BigDecimal>
    implements TableAggregationFunction<BigDecimal, BigDecimal> {

  private final MathContext context;

  DecimalSumKudaf(
      final String functionName,
      final int argIndexInValue,
      final Schema returnSchema
  ) {
    super(
        functionName,
        argIndexInValue,
        DecimalSumKudaf::initialValue,
        returnSchema,
        Collections.singletonList(returnSchema),
        "Computes the sum of decimal values for a key, resulting in a decimal with the same "
            + "precision and scale.");
    context = new MathContext(DecimalUtil.precision(returnSchema));
  }

  @Override
  public KsqlAggregateFunction<BigDecimal, BigDecimal> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    return new DecimalSumKudaf(
        functionName, aggregateFunctionArguments.udafIndex(), getReturnType());
  }

  @Override
  public BigDecimal aggregate(final BigDecimal currentValue, final BigDecimal aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }

    return aggregateValue.add(currentValue, context);
  }

  @Override
  public Merger<Struct, BigDecimal> getMerger() {
    return (key, agg1, agg2) -> agg1.add(agg2, context);
  }

  @Override
  public BigDecimal undo(final BigDecimal valueToUndo, final BigDecimal aggregateValue) {
    if (valueToUndo == null) {
      return aggregateValue;
    }
    return aggregateValue.subtract(valueToUndo, context);
  }

  private static BigDecimal initialValue() {
    return BigDecimal.ZERO;
  }
}
