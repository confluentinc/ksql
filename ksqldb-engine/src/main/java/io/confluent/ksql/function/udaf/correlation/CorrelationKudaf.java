/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.correlation;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.ParameterInfo;
import io.confluent.ksql.function.udaf.VariadicArgs;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.Pair;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.Merger;

public class CorrelationKudaf
        extends BaseAggregateFunction<Pair<Double, VariadicArgs<Double>>, Double, Double> {

  CorrelationKudaf(
          final String functionName,
          final List<Integer> argIndicesInValue
  ) {
    super(
            functionName,
            argIndicesInValue,
            () -> 0.0,
            SqlTypes.DOUBLE,
            SqlTypes.DOUBLE,
            ImmutableList.of(
                    new ParameterInfo(
                            "value",
                            SchemaConverters.sqlToFunctionConverter()
                                    .toFunctionType(SqlTypes.DOUBLE),
                            "the value to aggregate",
                            false
                    ),
                    new ParameterInfo(
                            "value2",
                            SchemaConverters.sqlToFunctionConverter()
                                    .toFunctionType(SqlTypes.DOUBLE),
                            "the value to aggregate",
                            true
                    )
            ),
            "Computes the maximum " + SqlTypes.DOUBLE.toString(FormatOptions.none())
                    + " value for a key."
    );
  }

  @Override
  public List<Integer> getArgIndicesInValue() {
    return super.getArgIndicesInValue();
  }

  @Override
  public Double aggregate(final Pair<Double, VariadicArgs<Double>> currentValue,
                          final Double aggregateValue) {
    return aggregateValue + currentValue.getLeft() + currentValue.getRight().get(0);
  }

  @Override
  public Merger<GenericKey, Double> getMerger() {
    return (key, a, b) -> a + b;
  }

  @Override
  public Function<Double, Double> getResultMapper() {
    return Function.identity();
  }
}
