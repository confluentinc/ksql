/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf.stddev;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "STDDEV_SAMPLE",
    description = "Returns the sample standard deviation of the column. "
        +
        "Applicable only to numeric types.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class StandardDeviationSampleUdaf {

  private static final String COUNT = "COUNT";
  private static final String SUM = "SUM";
  private static final String M2 = "M2";
  private static final Schema STRUCT_LONG = SchemaBuilder.struct().optional()
      .field(SUM, Schema.OPTIONAL_INT64_SCHEMA)
      .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
      .field(M2, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build();

  private static final Schema STRUCT_INT = SchemaBuilder.struct().optional()
      .field(SUM, Schema.OPTIONAL_INT32_SCHEMA)
      .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
      .field(M2, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build();

  private static final Schema STRUCT_DOUBLE = SchemaBuilder.struct().optional()
      .field(SUM, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
      .field(M2, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build();

  private StandardDeviationSampleUdaf() {
  }

  @UdafFactory(description = "Compute sample standard deviation of column with type Long.",
      aggregateSchema = "STRUCT<SUM bigint, COUNT bigint, M2 double>")
  public static TableUdaf<Long, Struct, Double> stdDevLong() {
    return getStdDevImplementation(
        0L,
        STRUCT_LONG,
        (agg, newValue) -> newValue + agg.getInt64(SUM),
        (agg, newValue) ->
            Double.valueOf(newValue * (agg.getInt64(COUNT) + 1) - (agg.getInt64(SUM) + newValue)),
        (agg1, agg2) ->
            agg1.getInt64(SUM).doubleValue() / agg1.getInt64(COUNT).doubleValue()
                - agg2.getInt64(SUM).doubleValue() / agg2.getInt64(COUNT).doubleValue(),
        (agg1, agg2) -> agg1.getInt64(SUM) + agg2.getInt64(SUM),
        (agg, valueToRemove) -> agg.getInt64(SUM) - valueToRemove);
  }

  @UdafFactory(description = "Compute sample standard deviation of column with type Integer.",
      aggregateSchema = "STRUCT<SUM integer, COUNT bigint, M2 double>")
  public static TableUdaf<Integer, Struct, Double> stdDevInt() {
    return getStdDevImplementation(
        0,
        STRUCT_INT,
        (agg, newValue) -> newValue + agg.getInt32(SUM),
        (agg, newValue) ->
            Double.valueOf(newValue * (agg.getInt64(COUNT) + 1) - (agg.getInt32(SUM) + newValue)),
        (agg1, agg2) ->
            agg1.getInt32(SUM).doubleValue() / agg1.getInt64(COUNT).doubleValue()
                - agg2.getInt32(SUM).doubleValue() / agg2.getInt64(COUNT).doubleValue(),
        (agg1, agg2) -> agg1.getInt32(SUM) + agg2.getInt32(SUM),
        (agg, valueToRemove) -> agg.getInt32(SUM) - valueToRemove);
  }

  @UdafFactory(description = "Compute sample standard deviation of column with type Double.",
      aggregateSchema = "STRUCT<SUM double, COUNT bigint, M2 double>")
  public static TableUdaf<Double, Struct, Double> stdDevDouble() {
    return getStdDevImplementation(
        0.0,
        STRUCT_DOUBLE,
        (agg, newValue) -> newValue + agg.getFloat64(SUM),
        (agg, newValue) -> newValue * (agg.getInt64(COUNT) + 1) - (agg.getFloat64(SUM) + newValue),
        (agg1, agg2) ->
            agg1.getFloat64(SUM) / agg1.getInt64(COUNT)
                - agg2.getFloat64(SUM) / agg2.getInt64(COUNT),
        (agg1, agg2) -> agg1.getFloat64(SUM) + agg2.getFloat64(SUM),
        (agg, valueToRemove) -> agg.getFloat64(SUM) - valueToRemove);
  }

  private static <I> TableUdaf<I, Struct, Double> getStdDevImplementation(
      final I initialValue,
      final Schema structSchema,
      final BiFunction<Struct, I, I> add,
      final BiFunction<Struct, I, Double> createDelta,
      final BiFunction<Struct, Struct, Double> mergeInner,
      final BiFunction<Struct, Struct, I> mergeSum,
      final BiFunction<Struct, I, I> undoSum) {
    return new TableUdaf<I, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(structSchema).put(SUM, initialValue).put(COUNT, 0L).put(M2, 0.0);
      }

      @Override
      public Struct aggregate(final I newValue, final Struct aggregate) {
        // Uses the Youngs-Cramer algorithm to calculate standard deviation
        if (newValue == null) {
          return aggregate;
        }
        final long newCount = aggregate.getInt64(COUNT) + 1;
        final double newM2;

        if (newCount - 1 > 0) {
          final double delta = createDelta.apply(aggregate, newValue);
          newM2 = delta * delta / (newCount * (newCount - 1));
        } else {
          // processing the first item
          newM2 = 0;
        }

        return new Struct(structSchema)
            .put(COUNT, newCount)
            .put(SUM, add.apply(aggregate, newValue))
            .put(M2, newM2 + aggregate.getFloat64(M2));
      }

      @Override
      public Struct merge(final Struct aggOne, final Struct aggTwo) {
        final long countOne = aggOne.getInt64(COUNT);
        final long countTwo = aggTwo.getInt64(COUNT);

        final double m2One = aggOne.getFloat64(M2);
        final double m2Two = aggTwo.getFloat64(M2);
        final long newCount = countOne + countTwo;
        final double newM2;

        if (countOne == 0 || countTwo == 0) {
          newM2 = m2One + m2Two;
        } else {
          final double innerCalc = mergeInner.apply(aggOne, aggTwo);
          newM2 = m2One + m2Two + countOne * countTwo * innerCalc * innerCalc / newCount;
        }

        return new Struct(structSchema)
            .put(COUNT, newCount)
            .put(SUM, mergeSum.apply(aggOne, aggTwo))
            .put(M2, newM2);
      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count < 2) {
          return 0.0;
        }
        return Math.sqrt(aggregate.getFloat64(M2) / (count - 1));
      }

      @Override
      public Struct undo(final I valueToUndo, final Struct aggregate) {
        if (valueToUndo == null) {
          return aggregate;
        }
        final long newCount = aggregate.getInt64(COUNT) - 1;
        final double newM2;
        if (newCount > 0) {
          final double delta = createDelta.apply(aggregate, valueToUndo);
          newM2 = delta * delta / (newCount * (newCount + 1));
        } else {
          newM2 = 0;
        }
        return new Struct(structSchema)
            .put(COUNT, newCount)
            .put(SUM, undoSum.apply(aggregate, valueToUndo))
            .put(M2, aggregate.getFloat64(M2) - newM2);
      }
    };
  }
}
