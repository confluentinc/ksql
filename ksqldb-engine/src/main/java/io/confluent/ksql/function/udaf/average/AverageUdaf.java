/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udaf.average;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "avg",
    description = "Returns the average value of the column computed as the sum divided by the"
        + "count. Applicable only to numeric types.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class AverageUdaf {

  private static final String COUNT = "COUNT";
  private static final String SUM = "SUM";
  private static final Schema STRUCT_LONG = SchemaBuilder.struct().optional()
      .field(SUM, Schema.OPTIONAL_INT64_SCHEMA)
      .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
      .build();
  private static final Schema STRUCT_INT = SchemaBuilder.struct().optional()
      .field(SUM, Schema.OPTIONAL_INT32_SCHEMA)
      .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
      .build();
  private static final Schema STRUCT_DOUBLE = SchemaBuilder.struct().optional()
      .field(SUM, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
      .build();


  private AverageUdaf() {
  }


  @UdafFactory(description = "Compute average of column with type Long.",
      aggregateSchema = "STRUCT<SUM bigint, COUNT bigint>")
  public static TableUdaf<Long, Struct, Double> averageLong() {
    return getAverageImplementation(
        0L,
        STRUCT_LONG,
        (sum, newValue) -> sum.getInt64(SUM) + newValue,
        (sum, count) -> sum.getInt64(SUM) / count,
        (sum1, sum2) -> sum1.getInt64(SUM) + sum2.getInt64(SUM),
        (sum, valueToUndo) -> sum.getInt64(SUM) - valueToUndo);
  }

  @UdafFactory(description = "Compute average of column with type Integer.",
      aggregateSchema = "STRUCT<SUM integer, COUNT bigint>")
  public static TableUdaf<Integer, Struct, Double> averageInt() {

    return getAverageImplementation(
        0,
        STRUCT_INT,
        (sum, newValue) -> sum.getInt32(SUM) + newValue,
        (sum, count) -> sum.getInt32(SUM) / count,
        (sum1, sum2) -> sum1.getInt32(SUM) + sum2.getInt32(SUM),
        (sum, valueToUndo) -> sum.getInt32(SUM) - valueToUndo);
  }

  @UdafFactory(description = "Compute average of column with type Double.",
      aggregateSchema = "STRUCT<SUM double, COUNT bigint>")
  public static TableUdaf<Double, Struct, Double> averageDouble() {

    return getAverageImplementation(
        0.0,
        STRUCT_DOUBLE,
        (sum, newValue) -> sum.getFloat64(SUM) + newValue,
        (sum, count) -> sum.getFloat64(SUM) / count,
        (sum1, sum2) -> sum1.getFloat64(SUM) + sum2.getFloat64(SUM),
        (sum, valueToUndo) -> sum.getFloat64(SUM) - valueToUndo);
  }


  private static <I> TableUdaf<I, Struct, Double> getAverageImplementation(
      final I initialValue,
      final Schema structSchema,
      final BiFunction<Struct, I, I> adder,
      final BiFunction<Struct, Double, Double> mapper,
      final BiFunction<Struct, Struct, I> merger,
      final BiFunction<Struct, I, I> subtracter) {

    return new TableUdaf<I, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(structSchema).put(SUM, initialValue).put(COUNT, 0L);
      }

      @Override
      public Struct aggregate(final I newValue,
                              final Struct aggregate) {

        if (newValue == null) {
          return aggregate;
        }
        return new Struct(structSchema)
            .put(SUM, adder.apply(aggregate, newValue))
            .put(COUNT, aggregate.getInt64(COUNT) + 1);

      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return mapper.apply(aggregate,((double)count));
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(structSchema)
            .put(SUM, merger.apply(agg1, agg2))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }

      @Override
      public Struct undo(final I valueToUndo,
                         final Struct aggregate) {
        if (valueToUndo == null) {
          return aggregate;
        }
        return new Struct(structSchema)
            .put(SUM, subtracter.apply(aggregate, valueToUndo))
            .put(COUNT, aggregate.getInt64(COUNT) - 1);
      }
    };
  }
}
