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

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "correlation",
        description = "Computes the Pearson correlation coefficient between two columns of data.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class CorrelationUdaf implements TableUdaf<Pair<Double, Double>, Struct, Double> {
  private static final String X_SUM = "X_SUM";
  private static final String Y_SUM = "Y_SUM";
  private static final String X_SQUARED_SUM = "X_SQUARED_SUM";
  private static final String Y_SQUARED_SUM = "Y_SQUARED_SUM";
  private static final String XY_SUM = "XY_SUM";
  private static final String COUNT = "COUNT";
  private static final Schema structSchema = SchemaBuilder.struct()
          .field(X_SUM, Schema.FLOAT64_SCHEMA)
          .field(Y_SUM, Schema.FLOAT64_SCHEMA)
          .field(X_SQUARED_SUM, Schema.FLOAT64_SCHEMA)
          .field(Y_SQUARED_SUM, Schema.FLOAT64_SCHEMA)
          .field(XY_SUM, Schema.FLOAT64_SCHEMA)
          .field(COUNT, Schema.INT64_SCHEMA)
          .build();

  @UdafFactory(description = "Computes the Pearson correlation coefficient between "
          + "two DOUBLE columns.")
  public static TableUdaf<Pair<Double, Double>, Struct, Double> createCorrelation() {
    return new CorrelationUdaf();
  }

  @Override
  public Struct initialize() {
    return new Struct(structSchema)
            .put(X_SUM, 0.0)
            .put(Y_SUM, 0.0)
            .put(X_SQUARED_SUM, 0.0)
            .put(Y_SQUARED_SUM, 0.0)
            .put(XY_SUM, 0.0)
            .put(COUNT, 0L);
  }

  @Override
  public Struct aggregate(final Pair<Double, Double> current, final Struct aggregate) {
    final Double x = current.getLeft();
    final Double y = current.getRight();

    if (x == null || y == null) {
      return aggregate;
    }

    return new Struct(structSchema)
            .put(X_SUM, aggregate.getFloat64(X_SUM) + x)
            .put(Y_SUM, aggregate.getFloat64(Y_SUM) + y)
            .put(X_SQUARED_SUM, aggregate.getFloat64(X_SQUARED_SUM) + x * x)
            .put(Y_SQUARED_SUM, aggregate.getFloat64(Y_SQUARED_SUM) + y * y)
            .put(XY_SUM, aggregate.getFloat64(XY_SUM) + x * y)
            .put(COUNT, aggregate.getInt64(COUNT) + 1L);
  }

  @Override
  public Struct merge(final Struct aggOne, final Struct aggTwo) {
    return new Struct(structSchema)
            .put(X_SUM, aggOne.getFloat64(X_SUM) + aggTwo.getFloat64(X_SUM))
            .put(Y_SUM, aggOne.getFloat64(Y_SUM) + aggTwo.getFloat64(Y_SUM))
            .put(X_SQUARED_SUM, aggOne.getFloat64(X_SQUARED_SUM) + aggTwo.getFloat64(X_SQUARED_SUM))
            .put(Y_SQUARED_SUM, aggOne.getFloat64(Y_SQUARED_SUM) + aggTwo.getFloat64(Y_SQUARED_SUM))
            .put(XY_SUM, aggOne.getFloat64(XY_SUM) + aggTwo.getFloat64(XY_SUM))
            .put(COUNT, aggOne.getInt64(COUNT) + aggTwo.getInt64(COUNT));
  }

  @Override
  public Double map(final Struct agg) {

    /* These calculations are based on the single-pass correlation formula shown at
    https://www.mathsisfun.com/data/correlation.html. (See "Note for Programmers.") */
    final double sumX = agg.getFloat64(X_SUM);
    final double sumY = agg.getFloat64(Y_SUM);
    final double squaredXSum = agg.getFloat64(X_SQUARED_SUM);
    final double squaredYSum = agg.getFloat64(Y_SQUARED_SUM);
    final double sumXY = agg.getFloat64(XY_SUM);
    final long count = agg.getInt64(COUNT);

    final double numerator = count * sumXY - sumX * sumY;

    final double denominatorX = count * squaredXSum - sumX * sumX;
    final double denominatorY = count * squaredYSum - sumY * sumY;
    final double denominator = Math.sqrt(denominatorX * denominatorY);

    return numerator / denominator;
  }

  @Override
  public Struct undo(final Pair<Double, Double> valueToUndo, final Struct aggregateValue) {
    final Double x = valueToUndo.getLeft();
    final Double y = valueToUndo.getRight();

    if (x == null || y == null) {
      return aggregateValue;
    }

    return new Struct(structSchema)
            .put(X_SUM, aggregateValue.getFloat64(X_SUM) - x)
            .put(Y_SUM, aggregateValue.getFloat64(Y_SUM) - y)
            .put(X_SQUARED_SUM, aggregateValue.getFloat64(X_SQUARED_SUM) - x * x)
            .put(Y_SQUARED_SUM, aggregateValue.getFloat64(Y_SQUARED_SUM) - y * y)
            .put(XY_SUM, aggregateValue.getFloat64(XY_SUM) - x * y)
            .put(COUNT, aggregateValue.getInt64(COUNT) - 1L);
  }
}
