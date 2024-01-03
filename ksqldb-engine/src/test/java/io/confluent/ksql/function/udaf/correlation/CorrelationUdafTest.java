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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.util.Pair;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class CorrelationUdafTest {

  @Test
  public void shouldCorrelateDoublesPerfectPositiveNoNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(1.5, 8.0),
            Pair.of(2.0, 9.0),
            Pair.of(0.5, 6.0),
            Pair.of(1.0, 7.0),
            Pair.of(-3.5, -2.0),
            Pair.of(-1.0, 3.0),
            Pair.of(1.5, 8.0),
            Pair.of(2.5, 10.0),
            Pair.of(-4.5, -4.0),
            Pair.of(-3.0, -1.0),
            Pair.of(3.0, 11.0),
            Pair.of(4.0, 13.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(1.0, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesStrongPositiveNoNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(1.5, 14.0),
            Pair.of(2.0, 11.0),
            Pair.of(5.5, 3.0),
            Pair.of(-5.0, 7.0),
            Pair.of(-3.5, -8.0),
            Pair.of(-1.0, 6.0),
            Pair.of(1.5, 5.0),
            Pair.of(8.5, 19.0),
            Pair.of(-8.0, -6.5),
            Pair.of(-3.0, -2.0),
            Pair.of(-3.0, 12.5),
            Pair.of(4.0, 16.5)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(0.6717, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesWeakPositiveNoNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(5.5, 14.0),
            Pair.of(9.0, 11.0),
            Pair.of(15.5, 3.0),
            Pair.of(-6.0, 7.0),
            Pair.of(-3.5, -8.0),
            Pair.of(-17.0, 6.0),
            Pair.of(9.5, 5.0),
            Pair.of(5.5, 19.0),
            Pair.of(-15.0, -6.5),
            Pair.of(-11.0, -2.0),
            Pair.of(-3.0, 12.5),
            Pair.of(9.0, 16.5)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(0.4890, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesNoCorrelationNoNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(-5.5, 14.0),
            Pair.of(5.5, 14.0),
            Pair.of(-9.0, -11.0),
            Pair.of(9.0, -11.0),
            Pair.of(-2.0, -8.0),
            Pair.of(2.0, -8.0),
            Pair.of(-3.5, 5.0),
            Pair.of(3.5, 5.0),
            Pair.of(-11.0, 6.5),
            Pair.of(11.0, 6.5),
            Pair.of(-3.0, 0.0),
            Pair.of(3.0, 0.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(0.0, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesWeakNegativeNoNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(5.5, -14.0),
            Pair.of(9.0, -11.0),
            Pair.of(15.5, -3.0),
            Pair.of(-6.0, -7.0),
            Pair.of(-3.5, 8.0),
            Pair.of(-17.0, -6.0),
            Pair.of(9.5, -5.0),
            Pair.of(5.5, -19.0),
            Pair.of(-15.0, 6.5),
            Pair.of(-11.0, 2.0),
            Pair.of(-3.0, -12.5),
            Pair.of(9.0, -16.5)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(-0.4890, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesStrongNegativeNoNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(1.5, -14.0),
            Pair.of(2.0, -11.0),
            Pair.of(5.5, -3.0),
            Pair.of(-5.0, -7.0),
            Pair.of(-3.5, 8.0),
            Pair.of(-1.0, -6.0),
            Pair.of(1.5, -5.0),
            Pair.of(8.5, -19.0),
            Pair.of(-8.0, 6.5),
            Pair.of(-3.0, 2.0),
            Pair.of(-3.0, -12.5),
            Pair.of(4.0, -16.5)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(-0.6717, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesPerfectNegativeNoNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(1.5, -8.0),
            Pair.of(2.0, -9.0),
            Pair.of(0.5, -6.0),
            Pair.of(1.0, -7.0),
            Pair.of(-3.5, 2.0),
            Pair.of(-1.0, -3.0),
            Pair.of(1.5, -8.0),
            Pair.of(2.5, -10.0),
            Pair.of(-4.5, 4.0),
            Pair.of(-3.0, 1.0),
            Pair.of(3.0, -11.0),
            Pair.of(4.0, -13.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(-1.0, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesOnlyPositiveValuesNoNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(14.2, 215.0),
            Pair.of(16.4, 325.0),
            Pair.of(11.9, 185.0),
            Pair.of(15.2, 332.0),
            Pair.of(18.5, 406.0),
            Pair.of(22.1, 522.0),
            Pair.of(19.4, 412.0),
            Pair.of(25.1, 614.0),
            Pair.of(23.4, 544.0),
            Pair.of(18.1, 421.0),
            Pair.of(22.6, 445.0),
            Pair.of(17.2, 408.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(0.9575, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesOnlyNegativeValuesNoNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(-14.2, -215.0),
            Pair.of(-16.4, -325.0),
            Pair.of(-11.9, -185.0),
            Pair.of(-15.2, -332.0),
            Pair.of(-18.5, -406.0),
            Pair.of(-22.1, -522.0),
            Pair.of(-19.4, -412.0),
            Pair.of(-25.1, -614.0),
            Pair.of(-23.4, -544.0),
            Pair.of(-18.1, -421.0),
            Pair.of(-22.6, -445.0),
            Pair.of(-17.2, -408.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(0.9575, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesIgnoresNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(14.2, 215.0),
            Pair.of(16.4, 325.0),
            Pair.of(11.9, 185.0),
            Pair.of(15.2, 332.0),
            Pair.of(18.5, 406.0),
            Pair.of(13.1, null),
            Pair.of(22.1, 522.0),
            Pair.of(19.4, 412.0),
            Pair.of(25.1, 614.0),
            Pair.of(23.4, 544.0),
            Pair.of(18.1, 421.0),
            Pair.of(22.6, 445.0),
            Pair.of(null, 620.0),
            Pair.of(17.2, 408.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(0.9575, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesOnlyNulls() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null),
            Pair.of(null, null)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(Double.isNaN(correlation), is(true));
  }

  @Test
  public void shouldCorrelateDoublesOnePoint() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(15.0, 20.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(Double.isNaN(correlation), is(true));
  }

  @Test
  public void shouldCorrelateDoublesTwoPointsPositive() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(15.0, 20.0),
            Pair.of(20.0, 30.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(correlation, closeTo(1.0, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesTwoPointsNegative() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(15.0, 20.0),
            Pair.of(20.0, -30.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(correlation, closeTo(-1.0, 0.00005));
  }

  @Test
  public void shouldCorrelateDoublesSameXValues() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(20.0, 15.0),
            Pair.of(20.0, 10.0),
            Pair.of(20.0, 5.0),
            Pair.of(20.0, 0.0),
            Pair.of(20.0, -5.0),
            Pair.of(20.0, -10.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(Double.isNaN(correlation), is(true));
  }

  @Test
  public void shouldCorrelateDoublesSameYValues() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(15.0, 20.0),
            Pair.of(10.0, 20.0),
            Pair.of(5.0, 20.0),
            Pair.of(0.0, 20.0),
            Pair.of(-5.0, 20.0),
            Pair.of(-10.0, 20.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(Double.isNaN(correlation), is(true));
  }

  @Test
  public void shouldMergeCorrelations() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct leftAgg = udaf.initialize();
    final List<Pair<Double, Double>> leftValues = ImmutableList.of(
            Pair.of(14.2, 215.0),
            Pair.of(16.4, 325.0),
            Pair.of(11.9, 185.0),
            Pair.of(15.2, 332.0),
            Pair.of(18.5, 406.0),
            Pair.of(22.1, 522.0),
            Pair.of(19.4, 412.0),
            Pair.of(25.1, 614.0),
            Pair.of(23.4, 544.0)
    );
    for (final Pair<Double, Double> thisValue : leftValues) {
      leftAgg = udaf.aggregate(thisValue, leftAgg);
    }

    Struct rightAgg = udaf.initialize();
    final List<Pair<Double, Double>> rightValues = ImmutableList.of(
            Pair.of(18.1, 421.0),
            Pair.of(22.6, 445.0),
            Pair.of(17.2, 408.0)
    );
    for (final Pair<Double, Double> thisValue : rightValues) {
      rightAgg = udaf.aggregate(thisValue, rightAgg);
    }

    final double correlation = udaf.map(udaf.merge(leftAgg, rightAgg));

    assertThat(0.9575, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldUndoCorrelations() {
    final TableUdaf<Pair<Double, Double>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationDouble();

    Struct agg = udaf.initialize();
    final List<Pair<Double, Double>> values = ImmutableList.of(
            Pair.of(14.2, 215.0),
            Pair.of(16.4, 325.0),
            Pair.of(11.9, 185.0),
            Pair.of(15.2, 332.0),
            Pair.of(18.5, 406.0),
            Pair.of(23.4, 203.0),
            Pair.of(13.1, null),
            Pair.of(22.1, 522.0),
            Pair.of(19.4, 412.0),
            Pair.of(25.1, 614.0),
            Pair.of(23.4, 544.0),
            Pair.of(18.1, 421.0),
            Pair.of(22.6, 445.0),
            Pair.of(null, 620.0),
            Pair.of(17.2, 408.0)
    );
    for (final Pair<Double, Double> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }

    agg = udaf.undo(Pair.of(23.4, 203.0), agg);
    agg = udaf.undo(Pair.of(13.1, null), agg);
    agg = udaf.undo(Pair.of(null, 620.0), agg);

    final double correlation = udaf.map(agg);

    assertThat(0.9575, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateIntegersIgnoresNulls() {
    final TableUdaf<Pair<Integer, Integer>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationInteger();

    Struct agg = udaf.initialize();
    final List<Pair<Integer, Integer>> values = ImmutableList.of(
            Pair.of(14, 215),
            Pair.of(16, 325),
            Pair.of(11, 185),
            Pair.of(15, 332),
            Pair.of(18, 406),
            Pair.of(13, null),
            Pair.of(22, 522),
            Pair.of(19, 412),
            Pair.of(25, 614),
            Pair.of(23, 544),
            Pair.of(18, 421),
            Pair.of(22, 445),
            Pair.of(null, 620),
            Pair.of(17, 408)
    );
    for (final Pair<Integer, Integer> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(0.9645, closeTo(correlation, 0.00005));
  }

  @Test
  public void shouldCorrelateLongsIgnoresNulls() {
    final TableUdaf<Pair<Long, Long>, Struct, Double> udaf =
            CorrelationUdaf.createCorrelationLong();

    Struct agg = udaf.initialize();
    final List<Pair<Long, Long>> values = ImmutableList.of(
            Pair.of(14L, 215L),
            Pair.of(16L, 325L),
            Pair.of(11L, 185L),
            Pair.of(15L, 332L),
            Pair.of(18L, 406L),
            Pair.of(13L, null),
            Pair.of(22L, 522L),
            Pair.of(19L, 412L),
            Pair.of(25L, 614L),
            Pair.of(23L, 544L),
            Pair.of(18L, 421L),
            Pair.of(22L, 445L),
            Pair.of(null, 620L),
            Pair.of(17L, 408L)
    );
    for (final Pair<Long, Long> thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double correlation = udaf.map(agg);

    assertThat(0.9645, closeTo(correlation, 0.00005));
  }

}
