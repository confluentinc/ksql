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

package ${package};

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.confluent.ksql.function.udaf.Udaf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Example class that demonstrates how to unit test UDAFs.
 */
public class SummaryStatsUdafTests {

  @Test
  void mergeAggregates() {
    final Udaf<Double, Map<String, Double>> udaf = SummaryStatsUdaf.createUdaf();
    final Map<String, Double> mergedAggregate = udaf.merge(
      // (sample_size, sum, mean)
      aggregate(3.0, 3300.0, 1100.0),
      aggregate(7.0, 6700.0, 957.143)
    );

    final Map<String, Double> expectedResult = aggregate(10.0, 10000.0, 1000.0);
    assertEquals(expectedResult, mergedAggregate);
  }

  @ParameterizedTest
  @MethodSource("aggSources")
  void calculateSummaryStats(
      final Double newValue,
      final Map<String, Double> currentAggregate,
      final Map<String, Double> expectedResult
    ) {
    final Udaf<Double, Map<String, Double>> udaf = SummaryStatsUdaf.createUdaf();
    assertEquals(expectedResult, udaf.aggregate(newValue, currentAggregate));
  }

  static Stream<Arguments> aggSources() {
    return Stream.of(
      // sample: 400
      arguments(
        // new value
        400.0,
        // current aggregate
        aggregate(0.0, 0.0, 0.0),
        // expected new aggregate
        aggregate(1.0, 400.0, 400.0)
      ),
      // sample: 400, 900
      arguments(
        // new value
        900.0,
        // current aggregate
        aggregate(1.0, 400.0, 400.0),
        // expected new aggregate
        aggregate(2.0, 1300.0, 650.0)
      )
    );
  }

  /**
   * Helper method for building an aggregate that mimics what KSQL would pass
   * to our UDAF instance.
   */
  static Map<String, Double> aggregate(
      final Double sampleSize,
      final Double sum,
      final Double mean
  ) {

    final Map<String, Double> result = new HashMap<>();
    result.put("mean", mean);
    result.put("sample_size", sampleSize);
    result.put("sum", sum);
    return result;
  }
}
