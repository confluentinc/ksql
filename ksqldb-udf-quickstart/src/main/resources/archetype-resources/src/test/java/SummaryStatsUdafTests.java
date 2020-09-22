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

import io.confluent.ksql.function.udaf.Udaf;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;


/**
 * Example class that demonstrates how to unit test UDAFs.
 */
public class SummaryStatsUdafTests {

  @Test
  public void mergeAggregates() {
    // Given:
    final Udaf<Double, Map<String, Double>, Map<String, Double>> udaf =
        SummaryStatsUdaf.createUdaf();

    // When:
    final Map<String, Double> mergedAggregate = udaf.merge(
        // (sample_size, sum, mean)
        aggregate(3.0, 3300.0, 1100.0),
        aggregate(7.0, 6700.0, 957.143)
    );

    // Then:
    final Map<String, Double> expectedResult = aggregate(10.0, 10000.0, 1000.0);
    Assert.assertEquals(expectedResult, mergedAggregate);
  }

  @Test
  public void shouldComputeNewAggregate() {
    // Given:
    final Udaf<Double, Map<String, Double>, Map<String, Double>> udaf =
        SummaryStatsUdaf.createUdaf();

    // When:
    final Map<String, Double> newAggregate = udaf.aggregate(900.0, aggregate(1.0, 400.0, 400.0));

    // Then:
    Assert.assertEquals(
        aggregate(2.0, 1300.0, 650.0),
        newAggregate
    );
  }

  /**
   * Helper method for building an aggregate that mimics what KSQL would pass
   * to our UDAF instance.
   */
  private static Map<String, Double> aggregate(
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
