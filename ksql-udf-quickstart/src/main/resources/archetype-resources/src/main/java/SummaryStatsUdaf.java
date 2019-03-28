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

package ${package};

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * In this example, we implement a UDAF for computing some summary statistics for a stream
 * of doubles.
 *
 * <p>Example query usage:
 *
 * <pre>{@code
 * CREATE STREAM api_responses (username VARCHAR, response_code INT, response_time DOUBLE) \
 * WITH (kafka_topic='api_logs', value_format='JSON');
 *
 * SELECT username, SUMMARY_STATS(response_time) \
 * FROM api_responses \
 * GROUP BY username ;
 * }</pre>
 */
@UdafDescription(
    name = "summary_stats",
    description = "Example UDAF that computes some summary stats for a stream of doubles",
    version = "${version}",
    author = "${author}"
)
public final class SummaryStatsUdaf {

  private SummaryStatsUdaf() {
  }

  @UdafFactory(description = "compute summary stats for doubles")
  // Can be used with stream aggregations. The input of our aggregation will be doubles,
  // and the output will be a map
  public static Udaf<Double, Map<String, Double>> createStddev() {

    return new Udaf<Double, Map<String, Double>>() {

      /**
       * Specify an initial value for our aggregation
       *
       * @return the initial state of the aggregate.
       */
      @Override
      public Map<String, Double> initialize() {
        final Map<String, Double> stats = new HashMap<>();
        stats.put("mean", 0.0);
        stats.put("sample_size", 0.0);
        stats.put("sum", 0.0);
        stats.put("sum_squares", 0.0);
        stats.put("stddev_population", 0.0);
        stats.put("stddev_sample", 0.0);
        return stats;
      }

      /**
       * Perform the aggregation whenever a new record appears in our stream.
       *
       * @param newValue the new value to add to the {@code aggregateValue}.
       * @param aggregateValue the current aggregate.
       * @return the new aggregate value.
       */
      @Override
      public Map<String, Double> aggregate(
          final Double newValue,
          final Map<String, Double> aggregateValue
      ) {
        final Double sampleSize = 1.0 + aggregateValue
            .getOrDefault("sample_size", 0.0);

        final Double sum = newValue + aggregateValue
            .getOrDefault("sum", 0.0);

        final Double sumSquares = (newValue * newValue) + aggregateValue
            .getOrDefault("sum_squares", 0.0);

        // Calculate a rolling population standard deviation.
        final Double mean = sum / sampleSize;
        final Double variancePopulation = (sumSquares / sampleSize) - (mean * mean);

        // Build the map of summary statistics
        aggregateValue.put("mean", mean);
        aggregateValue.put("sample_size", sampleSize);
        aggregateValue.put("sum", sum);
        aggregateValue.put("sum_squares", sumSquares);
        aggregateValue.put("stddev_population", Math.sqrt(variancePopulation));

        if (sampleSize > 1) {
          // Get the unbiased variance to calculate a rolling sample standard deviation
          final Double inverse = sampleSize / (sampleSize - 1);
          final Double varianceSample = inverse * variancePopulation;
          aggregateValue.put("stddev_sample", Math.sqrt(varianceSample));
        } else {
          aggregateValue.put("stddev_sample", 0.0);
        }

        return aggregateValue;
      }

      /**
       * Called to merge two aggregates together.
       *
       * @param aggOne the first aggregate
       * @param aggTwo the second aggregate
       * @return the merged result
       */
      @Override
      public Map<String, Double> merge(
          final Map<String, Double> aggOne,
          final Map<String, Double> aggTwo
      ) {
        // Merging the two aggregates is left as an exercise for the reader ;)
        return aggTwo;
      }
    };
  }
}
