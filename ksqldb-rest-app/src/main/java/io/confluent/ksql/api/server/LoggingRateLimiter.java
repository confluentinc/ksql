/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.server;

import static io.confluent.ksql.rest.server.KsqlRestConfig.KSQL_LOGGING_SERVER_RATE_LIMITED_REQUEST_PATHS_CONFIG;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class LoggingRateLimiter {

  private final Map<String, Double> rateLimitedPaths;
  private final Function<Double, RateLimiter> rateLimiterFactory;

  private final Map<String, RateLimiter> rateLimiters = new ConcurrentHashMap<>();

  LoggingRateLimiter(final KsqlRestConfig ksqlRestConfig) {
    this(ksqlRestConfig, RateLimiter::create);
  }

  @VisibleForTesting
  LoggingRateLimiter(
      final KsqlRestConfig ksqlRestConfig,
      final Function<Double, RateLimiter> rateLimiterFactory) {
    requireNonNull(ksqlRestConfig);
    this.rateLimiterFactory = requireNonNull(rateLimiterFactory);
    this.rateLimitedPaths = getRateLimitedRequestPaths(ksqlRestConfig);
  }

  public boolean shouldLog(final String path) {
    if (rateLimitedPaths.containsKey(path)) {
      final double rateLimit = rateLimitedPaths.get(path);
      rateLimiters.computeIfAbsent(path, (k) -> rateLimiterFactory.apply(rateLimit));
      return rateLimiters.get(path).tryAcquire();
    }
    return true;
  }

  private static Map<String, Double> getRateLimitedRequestPaths(final KsqlRestConfig config) {
    // Already validated as having double values
    return config.getStringAsMap(KSQL_LOGGING_SERVER_RATE_LIMITED_REQUEST_PATHS_CONFIG)
        .entrySet().stream()
        .collect(ImmutableMap.toImmutableMap(Entry::getKey,
            entry -> Double.parseDouble(entry.getValue())));
  }

}
