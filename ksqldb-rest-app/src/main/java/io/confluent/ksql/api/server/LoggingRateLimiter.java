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
import static io.confluent.ksql.rest.server.KsqlRestConfig.KSQL_LOGGING_SERVER_RATE_LIMITED_RESPONSE_CODES_CONFIG;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;

class LoggingRateLimiter {
  // Print "You hit a rate limit" every 5 seconds
  private static final double LIMIT_HIT_LOG_RATE = 0.2;

  private final Map<String, Double> rateLimitedPaths;
  private final Map<Integer, Double> rateLimitedResponseCodes;

  private final Function<Double, RateLimiter> rateLimiterFactory;

  private final Map<String, RateLimiter> rateLimitersByPath = new ConcurrentHashMap<>();
  private final Map<Integer, RateLimiter> rateLimitersByResponseCode = new ConcurrentHashMap<>();

  // Rate limiters for printing the "You hit a rate limit" message
  private final RateLimiter pathLimitHit;
  private final RateLimiter responseCodeLimitHit;

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
    this.rateLimitedResponseCodes = getRateLimitedResponseCodes(ksqlRestConfig);
    this.pathLimitHit = rateLimiterFactory.apply(LIMIT_HIT_LOG_RATE);
    this.responseCodeLimitHit = rateLimiterFactory.apply(LIMIT_HIT_LOG_RATE);
  }

  public boolean shouldLog(final Logger logger, final String path, final int responseCode) {
    if (rateLimitedPaths.containsKey(path)) {
      final double rateLimit = rateLimitedPaths.get(path);
      rateLimitersByPath.computeIfAbsent(path, (k) -> rateLimiterFactory.apply(rateLimit));
      if (!rateLimitersByPath.get(path).tryAcquire()) {
        if (pathLimitHit.tryAcquire()) {
          logger.info("Hit rate limit for path " + path + " with limit " + rateLimit);
        }
        return false;
      }
    }
    if (rateLimitedResponseCodes.containsKey(responseCode)) {
      final double rateLimit = rateLimitedResponseCodes.get(responseCode);
      rateLimitersByResponseCode.computeIfAbsent(
          responseCode, (k) -> rateLimiterFactory.apply(rateLimit));
      if (!rateLimitersByResponseCode.get(responseCode).tryAcquire()) {
        if (responseCodeLimitHit.tryAcquire()) {
          logger.info("Hit rate limit for response code " + responseCode + " with limit "
              + rateLimit);
        }
        return false;
      }
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

  private static Map<Integer, Double> getRateLimitedResponseCodes(final KsqlRestConfig config) {
    // Already validated as all ints
    return config.getStringAsMap(KSQL_LOGGING_SERVER_RATE_LIMITED_RESPONSE_CODES_CONFIG)
        .entrySet().stream()
        .collect(ImmutableMap.toImmutableMap(
            entry -> Integer.parseInt(entry.getKey()),
            entry -> Double.parseDouble(entry.getValue())));
  }
}
