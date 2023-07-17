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
import io.confluent.ksql.util.Pair;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;

public class LoggingRateLimiter {
  // Print "You hit a rate limit" every 5 seconds
  private static final double LIMIT_HIT_LOG_RATE = 0.2;

  private final Map<String, RateLimiter> rateLimitersByPath;
  private final Map<Integer, RateLimiter> rateLimitersByResponseCode;

  // Rate limiters for printing the "You hit a rate limit" message
  private final RateLimiter pathLimitHit;
  private final RateLimiter responseCodeLimitHit;

  LoggingRateLimiter(final KsqlRestConfig ksqlRestConfig) {
    this(ksqlRestConfig, RateLimiter::create);
  }

  @VisibleForTesting
  LoggingRateLimiter(
      final KsqlRestConfig ksqlRestConfig,
      final Function<Double, RateLimiter> rateLimiterFactory
  ) {
    requireNonNull(ksqlRestConfig);
    requireNonNull(rateLimiterFactory);
    this.pathLimitHit = rateLimiterFactory.apply(LIMIT_HIT_LOG_RATE);
    this.responseCodeLimitHit = rateLimiterFactory.apply(LIMIT_HIT_LOG_RATE);
    this.rateLimitersByPath = getRateLimitedRequestPaths(ksqlRestConfig, rateLimiterFactory);
    this.rateLimitersByResponseCode
        = getRateLimitedResponseCodes(ksqlRestConfig, rateLimiterFactory);
  }

  public boolean shouldLog(final Logger logger, final String path, final int responseCode) {
    if (rateLimitersByPath.containsKey(path)) {
      final RateLimiter rateLimiter = rateLimitersByPath.get(path);
      if (!rateLimiter.tryAcquire()) {
        if (pathLimitHit.tryAcquire()) {
          logger.info("Hit rate limit for path " + path + " with limit " + rateLimiter.getRate());
        }
        return false;
      }
    }
    if (rateLimitersByResponseCode.containsKey(responseCode)) {
      final RateLimiter rateLimiter = rateLimitersByResponseCode.get(responseCode);
      if (!rateLimiter.tryAcquire()) {
        if (responseCodeLimitHit.tryAcquire()) {
          logger.info("Hit rate limit for response code " + responseCode + " with limit "
              + rateLimiter.getRate());
        }
        return false;
      }
    }
    return true;
  }

  private static Map<String, RateLimiter> getRateLimitedRequestPaths(
      final KsqlRestConfig config,
      final Function<Double, RateLimiter> rateLimiterFactory
  ) {
    // Already validated as having double values
    return config.getStringAsMap(KSQL_LOGGING_SERVER_RATE_LIMITED_REQUEST_PATHS_CONFIG)
        .entrySet().stream()
        .map(entry -> {
          final double rateLimit = Double.parseDouble(entry.getValue());
          return Pair.of(entry.getKey(), rateLimiterFactory.apply(rateLimit));
        })
        .collect(ImmutableMap.toImmutableMap(Pair::getLeft, Pair::getRight));
  }

  private static Map<Integer, RateLimiter> getRateLimitedResponseCodes(
      final KsqlRestConfig config,
      final Function<Double, RateLimiter> rateLimiterFactory
  ) {
    // Already validated as all ints
    return config.getStringAsMap(KSQL_LOGGING_SERVER_RATE_LIMITED_RESPONSE_CODES_CONFIG)
        .entrySet().stream()
        .map(entry -> {
          final int statusCode = Integer.parseInt(entry.getKey());
          final double rateLimit = Double.parseDouble(entry.getValue());
          return Pair.of(statusCode, rateLimiterFactory.apply(rateLimit));
        })
        .collect(ImmutableMap.toImmutableMap(Pair::getLeft, Pair::getRight));
  }
}
