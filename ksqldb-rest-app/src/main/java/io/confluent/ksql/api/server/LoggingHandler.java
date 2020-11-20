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

import static io.confluent.ksql.rest.server.KsqlRestConfig.KSQL_LOGGING_RATE_LIMITED_REQUEST_PATHS_CONFIG;
import static io.confluent.ksql.rest.server.KsqlRestConfig.KSQL_LOGGING_SKIPPED_RESPONSE_CODES_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.ksql.api.auth.ApiUser;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.impl.Utils;
import java.time.Clock;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingHandler implements Handler<RoutingContext> {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingHandler.class);
  static final String HTTP_HEADER_USER_AGENT = "User-Agent";

  private final Set<Integer> skipResponseCodes;
  private final Map<String, Double> rateLimitedPaths;
  private final Logger logger;
  private final Clock clock;
  private final Function<Double, RateLimiter> rateLimiterFactory;

  private final Map<String, RateLimiter> rateLimiters = new ConcurrentHashMap<>();

  public LoggingHandler(final Server server) {
    this(server, LOG, Clock.systemUTC(), RateLimiter::create);
  }

  @VisibleForTesting
  LoggingHandler(
      final Server server,
      final Logger logger,
      final Clock clock,
      final Function<Double, RateLimiter> rateLimiterFactory) {
    this.skipResponseCodes = getSkipResponseCodes(server.getConfig());
    this.rateLimitedPaths = getSkipRequestPaths(server.getConfig());
    this.logger = logger;
    this.clock = clock;
    this.rateLimiterFactory = rateLimiterFactory;
  }

  private static Set<Integer> getSkipResponseCodes(final KsqlRestConfig config) {
    // Already validated as all ints
    return config.getList(KSQL_LOGGING_SKIPPED_RESPONSE_CODES_CONFIG)
        .stream()
        .map(Integer::parseInt).collect(ImmutableSet.toImmutableSet());
  }

  private static Map<String, Double> getSkipRequestPaths(final KsqlRestConfig config) {
    // Already validated as having a double value
    return config.getStringAsMap(KSQL_LOGGING_RATE_LIMITED_REQUEST_PATHS_CONFIG)
        .entrySet().stream()
        .collect(ImmutableMap.toImmutableMap(Entry::getKey,
            entry -> Double.parseDouble(entry.getValue())));
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    routingContext.addEndHandler(ar -> {
      // After the response is complete, log results here.
      if (skipResponseCodes.contains(routingContext.response().getStatusCode())) {
        return;
      }
      if (rateLimitedPaths.containsKey(routingContext.request().path())) {
        String path = routingContext.request().path();
        double rateLimit = rateLimitedPaths.get(path);
        rateLimiters.computeIfAbsent(path, (k) -> rateLimiterFactory.apply(rateLimit));
        if (!rateLimiters.get(path).tryAcquire()) {
          return;
        }
      }
      long contentLength = routingContext.request().response().bytesWritten();
      HttpVersion version = routingContext.request().version();
      HttpMethod method = routingContext.request().method();
      String uri = routingContext.request().uri();
      int status = routingContext.request().response().getStatusCode();
      String versionFormatted = "-";
      long requestBodyLength = routingContext.request().bytesRead();
      switch (version) {
        case HTTP_1_0:
          versionFormatted = "HTTP/1.0";
          break;
        case HTTP_1_1:
          versionFormatted = "HTTP/1.1";
          break;
        case HTTP_2:
          versionFormatted = "HTTP/2.0";
          break;
      }
      String name = Optional.ofNullable((ApiUser) routingContext.user())
          .map(u -> u.getPrincipal().getName())
          .orElse("-");
      String userAgent = Optional.ofNullable(
          routingContext.request().getHeader(HTTP_HEADER_USER_AGENT)).orElse("-");
      String timestamp = Utils.formatRFC1123DateTime(clock.millis());
      final String message = String.format(
          "%s - %s [%s] \"%s %s %s\" %d %d \"-\" \"%s\" %d",
          routingContext.request().remoteAddress().host(),
          name,
          timestamp,
          method,
          uri,
          versionFormatted,
          status,
          contentLength,
          userAgent,
          requestBodyLength);
      doLog(status, message);
    });
    routingContext.next();
  }

  private void doLog(int status, String message) {
    if (status >= 500) {
      logger.error(message);
    } else if (status >= 400) {
      logger.warn(message);
    } else {
      logger.info(message);
    }
  }
}
