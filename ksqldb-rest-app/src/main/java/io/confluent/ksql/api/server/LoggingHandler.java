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

import static io.confluent.ksql.rest.server.KsqlRestConfig.KSQL_LOGGING_SERVER_SKIPPED_RESPONSE_CODES_CONFIG;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingHandler implements Handler<RoutingContext> {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingHandler.class);
  static final String HTTP_HEADER_USER_AGENT = "User-Agent";

  private final Set<Integer> skipResponseCodes;
  private final Logger logger;
  private final Clock clock;
  private final LoggingRateLimiter loggingRateLimiter;

  private final Map<String, RateLimiter> rateLimiters = new ConcurrentHashMap<>();

  public LoggingHandler(final Server server, final LoggingRateLimiter loggingRateLimiter) {
    this(server, loggingRateLimiter, LOG, Clock.systemUTC());
  }

  @VisibleForTesting
  LoggingHandler(
      final Server server,
      final LoggingRateLimiter loggingRateLimiter,
      final Logger logger,
      final Clock clock) {
    requireNonNull(server);
    this.loggingRateLimiter = requireNonNull(loggingRateLimiter);
    this.skipResponseCodes = getSkipResponseCodes(server.getConfig());
    this.logger = logger;
    this.clock = clock;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    routingContext.addEndHandler(ar -> {
      // After the response is complete, log results here.
      if (skipResponseCodes.contains(routingContext.response().getStatusCode())) {
        return;
      }
      if (!loggingRateLimiter.shouldLog(routingContext.request().path())) {
        return;
      }
      final long contentLength = routingContext.request().response().bytesWritten();
      final HttpVersion version = routingContext.request().version();
      final HttpMethod method = routingContext.request().method();
      final String uri = routingContext.request().uri();
      final int status = routingContext.request().response().getStatusCode();
      final long requestBodyLength = routingContext.request().bytesRead();
      final String versionFormatted;
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
        default:
          versionFormatted = "-";
      }
      final String name = Optional.ofNullable((ApiUser) routingContext.user())
          .map(u -> u.getPrincipal().getName())
          .orElse("-");
      final String userAgent = Optional.ofNullable(
          routingContext.request().getHeader(HTTP_HEADER_USER_AGENT)).orElse("-");
      final String timestamp = Utils.formatRFC1123DateTime(clock.millis());
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

  private static Set<Integer> getSkipResponseCodes(final KsqlRestConfig config) {
    // Already validated as all ints
    return config.getList(KSQL_LOGGING_SERVER_SKIPPED_RESPONSE_CODES_CONFIG)
        .stream()
        .map(Integer::parseInt).collect(ImmutableSet.toImmutableSet());
  }

  private void doLog(final int status, final String message) {
    if (status >= 500) {
      logger.error(message);
    } else if (status >= 400) {
      logger.warn(message);
    } else {
      logger.info(message);
    }
  }
}
