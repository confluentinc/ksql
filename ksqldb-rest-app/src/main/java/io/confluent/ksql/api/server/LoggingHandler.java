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

import static io.confluent.ksql.rest.server.KsqlRestConfig.KSQL_LOGGING_SKIP_RESPONSE_CODES_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingHandler implements Handler<RoutingContext> {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingHandler.class);
  static final String HTTP_HEADER_USER_AGENT = "User-Agent";

  private final Set<Integer> skipResponseCodes;
  private final Consumer<String> logger;

  public LoggingHandler(final Server server) {
    this(server, LOG::info);
  }

  @VisibleForTesting
  LoggingHandler(final Server server, final Consumer<String> logger) {
    this.skipResponseCodes = getSkipResponseCodes(server.getConfig());
    this.logger = logger;
  }

  private static Set<Integer> getSkipResponseCodes(final KsqlRestConfig config) {
    final Set<Integer> skipCodes = config.getList(KSQL_LOGGING_SKIP_RESPONSE_CODES_CONFIG)
        .stream()
        .map(responseCode -> {
          try {
            return Integer.parseInt(responseCode);
          } catch (NumberFormatException e) {
            throw new IllegalStateException("Configured bad response code " + responseCode);
          }
        }).collect(ImmutableSet.toImmutableSet());
    return skipCodes;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    // If we wanted to log at the beginning of a request, it would go here.
    routingContext.addEndHandler(ar -> {
      // After the response is complete, log results here.
      if (skipResponseCodes.contains(routingContext.response().getStatusCode())) {
        return;
      }
      String errorMessage = "none";
      if (routingContext.response().getStatusCode() > 300) {
        errorMessage = routingContext.response().getStatusMessage();
        if (Strings.isNullOrEmpty(errorMessage)) {
          errorMessage = routingContext.getBodyAsString();
        }
      }
      logger.accept(String.format(
          "Request complete - %s %s status: %d, user agent: %s, request body: %d bytes,"
              + " error response: %s",
          routingContext.request().remoteAddress().host(),
          routingContext.request().uri(),
          routingContext.response().getStatusCode(),
          routingContext.request().getHeader(HTTP_HEADER_USER_AGENT),
          routingContext.request().bytesRead(),
          errorMessage));
    });
    routingContext.next();
  }
}
