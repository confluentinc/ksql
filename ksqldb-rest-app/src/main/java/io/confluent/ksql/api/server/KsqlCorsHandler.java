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

import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class KsqlCorsHandler implements Handler<RoutingContext> {

  private static final List<String> DEFAULT_ALLOWED_METHODS = Arrays.asList("GET", "POST", "HEAD");
  private static final List<String> DEFAULT_ALLOWED_HEADERS = Arrays
      .asList("X-Requested-With", "Content-Type", "Accept", "Origin");
  private static final List<String> EXCLUDED_PATH_PREFIXES = Collections.singletonList("/ws/");

  static void setupCorsHandler(final Server server, final Router router) {
    final ApiServerConfig apiServerConfig = server.getConfig();
    final String allowedOrigins = apiServerConfig
        .getString(ApiServerConfig.CORS_ALLOWED_ORIGINS);
    if (allowedOrigins.trim().isEmpty()) {
      return;
    }
    final String convertedPattern = convertAllowedOrigin(allowedOrigins);
    final CorsHandler corsHandler = CorsHandler.create(convertedPattern);
    final Set<String> allowedMethodsSet = new HashSet<>(apiServerConfig
        .getList(ApiServerConfig.CORS_ALLOWED_METHODS));
    if (allowedMethodsSet.isEmpty()) {
      allowedMethodsSet.addAll(DEFAULT_ALLOWED_METHODS);
    }
    corsHandler.allowedMethods(
        allowedMethodsSet.stream().map(sMethod -> HttpMethod.valueOf(sMethod.toUpperCase()))
            .collect(Collectors.toSet()));

    final Set<String> allowedHeadersSet = new HashSet<>(apiServerConfig
        .getList(ApiServerConfig.CORS_ALLOWED_HEADERS));
    if (allowedHeadersSet.isEmpty()) {
      allowedHeadersSet.addAll(DEFAULT_ALLOWED_HEADERS);
    }
    corsHandler.allowedHeaders(allowedHeadersSet);

    corsHandler.allowCredentials(true);

    router.route().handler(new KsqlCorsHandler(corsHandler));
  }

  private final CorsHandler corsHandler;

  public KsqlCorsHandler(final CorsHandler corsHandler) {
    this.corsHandler = corsHandler;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    final String path = routingContext.normalisedPath();
    for (String excludedPrefix : EXCLUDED_PATH_PREFIXES) {
      if (path.startsWith(excludedPrefix)) {
        routingContext.next();
        return;
      }
    }
    corsHandler.handle(routingContext);
  }

  // Convert to regex
  private static String convertAllowedOrigin(final String allowedOrigins) {
    final String[] parts = allowedOrigins.split(",");
    final StringBuilder out = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i].trim();
      part = part.replace(".", "\\.")
          .replace("+", "\\+")
          .replace("?", "\\?")
          .replace("*", ".*");
      out.append(part);
      if (i != parts.length - 1) {
        out.append('|');
      }
    }
    return out.toString();
  }

}
