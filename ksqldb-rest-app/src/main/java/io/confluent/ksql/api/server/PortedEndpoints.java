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

import static io.confluent.ksql.rest.Errors.toErrorCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.api.spi.EndpointResponse;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import javax.ws.rs.core.MediaType;

public class PortedEndpoints {

  private static final Set<String> PORTED_ENDPOINTS = ImmutableSet.of("/ksql");

  private final Endpoints endpoints;
  private final Server server;

  public PortedEndpoints(final Endpoints endpoints, final Server server) {
    this.endpoints = endpoints;
    this.server = server;
  }

  static void setupEndpoints(final Endpoints endpoints, final Server server,
      final Router router) {
    router.route(HttpMethod.POST, "/ksql")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleKsqlRequest);
    router.route(HttpMethod.POST, "/ksql/terminate")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleTerminateRequest);
  }

  static void setupFailureHandler(final Router router) {
    for (String path : PORTED_ENDPOINTS) {
      router.route(path).failureHandler(PortedEndpoints::oldApiFailureHandler);
    }
  }

  void handleKsqlRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(routingContext, KsqlRequest.class,
        (ksqlRequest, apiSecurityContext) ->
            endpoints
                .executeKsqlRequest(ksqlRequest, server.getWorkerExecutor(),
                    DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleTerminateRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(routingContext, ClusterTerminateRequest.class,
        (request, apiSecurityContext) ->
            endpoints
                .executeTerminate(request, server.getWorkerExecutor(),
                    DefaultApiSecurityContext.create(routingContext))
    );
  }

  private static <T> void handlePortedOldApiRequest(final RoutingContext routingContext,
      final Class<T> requestClass,
      final BiFunction<T, ApiSecurityContext, CompletableFuture<EndpointResponse>> requestor) {
    final HttpServerResponse response = routingContext.response();
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    final T requestObject;
    try {
      requestObject = objectMapper.readValue(routingContext.getBody().getBytes(), requestClass);
    } catch (Exception e) {
      routingContext.fail(400,
          new KsqlApiException("Malformed JSON", ErrorCodes.ERROR_CODE_MALFORMED_REQUEST));
      return;
    }
    final CompletableFuture<EndpointResponse> completableFuture = requestor
        .apply(requestObject, DefaultApiSecurityContext.create(routingContext));
    completableFuture.thenAccept(endpointResponse -> {

      final Buffer responseBody;
      try {
        final byte[] bytes = objectMapper.writeValueAsBytes(endpointResponse.getResponseBody());
        responseBody = Buffer.buffer(bytes);
      } catch (JsonProcessingException e) {
        // This is an internal error as it's a bug in the server
        routingContext.fail(500, e);
        return;
      }

      response.setStatusCode(endpointResponse.getStatusCode())
          .setStatusMessage(endpointResponse.getStatusMessage())
          .end(responseBody);

    }).exceptionally(t -> {
      routingContext.fail(500, t);
      return null;
    });
  }

  public static void oldApiFailureHandler(final RoutingContext routingContext) {
    final int statusCode = routingContext.statusCode();
    final KsqlErrorMessage ksqlErrorMessage = new KsqlErrorMessage(
        toErrorCode(statusCode),
        routingContext.failure().getMessage());
    try {
      final byte[] bytes = JsonMapper.INSTANCE.mapper.writeValueAsBytes(ksqlErrorMessage);
      routingContext.response().setStatusCode(statusCode)
          .end(Buffer.buffer(bytes));
    } catch (JsonProcessingException e) {
      routingContext.fail(500, e);
    }
  }

}
