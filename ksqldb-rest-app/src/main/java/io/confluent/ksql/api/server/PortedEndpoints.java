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
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.TEMPORARY_REDIRECT;
import static org.apache.http.HttpHeaders.TRANSFER_ENCODING;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;

class PortedEndpoints {

  private static final Set<String> PORTED_ENDPOINTS = ImmutableSet
      .of("/ksql", "/ksql/terminate", "/query", "/info", "/heartbeat", "/clusterStatus",
          "/status/:type/:entity/:action", "/status", "/lag", "/healthcheck", "/v1/metadata",
          "/v1/metadata/id", "/ws/query");

  private static final String CONTENT_TYPE_HEADER = HttpHeaders.CONTENT_TYPE.toString();
  private static final String JSON_CONTENT_TYPE = "application/json";

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private static final String CHUNKED_ENCODING = "chunked";

  private final Endpoints endpoints;
  private final Server server;

  PortedEndpoints(final Endpoints endpoints, final Server server) {
    this.endpoints = endpoints;
    this.server = server;
  }

  static void setupEndpoints(final Endpoints endpoints, final Server server,
      final Router router) {
    router.route(HttpMethod.GET, "/")
        .handler(new PortedEndpoints(endpoints, server)::handleInfoRedirect);
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
    router.route(HttpMethod.POST, "/query")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleQueryRequest);
    router.route(HttpMethod.GET, "/info")
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleInfoRequest);
    router.route(HttpMethod.POST, "/heartbeat")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleHeartbeatRequest);
    router.route(HttpMethod.GET, "/clusterStatus")
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleClusterStatusRequest);
    router.route(HttpMethod.GET, "/status/:type/:entity/:action")
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleStatusRequest);
    router.route(HttpMethod.GET, "/status")
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleAllStatusesRequest);
    router.route(HttpMethod.POST, "/lag")
        .handler(BodyHandler.create())
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleLagReportRequest);
    router.route(HttpMethod.GET, "/healthcheck")
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleHealthcheckRequest);
    router.route(HttpMethod.GET, "/v1/metadata")
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleServerMetadataRequest);
    router.route(HttpMethod.GET, "/v1/metadata/id")
        .produces(Versions.KSQL_V1_JSON)
        .produces(MediaType.APPLICATION_JSON)
        .handler(new PortedEndpoints(endpoints, server)::handleServerMetadataClusterIdRequest);
    router.route(HttpMethod.GET, "/ws/query")
        .handler(new PortedEndpoints(endpoints, server)::handleWebsocket);
  }

  static void setupFailureHandler(final Router router) {
    for (String path : PORTED_ENDPOINTS) {
      router.route(path).failureHandler(PortedEndpoints::oldApiFailureHandler);
    }
  }

  void handleKsqlRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, KsqlRequest.class,
        (ksqlRequest, apiSecurityContext) ->
            endpoints
                .executeKsqlRequest(ksqlRequest, server.getWorkerExecutor(),
                    DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleTerminateRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, ClusterTerminateRequest.class,
        (request, apiSecurityContext) ->
            endpoints
                .executeTerminate(request, server.getWorkerExecutor(),
                    DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleQueryRequest(final RoutingContext routingContext) {

    final CompletableFuture<Void> connectionClosedFuture = new CompletableFuture<>();
    routingContext.request().connection().closeHandler(v -> connectionClosedFuture.complete(null));

    handlePortedOldApiRequest(server, routingContext, KsqlRequest.class,
        (request, apiSecurityContext) ->
            endpoints
                .executeQueryRequest(request, server.getWorkerExecutor(), connectionClosedFuture,
                    DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleInfoRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints.executeInfo(DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleClusterStatusRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints.executeClusterStatus(DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleHeartbeatRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, HeartbeatMessage.class,
        (request, apiSecurityContext) ->
            endpoints.executeHeartbeat(request, DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleStatusRequest(final RoutingContext routingContext) {
    final HttpServerRequest request = routingContext.request();
    final String type = request.getParam("type");
    final String entity = request.getParam("entity");
    final String action = request.getParam("action");
    handlePortedOldApiRequest(server, routingContext, null,
        (r, apiSecurityContext) ->
            endpoints.executeStatus(type, entity, action,
                DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleAllStatusesRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, null,
        (r, apiSecurityContext) ->
            endpoints.executeAllStatuses(DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleLagReportRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, LagReportingMessage.class,
        (request, apiSecurityContext) ->
            endpoints.executeLagReport(request, DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleHealthcheckRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints.executeCheckHealth(DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleServerMetadataRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints.executeServerMetadata(DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleServerMetadataClusterIdRequest(final RoutingContext routingContext) {
    handlePortedOldApiRequest(server, routingContext, null,
        (request, apiSecurityContext) ->
            endpoints
                .executeServerMetadataClusterId(DefaultApiSecurityContext.create(routingContext))
    );
  }

  void handleInfoRedirect(final RoutingContext routingContext) {
    // We redirect to the /info endpoint.
    // (This preserves behaviour of the old API)
    routingContext.response().putHeader("location", "/info")
        .setStatusCode(TEMPORARY_REDIRECT.code()).end();
  }

  void handleWebsocket(final RoutingContext routingContext) {
    final ApiSecurityContext apiSecurityContext = DefaultApiSecurityContext.create(routingContext);
    final ServerWebSocket serverWebSocket = routingContext.request().upgrade();
    endpoints
        .executeWebsocketStream(serverWebSocket, routingContext.request().params(),
            server.getWorkerExecutor(), apiSecurityContext);
  }

  private static <T> void handlePortedOldApiRequest(final Server server,
      final RoutingContext routingContext,
      final Class<T> requestClass,
      final BiFunction<T, ApiSecurityContext, CompletableFuture<EndpointResponse>> requestor) {
    final HttpServerResponse response = routingContext.response();
    final T requestObject;
    if (requestClass != null) {
      try {
        requestObject = OBJECT_MAPPER.readValue(routingContext.getBody().getBytes(), requestClass);
      } catch (Exception e) {
        routingContext.fail(BAD_REQUEST.code(),
            new KsqlApiException("Malformed JSON", ErrorCodes.ERROR_CODE_MALFORMED_REQUEST));
        return;
      }
    } else {
      requestObject = null;
    }
    final CompletableFuture<EndpointResponse> completableFuture = requestor
        .apply(requestObject, DefaultApiSecurityContext.create(routingContext));
    completableFuture.thenAccept(endpointResponse -> {
      handleOldApiResponse(server, routingContext, response, endpointResponse);
    }).exceptionally(t -> {
      if (t instanceof CompletionException) {
        t = t.getCause();
      }
      final EndpointResponse errResponse = OldApiExceptionMapper.mapException(t);
      handleOldApiResponse(server, routingContext, response, errResponse);
      return null;
    });
  }

  private static void handleOldApiResponse(final Server server, final RoutingContext routingContext,
      final HttpServerResponse response,
      final EndpointResponse endpointResponse) {
    response.putHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE);

    response.setStatusCode(endpointResponse.getStatus());

    // What the old API returns in it's response is something of a mishmash - sometimes it's
    // a plain String, other times it's an object that needs to be JSON encoded, other times
    // it represents a stream.
    if (endpointResponse.getEntity() instanceof StreamingOutput) {
      if (routingContext.request().version() == HttpVersion.HTTP_2) {
        // The old /query endpoint uses chunked encoding which is not supported in HTTP2
        routingContext.response().setStatusCode(METHOD_NOT_ALLOWED.code())
            .setStatusMessage("The /query endpoint is not available using HTTP2").end();
        return;
      }
      response.putHeader(TRANSFER_ENCODING, CHUNKED_ENCODING);
      streamEndpointResponse(server, response, (StreamingOutput) endpointResponse.getEntity());
    } else {
      if (endpointResponse.getEntity() == null) {
        response.end();
      } else {
        final Buffer responseBody;
        if (endpointResponse.getEntity() instanceof String) {
          responseBody = Buffer.buffer((String) endpointResponse.getEntity());
        } else {
          try {
            final byte[] bytes = OBJECT_MAPPER
                .writeValueAsBytes(endpointResponse.getEntity());
            responseBody = Buffer.buffer(bytes);
          } catch (JsonProcessingException e) {
            // This is an internal error as it's a bug in the server
            routingContext.fail(INTERNAL_SERVER_ERROR.code(), e);
            return;
          }
        }
        response.end(responseBody);
      }
    }
  }

  private static void streamEndpointResponse(final Server server, final HttpServerResponse response,
      final StreamingOutput streamingOutput) {
    final WorkerExecutor workerExecutor = server.getWorkerExecutor();
    final VertxCompletableFuture<Void> vcf = new VertxCompletableFuture<>();
    workerExecutor.executeBlocking(promise -> {
      try (OutputStream os = new BufferedOutputStream(new ResponseOutputStream(response))) {
        streamingOutput.write(os);
      } catch (Exception e) {
        promise.fail(e);
      }
    }, vcf);
  }

  private static void oldApiFailureHandler(final RoutingContext routingContext) {
    final int statusCode = routingContext.statusCode();

    final KsqlErrorMessage ksqlErrorMessage;
    if (routingContext.failure() instanceof KsqlApiException) {
      final KsqlApiException ksqlApiException = (KsqlApiException) routingContext.failure();
      ksqlErrorMessage = new KsqlErrorMessage(
          ksqlApiException.getErrorCode(),
          ksqlApiException.getMessage());
    } else {
      ksqlErrorMessage = new KsqlErrorMessage(
          toErrorCode(statusCode),
          routingContext.failure().getMessage());
    }
    try {
      final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(ksqlErrorMessage);
      routingContext.response().setStatusCode(statusCode)
          .end(Buffer.buffer(bytes));
    } catch (JsonProcessingException e) {
      routingContext.fail(INTERNAL_SERVER_ERROR.code(), e);
    }
  }

}
