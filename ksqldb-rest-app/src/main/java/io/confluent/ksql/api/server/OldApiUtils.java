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

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.apache.hc.core5.http.HttpHeaders.TRANSFER_ENCODING;

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.RoutingContext;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import org.apache.kafka.common.utils.Time;

public final class OldApiUtils {

  private OldApiUtils() {
  }

  private static final String CONTENT_TYPE_HEADER = HttpHeaders.CONTENT_TYPE.toString();
  private static final String JSON_CONTENT_TYPE = "application/json";
  private static final String CHUNKED_ENCODING = "chunked";

  static <T> void handleOldApiRequest(
      final Server server,
      final RoutingContext routingContext,
      final Class<T> requestClass,
      final Optional<MetricsCallbackHolder> metricsCallbackHolder,
      final BiFunction<T, ApiSecurityContext, CompletableFuture<EndpointResponse>> requestor) {
    final long startTimeNanos = Time.SYSTEM.nanoseconds();
    final T requestObject;
    if (requestClass != null) {
      final Optional<T> optRequestObject = ServerUtils
          .deserialiseObject(routingContext.getBody(), routingContext, requestClass);
      if (!optRequestObject.isPresent()) {
        return;
      }
      requestObject = optRequestObject.get();
      if (requestObject instanceof KsqlRequest) {
        final KsqlRequest request = (KsqlRequest) requestObject;
        ApiServerUtils.setMaskedSql(request);
      }
    } else {
      requestObject = null;
    }
    final CompletableFuture<EndpointResponse> completableFuture = requestor
        .apply(requestObject, DefaultApiSecurityContext.create(routingContext, server));
    completableFuture.thenAccept(endpointResponse -> {
      handleOldApiResponse(
          server, routingContext, endpointResponse, metricsCallbackHolder, startTimeNanos);
    }).exceptionally(t -> {
      if (t instanceof CompletionException) {
        t = t.getCause();
      }
      handleOldApiResponse(
          server, routingContext, mapException(t), metricsCallbackHolder, startTimeNanos);
      return null;
    });
  }

  static void handleOldApiResponse(
      final Server server, final RoutingContext routingContext,
      final EndpointResponse endpointResponse,
      final Optional<MetricsCallbackHolder> metricsCallbackHolder,
      final long startTimeNanos
  ) {
    // If there's a {@link NextHandlerOutput}, then that an indication that the handler doesn't want
    // to handle the response and is passing it off to the next handler in the line. This is
    // primarily used for allowing one handler to have the first shot at trying to handle it and
    // falling back to another, such as for a migration.
    if (endpointResponse.getEntity() instanceof NextHandlerOutput) {
      routingContext.next();
      return;
    }

    final HttpServerResponse response = routingContext.response();
    response.putHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE);

    response.setStatusCode(endpointResponse.getStatus());

    // What the old API returns in it's response is something of a mishmash - sometimes it's
    // a plain String, other times it's an object that needs to be JSON encoded, other times
    // it represents a stream.
    if (endpointResponse.getEntity() instanceof StreamingOutput) {
      final StreamingOutput streamingOutput = (StreamingOutput) endpointResponse.getEntity();
      if (routingContext.request().version() == HttpVersion.HTTP_2) {
        // The old /query endpoint uses chunked encoding which is not supported in HTTP2
        routingContext.response().setStatusCode(METHOD_NOT_ALLOWED.code())
            .setStatusMessage("The /query endpoint is not available using HTTP2").end();
        streamingOutput.close();
        return;
      }
      response.putHeader(TRANSFER_ENCODING, CHUNKED_ENCODING);
      streamEndpointResponse(server, routingContext, streamingOutput, metricsCallbackHolder,
          startTimeNanos);
    } else {
      if (endpointResponse.getEntity() == null) {
        response.end();
      } else {
        final Buffer responseBody;
        if (endpointResponse.getEntity() instanceof String) {
          responseBody = Buffer.buffer((String) endpointResponse.getEntity());
        } else {
          responseBody = ServerUtils.serializeObject(endpointResponse.getEntity());
        }
        response.end(responseBody);
      }
      reportMetrics(routingContext, metricsCallbackHolder, startTimeNanos);
    }
  }

  private static void streamEndpointResponse(final Server server,
      final RoutingContext routingContext,
      final StreamingOutput streamingOutput,
      final Optional<MetricsCallbackHolder> metricsCallbackHolder,
      final long startTimeNanos) {
    final WorkerExecutor workerExecutor = server.getWorkerExecutor();
    final VertxCompletableFuture<Void> vcf = new VertxCompletableFuture<>();
    workerExecutor.executeBlocking(
        promise -> {
          final OutputStream ros = new ResponseOutputStream(routingContext.response(),
                  streamingOutput.getWriteTimeoutMs());
          routingContext.request().connection().closeHandler(v -> {
            // Close the OutputStream on close of the HTTP connection
            try {
              streamingOutput.close();
            } catch (final Throwable t) {
              promise.fail(t);
            }
            try {
              ros.close();
            } catch (IOException e) {
              promise.fail(e);
            }
          });
          try {
            streamingOutput.write(new BufferedOutputStream(ros));
            promise.complete();
          } catch (Exception e) {
            promise.fail(e);
          } finally {
            try {
              ros.close();
            } catch (IOException ignore) {
              // Ignore - it might already be closed
            }
          }
        },
        false /*if this is true, worker execution blocks the main event loop*/,
        vcf
    );
    vcf.handle((v, throwable) -> {
      reportMetrics(routingContext, metricsCallbackHolder, startTimeNanos);
      return null;
    });
  }

  private static void reportMetrics(
      final RoutingContext routingContext,
      final Optional<MetricsCallbackHolder> metricsCallbackHolder,
      final long startTimeNanos
  ) {
    metricsCallbackHolder.ifPresent(mc -> mc.reportMetrics(
        routingContext.response().getStatusCode(),
        routingContext.request().bytesRead(),
        routingContext.response().bytesWritten(),
        startTimeNanos));
  }

  public static EndpointResponse mapException(final Throwable exception) {
    if (exception instanceof KsqlRestException) {
      final KsqlRestException restException = (KsqlRestException) exception;
      return restException.getResponse();
    }
    return EndpointResponse.create()
        .status(INTERNAL_SERVER_ERROR.code())
        .type("application/json")
        .entity(new KsqlErrorMessage(Errors.ERROR_CODE_SERVER_ERROR, exception))
        .build();
  }

}
