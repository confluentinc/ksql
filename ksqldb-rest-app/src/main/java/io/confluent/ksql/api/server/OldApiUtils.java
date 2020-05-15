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

import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.apache.http.HttpHeaders.TRANSFER_ENCODING;

import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.RoutingContext;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

public final class OldApiUtils {

  private OldApiUtils() {
  }

  private static final String CONTENT_TYPE_HEADER = HttpHeaders.CONTENT_TYPE.toString();
  private static final String JSON_CONTENT_TYPE = "application/json";
  private static final String CHUNKED_ENCODING = "chunked";

  static <T> void handleOldApiRequest(final Server server,
      final RoutingContext routingContext,
      final Class<T> requestClass,
      final BiFunction<T, ApiSecurityContext, CompletableFuture<EndpointResponse>> requestor) {
    final HttpServerResponse response = routingContext.response();

    final T requestObject;
    if (requestClass != null) {
      final Optional<T> optRequestObject = ServerUtils
          .deserialiseObject(routingContext.getBody(), routingContext, requestClass);
      if (!optRequestObject.isPresent()) {
        return;
      }
      requestObject = optRequestObject.get();
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

  static void handleOldApiResponse(final Server server, final RoutingContext routingContext,
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
          responseBody = ServerUtils.serializeObject(endpointResponse.getEntity());
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

}
