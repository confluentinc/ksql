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

import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_MISSING_PARAM;

import io.confluent.ksql.api.server.protocol.PojoCodec;
import io.confluent.ksql.api.server.protocol.PojoDeserializerErrorHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * static util methods used in the server
 */
public final class ServerUtils {

  private static final Logger log = LoggerFactory.getLogger(ServerUtils.class);

  private ServerUtils() {
  }

  public static <T> Optional<T> deserialiseObject(final Buffer buffer,
      final RoutingContext routingContext,
      final Class<T> clazz) {
    return PojoCodec.deserialiseObject(buffer, new HttpResponseErrorHandler(routingContext), clazz);
  }

  private static class HttpResponseErrorHandler implements PojoDeserializerErrorHandler {

    private final RoutingContext routingContext;

    HttpResponseErrorHandler(final RoutingContext routingContext) {
      this.routingContext = routingContext;
    }

    @Override
    public void onMissingParam(final String paramName) {
      routingContext.fail(400, new KsqlApiException("No " + paramName + " in arguments",
          ERROR_CODE_MISSING_PARAM));
    }

    @Override
    public void onExtraParam(final String paramName) {
      routingContext.fail(400, new KsqlApiException("Unknown arg " + paramName,
          ErrorCodes.ERROR_CODE_UNKNOWN_PARAM));
    }

    @Override
    public void onInvalidJson() {
      routingContext.fail(400, new KsqlApiException("Malformed JSON in request",
          ErrorCodes.ERROR_CODE_MALFORMED_REQUEST));
    }
  }

}
