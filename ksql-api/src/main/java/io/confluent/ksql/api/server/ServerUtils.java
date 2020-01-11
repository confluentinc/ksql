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

import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * static util methods used in the server
 */
public class ServerUtils {

  private static final Logger log = LoggerFactory.getLogger(ServerUtils.class);

  public static void handleError(final HttpServerResponse response, final int statusCode,
      final int errorCode, final String errMsg) {
    JsonObject errResponse = new JsonObject().put("status", "error").put("errorCode", errorCode)
        .put("message", errMsg);
    response.setStatusCode(statusCode).end(errResponse.toBuffer());
  }

  public static void unhandledExceptonHandler(final Throwable t) {
    log.error("Unhandled exception", t);
  }

}
