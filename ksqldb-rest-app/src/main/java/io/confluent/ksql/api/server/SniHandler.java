/*
 * Copyright 2022 Confluent Inc.
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

import static io.netty.handler.codec.http.HttpResponseStatus.MISDIRECTED_REQUEST;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import io.confluent.ksql.rest.Errors;

public class SniHandler implements Handler<RoutingContext> {

  public SniHandler() {
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    if (routingContext.request().isSSL()) {
      final String indicatedServerName = routingContext.request().connection()
          .indicatedServerName();
      if (!indicatedServerName.equals("")) {
        if (!routingContext.request().getHeader(HttpHeaders.HOST).equals(indicatedServerName)) {
          routingContext.fail(MISDIRECTED_REQUEST.code(),
              new KsqlApiException("This request was incorrectly sent to this ksqlDB server",
                 Errors.ERROR_CODE_MISDIRECTED_REQUEST));
          return;
        }
      }
    }
    routingContext.next();
  }
}
