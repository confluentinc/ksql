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

import io.confluent.ksql.rest.Errors;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SniHandler implements Handler<RoutingContext> {
  private static final Logger log = LoggerFactory.getLogger(SniHandler.class);

  public SniHandler() {
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    if (routingContext.request().isSSL()) {
      final String indicatedServerName = routingContext.request().connection()
          .indicatedServerName();
      final String requestHost = routingContext.request().host();
      if (indicatedServerName != null && requestHost != null) {
        // sometimes the port is present in the host header, remove it
        final String requestHostNoPort = requestHost.replaceFirst(":\\d+", "");
        if (!requestHostNoPort.equals(indicatedServerName)) {
          log.error(String.format(
              "Sni check failed, host header: %s, sni value %s",
              requestHostNoPort,
              indicatedServerName)
          );
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
