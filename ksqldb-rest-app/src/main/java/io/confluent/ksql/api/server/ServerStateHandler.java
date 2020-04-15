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

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.state.ServerState;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import java.util.Objects;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.http.HttpStatus;

public class ServerStateHandler implements Handler<RoutingContext> {

  private final ServerState serverState;

  ServerStateHandler(final ServerState serverState) {
    this.serverState = Objects.requireNonNull(serverState, "serverState");
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    final Optional<Response> response = serverState.checkReady();
    if (response.isPresent()) {
      final KsqlErrorMessage errorMsg = (KsqlErrorMessage) response.get().getEntity();
      routingContext.fail(
          HttpStatus.SERVICE_UNAVAILABLE_503,
          new KsqlApiException(errorMsg.getMessage(), errorMsg.getErrorCode())
      );
    } else {
      routingContext.next();
    }
  }
}
