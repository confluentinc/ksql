/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.state;

import io.confluent.ksql.rest.server.resources.Errors;
import java.util.Optional;
import javax.ws.rs.core.Response;

public class ServerState {
  public enum State {
    INITIALIZING,
    READY,
    TERMINATING
  }

  private volatile State state = State.INITIALIZING;
  private volatile String initializingReason = "KSQL is not yet ready to serve requests.";

  public ServerState() {
  }

  public void setInitializingReason(final String initializingReason) {
    this.initializingReason = initializingReason;
  }

  public void setReady() {
    this.state = State.READY;
  }

  public void setTerminating() {
    this.state = State.TERMINATING;
  }

  public Optional<Response> checkReady() {
    switch (state) {
      case INITIALIZING:
        return Optional.of(Errors.serverNotReady(initializingReason));
      case TERMINATING:
        return Optional.of(Errors.serverShuttingDown());
      default:
    }
    return Optional.empty();
  }
}
