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
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.Response;

/**
 * State machine for managing the lifecycle of a KSQL Server. The machine goes through the
 * following states:
 *     INITIALIZING: The server is still setting up to serve requests (checking preconditons,
 *                   initialing the engine, etc).
 *     READY:        The server is ready to serve the KSQL API.
 *     TERMINATING:  The server has been asked to shut down and is cleaning up state.
 */
public class ServerState {
  public enum State {
    INITIALIZING,
    READY,
    TERMINATING
  }

  private static final class StateWithReason {
    final State state;
    final String reason;

    private StateWithReason(final State state, final String reason) {
      this.state = state;
      this.reason = reason;
    }

    private StateWithReason(final State state) {
      this.state = state;
      this.reason = null;
    }
  }

  private final AtomicReference<StateWithReason> state = new AtomicReference<>(
      new StateWithReason(State.INITIALIZING, "KSQL is not yet ready to serve requests.")
  );

  /**
   * Creates a new ServerState with state INITIALIZING.
   */
  public ServerState() {
  }

  /**
   * Set a reason string explaining why the server is still in the INITIALIZING state.
   * This reason string will be used to add context to the API error indicating that the server
   * is not yet ready to serve requests.
   *
   * @param initializingReason The reason string indicating why the server is still initializing.
   */
  public void setInitializingReason(final String initializingReason) {
    this.state.set(new StateWithReason(State.INITIALIZING, initializingReason));
  }

  /**
   * Sets the server state to READY.
   */
  public void setReady() {
    this.state.set(new StateWithReason(State.READY));
  }

  /**
   * Sets the server state to TERMINATING.
   */
  public void setTerminating() {
    this.state.set(new StateWithReason(State.TERMINATING));
  }

  /**
   * Checks whether the server is READY (and can therefore server requests). If it is not READY
   * then returns an API error to be sent back to the user.
   *
   * @return If empty, indicates that the server is ready. Otherwise, contains an error response
   *         to be returned back to the user.
   */
  public Optional<Response> checkReady() {
    final StateWithReason state = this.state.get();
    switch (state.state) {
      case INITIALIZING:
        return Optional.of(Errors.serverNotReady(state.reason));
      case TERMINATING:
        return Optional.of(Errors.serverShuttingDown());
      default:
    }
    return Optional.empty();
  }
}
