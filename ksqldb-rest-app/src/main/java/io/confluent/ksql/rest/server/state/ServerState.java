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

import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * State machine for managing the lifecycle of a KSQL Server. The machine goes through the
 * following states:
 *     INITIALIZING: The server is still setting up to serve requests (checking preconditons,
 *                   initialing the engine, etc).
 *     READY:        The server is ready to serve the KSQL API.
 *     TERMINATING:  The server has been asked to shut down and is cleaning up state.
 *     TERMINATED:   The server has cleaned up all state.
 */
public class ServerState {
  public enum State {
    INITIALIZING,
    READY,
    TERMINATING,
    TERMINATED
  }

  private static final class StateWithErrorMessage {
    final State state;
    final KsqlErrorMessage errorMessage;

    private StateWithErrorMessage(final State state, final KsqlErrorMessage error) {
      this.state = state;
      this.errorMessage = error;
    }

    private StateWithErrorMessage(final State state) {
      this.state = state;
      this.errorMessage = null;
    }
  }

  private final AtomicReference<StateWithErrorMessage> state = new AtomicReference<>(
      new StateWithErrorMessage(State.INITIALIZING,
              new KsqlErrorMessage(
                      Errors.ERROR_CODE_SERVER_NOT_READY,
                      "KSQL is not yet ready to serve requests."
              )
      )
  );

  /**
   * Creates a new ServerState with state INITIALIZING.
   */
  public ServerState() {
  }

  /**
   * Set a KsqlErrorMessage object containing a reason string explaining why the server
   * is still in the INITIALIZING state and an error code. This reason string and corresponding
   * error code will be used to add context to the API error indicating that the server is not
   * yet ready to serve requests.
   *
   * @param error KsqlErrorMessage object containing the error code and the error string.
   */
  public void setInitializingReason(final KsqlErrorMessage error) {
    this.state.set(new StateWithErrorMessage(State.INITIALIZING, error));
  }

  /**
   * Sets the server state to READY.
   */
  public void setReady() {
    this.state.set(new StateWithErrorMessage(State.READY));
  }

  /**
   * Sets the server state to TERMINATING.
   */
  public void setTerminating() {
    this.state.set(new StateWithErrorMessage(State.TERMINATING));
  }

  /**
   * Sets the server state to TERMINATED.
   */
  public void setTerminated() {
    this.state.set(new StateWithErrorMessage(State.TERMINATED));
  }

  /**
   * Checks whether the server is READY (and can therefore server requests). If it is not READY then
   * returns an API error to be sent back to the user.
   *
   * @return If empty, indicates that the server is ready. Otherwise, contains an error response to
   *     be returned back to the user.
   */
  public Optional<EndpointResponse> checkReady() {
    final StateWithErrorMessage state = this.state.get();
    switch (state.state) {
      case INITIALIZING:
        return Optional.of(Errors.serverNotReady(state.errorMessage));
      case TERMINATING:
        return Optional.of(Errors.serverShuttingDown());
      case TERMINATED:
        return Optional.of(Errors.serverShutDown());
      default:
    }
    return Optional.empty();
  }

  public State getState() {
    return this.state.get().state;
  }
}
