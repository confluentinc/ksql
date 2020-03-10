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

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

/**
 * A javax request filter that ensures the KSQL server is in the READY state before
 * serving requests.
 */
public class ServerStateFilter implements ContainerRequestFilter {
  private final ServerState state;

  ServerStateFilter(final ServerState state) {
    this.state = state;
  }

  /**
   * Ensures the KSQL server is in the READY state. If not, filter will abort the passed
   * in request context with the API error returned by the state machine.
   *
   * @param requestContext the ContainerRequestContext instance provided by the javax
   *                       implementation.
   */
  @Override
  public void filter(final ContainerRequestContext requestContext) {
    state.checkReady().ifPresent(requestContext::abortWith);
  }
}
