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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import java.util.Optional;
import org.junit.Test;

public class ServerStateTest {
  private final ServerState serverState = new ServerState();

  @Test
  public void shouldReturnErrorWhenInitializing() {
    // Given:
    final KsqlErrorMessage error = new KsqlErrorMessage(12345, "bad stuff is happening");
    serverState.setInitializingReason(error);

    // When:
    final Optional<EndpointResponse> result = serverState.checkReady();

    // Then:
    assertThat(result.isPresent(), is(true));
    final EndpointResponse response = result.get();
    final EndpointResponse expected = Errors.serverNotReady(error);
    assertThat(response.getStatus(), equalTo(expected.getStatus()));
    assertThat(response.getEntity(), equalTo(expected.getEntity()));
  }

  @Test
  public void shouldReturnErrorWhenTerminating() {
    // Given:
    serverState.setTerminating();

    // When:
    final Optional<EndpointResponse> result = serverState.checkReady();

    // Then:
    assertThat(result.isPresent(), is(true));
    final EndpointResponse response = result.get();
    final EndpointResponse expected = Errors.serverShuttingDown();
    assertThat(response.getStatus(), equalTo(expected.getStatus()));
    assertThat(response.getEntity(), equalTo(expected.getEntity()));
  }

  @Test
  public void shouldReturnErrorWhenTerminated() {
    // Given:
    serverState.setTerminated();

    // When:
    final Optional<EndpointResponse> result = serverState.checkReady();

    // Then:
    assertThat(result.isPresent(), is(true));
    final EndpointResponse response = result.get();
    final EndpointResponse expected = Errors.serverShutDown();
    assertThat(response.getStatus(), equalTo(expected.getStatus()));
    assertThat(response.getEntity(), equalTo(expected.getEntity()));
  }
}