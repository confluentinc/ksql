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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.resources.Errors;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.junit.Test;

public class ServerStateTest {
  private final ServerState serverState = new ServerState();

  @Test
  public void shouldReturnErrorWhenInitializing() {
    // Given:
    KsqlErrorMessage error = new KsqlErrorMessage(12345, "bad stuff is happening");
    serverState.setInitializingReason(error);

    // When:
    final Optional<Response> result = serverState.checkReady();

    // Then:
    assertThat(result.isPresent(), is(true));
    final Response response = result.get();
    final Response expected = Errors.serverNotReady(error);
    assertThat(response.getStatus(), equalTo(expected.getStatus()));
    assertThat(response.getEntity(), equalTo(expected.getEntity()));
  }

  @Test
  public void shouldReturnErrorWhenTerminating() {
    // Given:
    serverState.setTerminating();

    // When:
    final Optional<Response> result = serverState.checkReady();

    // Then:
    assertThat(result.isPresent(), is(true));
    final Response response = result.get();
    final Response expected = Errors.serverShuttingDown();
    assertThat(response.getStatus(), equalTo(expected.getStatus()));
    assertThat(response.getEntity(), equalTo(expected.getEntity()));
  }
}