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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ServerStateFilterTest {
  @Mock
  private ContainerRequestContext requestContext;
  @Mock
  private ServerState serverState;
  @Mock
  private Response response;
  private ServerStateFilter filter;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    filter = new ServerStateFilter(serverState);
  }

  @Test
  public void shouldAbortRequestIfStateCheckFails() {
    // Given:
    when(serverState.checkReady()).thenReturn(Optional.of(response));

    // When:
    filter.filter(requestContext);

    // Then:
    verify(requestContext).abortWith(response);
  }

  @Test
  public void shouldDoNothingIfStateCheckPasses() {
    // When:
    filter.filter(requestContext);

    // Then:
    verifyZeroInteractions(requestContext);
  }
}