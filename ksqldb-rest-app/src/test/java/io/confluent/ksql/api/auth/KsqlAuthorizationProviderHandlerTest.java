/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.auth;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.web.RoutingContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlAuthorizationProviderHandlerTest {

  @Mock
  private Server server;
  @Mock
  private KsqlAuthorizationProvider authProvider;
  @Mock
  private WorkerExecutor workerExecutor;
  @Mock
  private RoutingContext routingContext;

  @Test
  public void shouldRespectServerAuthSkipPathsConfig() {
    // Given:
    Mockito.when(server.getWorkerExecutor()).thenReturn(workerExecutor);
    Mockito.when(server.getConfig()).thenReturn(new KsqlRestConfig(
        ImmutableMap.of(
            KsqlRestConfig.AUTHENTICATION_SKIP_PATHS_CONFIG,
            ImmutableList.of("/heartbeat")
        )
    ));
    Mockito.when(routingContext.normalisedPath()).thenReturn("/heartbeat");
    KsqlAuthorizationProviderHandler handler = new KsqlAuthorizationProviderHandler(server, authProvider);

    // When:
    handler.handle(routingContext);

    // Then (make sure the authorization "work" is skipped):
    Mockito.verify(routingContext).next();
    Mockito.verifyNoInteractions(workerExecutor);
  }

  @Test
  public void shouldNotSkipNonAuthenticatedPaths() {
    // Given:
    Mockito.when(server.getWorkerExecutor()).thenReturn(workerExecutor);
    Mockito.when(server.getConfig()).thenReturn(new KsqlRestConfig(
        ImmutableMap.of(
            KsqlRestConfig.AUTHENTICATION_SKIP_PATHS_CONFIG,
            ImmutableList.of("/heartbeat")
        )
    ));
    Mockito.when(routingContext.normalisedPath()).thenReturn("/foo");
    KsqlAuthorizationProviderHandler handler = new KsqlAuthorizationProviderHandler(server, authProvider);

    // When:
    handler.handle(routingContext);

    // Then (make sure the authorization "work" is not skipped):
    Mockito.verify(routingContext, Mockito.never()).next();
    Mockito.verify(workerExecutor).executeBlocking(Mockito.any(), Mockito.anyBoolean(), Mockito.any());
  }

}