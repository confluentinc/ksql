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

package io.confluent.ksql.rest.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.vertx.core.Vertx;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PreconditionCheckerTest {
  private static final KsqlErrorMessage ERROR = new KsqlErrorMessage(123, "oops");
  private static final Map<String, String> PROPERTIES = ImmutableMap.of();

  @Mock
  private PreconditionServer server;
  @Mock
  private KsqlServerPrecondition precondition1;
  @Mock
  private KsqlServerPrecondition precondition2;
  @Mock
  private Vertx vertx;
  @Mock
  private Vertx clientVertx;
  @Mock
  private Admin admin;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ServerState serverState;
  @Mock
  private Supplier<Map<String, String>> propertiesLoader;

  private PreconditionChecker checker;

  private final KsqlRestConfig restConfig = new KsqlRestConfig(PROPERTIES);

  @Before
  public void setup() {
    checker = new PreconditionChecker(
        propertiesLoader,
        restConfig,
        () -> new PreconditionChecker.Clients(serviceContext, clientVertx, admin, topicClient),
        vertx,
        ImmutableList.of(precondition1, precondition2),
        server,
        serverState
    );
    when(precondition1.checkPrecondition(any(), any(), any())).thenReturn(Optional.empty());
    when(precondition2.checkPrecondition(any(), any(), any())).thenReturn(Optional.empty());
    when(server.started()).thenReturn(true);
    when(propertiesLoader.get()).thenReturn(PROPERTIES);
  }

  @Test
  public void shouldNotStartServerIfNoPreconditions() {
    // Given:
    PreconditionChecker checker = new PreconditionChecker(
        propertiesLoader,
        restConfig,
        () -> new PreconditionChecker.Clients(serviceContext, clientVertx, admin, topicClient),
        vertx,
        ImmutableList.of(),
        server,
        serverState
    );

    // When:
    checker.startAsync();

    // Then:
    verifyNoInteractions(server);
  }

  @Test
  public void shouldNotStartServerIfPreconditionsPass() {
    // When:
    checker.startAsync();

    // Then:
    verifyNoInteractions(server);
  }

  @Test
  public void shouldCloseClientsIfPreconditionsPass() {
    // When:
    checker.startAsync();

    // Then:
    verify(serviceContext).close();
    verify(admin).close();
    verify(clientVertx).close();
  }

  @Test
  public void shouldStartServerIfPreconditionsDoNotPass() {
    // Given:
    givenPreconditionFailures(precondition1);

    // When:
    checker.startAsync();

    // Then:
    verify(server).start();
  }

  @Test
  public void shouldUpdateServerStateOnFailure() {
    // Given:
    givenPreconditionFailures(precondition1);

    // When:
    checker.startAsync();
    checker.awaitTerminated();

    // Then:
    verify(serverState).setInitializingReason(ERROR);
  }

  @Test
  public void shouldRecheckPreconditions() {
    // Given:
    givenPreconditionFailures(precondition1);

    // When:
    checker.startAsync();
    checker.awaitTerminated();

    // Then:
    verify(serviceContext, times(4)).close();
    verify(admin, times(4)).close();
    verify(clientVertx, times(4)).close();
  }

  @Test
  public void shouldCloseClientsWhenPreconditionsCheckedInServer() {
    // Given:
    givenPreconditionFailures(precondition1);

    // When:
    checker.startAsync();
    checker.awaitTerminated();

    // Then:
    verify(precondition1, times(3))
        .checkPrecondition(PROPERTIES, serviceContext, topicClient);
  }

  @Test
  public void shouldPassFreshPropertiesToPreconditions() {
    // Given:
    givenPreconditionFailures(precondition1);
    final Map<String, String> properties1 = ImmutableMap.of("a", "b");
    final Map<String, String> properties2 = ImmutableMap.of("c", "d");
    final Map<String, String> properties3 = ImmutableMap.of("e", "f");
    when(propertiesLoader.get())
        .thenReturn(properties1)
        .thenReturn(properties2)
        .thenReturn(properties3);

    // When:
    checker.startAsync();
    checker.awaitTerminated();

    // Then:
    verify(precondition1).checkPrecondition(properties1, serviceContext, topicClient);
    verify(precondition1).checkPrecondition(properties2, serviceContext, topicClient);
    verify(precondition1).checkPrecondition(properties3, serviceContext, topicClient);
  }

  @Test
  public void shouldCloseInners() {
    // When:
    checker.shutdown();

    // Then:
    verify(server).stop();
    verify(vertx).close();
  }

  @Test
  public void shouldNotStopServerIfNotStarted() {
    // Given:
    when(server.started()).thenReturn(false);

    // When:
    checker.shutdown();

    // Then:
    verify(server, never()).stop();
  }

  void givenPreconditionFailures(final KsqlServerPrecondition precondition) {
    when(precondition.checkPrecondition(any(), any(), any()))
        .thenReturn(Optional.of(ERROR))
        .thenReturn(Optional.of(ERROR))
        .thenReturn(Optional.empty());
  }
}