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

package io.confluent.ksql.rest.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.PreconditionChecker;
import io.confluent.ksql.rest.server.state.ServerState;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class PreconditionCheckerIntegrationTest {
  private static final Map<String, String> PROPERTIES = ImmutableMap.of(
      KsqlRestConfig.KSQL_SERVER_PRECONDITIONS,
      PreconditionCheckerIntegrationTestPrecondition.class.getCanonicalName(),
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:8888"
  );

  private final ServerState serverState = new ServerState();

  private PreconditionChecker checker;

  @Before
  public void setup() {
    PreconditionCheckerIntegrationTestPrecondition.ACTION.set(() -> Optional.of(new KsqlErrorMessage(123, "oops")));
    checker = new PreconditionChecker(
        () -> PROPERTIES,
        serverState
    );
    checker.startAsync();
  }

  @After
  public void cleanup() {
    checker.shutdown();
  }

  @Test
  public void shouldReturn503WhilePreconditionsDontPass() {
    // Given:
    final KsqlRestClient client = buildKsqlClient();

    // When:
    final RestResponse<?> rsp = client.getServerInfo();

    // Then:
    assertThat(rsp.getStatusCode(), equalTo(503));
  }

  @Test
  public void shouldExitAwaitOncePreconditionPasses() throws InterruptedException {
    // Given:
    final Thread waiter = new Thread(() -> checker.awaitTerminated());
    waiter.start();
    PreconditionCheckerIntegrationTestPrecondition.ACTION.set(Optional::empty);

    // When:
    waiter.join(Duration.ofSeconds(30).toMillis());

    // Then:
    assertThat(waiter.isAlive(), is(false));
  }

  @Test
  public void shouldExitWhenNotifiedToTerminate() throws InterruptedException {
    // Given:
    final Thread waiter = new Thread(() -> checker.awaitTerminated());
    waiter.start();
    checker.notifyTerminated();

    // When:
    waiter.join(Duration.ofSeconds(30).toMillis());

    // Then:
    assertThat(waiter.isAlive(), is(false));
  }

  @Test
  public void shouldReturn200ForReadyEndpoint() {
    shouldReturn200ForHealthcheckEndpoint("ready");
  }

  @Test
  public void shouldReturn200ForLivenessEndpoint() {
    shouldReturn200ForHealthcheckEndpoint("live");
  }

  private void shouldReturn200ForHealthcheckEndpoint(final String subPath) {
    // When:
    HttpResponse<Buffer> resp = RestIntegrationTestUtil.rawRestRequest(
        checker.getListeners().get(0),
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/chc/" + subPath,
        null,
        "application/json",
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    assertThat(resp.statusCode(), equalTo(200));
    assertThat(resp.bodyAsString(), equalTo("{}"));
  }

  public KsqlRestClient buildKsqlClient() {
    return KsqlRestClient.create(
        checker.getListeners().get(0).toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        Optional.empty(),
        Optional.empty()
    );
  }

}
