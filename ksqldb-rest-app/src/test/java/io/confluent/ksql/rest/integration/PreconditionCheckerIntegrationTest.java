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
import io.confluent.ksql.api.CorsTest;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.PreconditionChecker;
import io.confluent.ksql.rest.server.state.ServerState;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.time.Duration;
import java.util.Collections;
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
  private final CorsTest corsTest = new CorsTest(this::init, 503);

  private Vertx vertx;
  private WebClient client;

  private PreconditionChecker checker;

  @Before
  public void setup() {
    vertx = Vertx.vertx();
    PreconditionCheckerIntegrationTestPrecondition.ACTION.set(() -> Optional.of(new KsqlErrorMessage(123, "oops")));
  }

  @After
  public void cleanup() {
    if (checker != null) {
      checker.shutdown();
    }
    if (vertx != null) {
      vertx.close();
    }
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void shouldReturn503WhilePreconditionsDontPass() {
    // Given:
    init();
    final KsqlRestClient client = buildKsqlClient();

    // When:
    final RestResponse<?> rsp = client.getServerInfo();

    // Then:
    assertThat(rsp.getStatusCode(), equalTo(503));
  }

  @Test
  public void shouldExitAwaitOncePreconditionPasses() throws InterruptedException {
    // Given:
    init();
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
    init();
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

  @Test
  public void shouldNotBeCorsResponseIfNoCorsConfigured() throws Exception {
    corsTest.shouldNotBeCorsResponseIfNoCorsConfigured();
  }

  @Test
  public void shouldExcludePath() throws Exception {
    corsTest.shouldExcludePath(503);
  }

  @Test
  public void shouldNotBeCorsResponseIfNotCorsRequest() throws Exception {
    corsTest.shouldNotBeCorsResponseIfNotCorsRequest();
  }

  @Test
  public void shouldAcceptCorsRequestOriginExactMatch() throws Exception {
    corsTest.shouldAcceptCorsRequestOriginExactMatch();
  }

  @Test
  public void shouldRejectCorsRequestOriginExactMatch() throws Exception {
    corsTest.shouldRejectCorsRequestOriginExactMatch();
  }

  @Test
  public void shouldAcceptCorsRequestOriginExactMatchOneOfList() throws Exception {
    corsTest.shouldAcceptCorsRequestOriginExactMatchOneOfList();
  }

  @Test
  public void shouldRejectCorsRequestOriginExactMatchOenOfList() throws Exception {
    corsTest.shouldRejectCorsRequestOriginExactMatchOenOfList();
  }

  @Test
  public void shouldAcceptCorsRequestAcceptAll() throws Exception {
    corsTest.shouldAcceptCorsRequestAcceptAll();
  }

  @Test
  public void shouldAcceptCorsRequestWildcardMatch() throws Exception {
    corsTest.shouldAcceptCorsRequestWildcardMatch();
  }

  @Test
  public void shouldRejectCorsRequestWildcardMatch() throws Exception {
    corsTest.shouldRejectCorsRequestWildcardMatch();
  }

  @Test
  public void shouldAcceptCorsRequestWilcardMatchList() throws Exception {
    corsTest.shouldAcceptCorsRequestWilcardMatchList();
  }

  @Test
  public void shouldRejectCorsRequestWilcardMatchList() throws Exception {
    corsTest.shouldRejectCorsRequestWilcardMatchList();
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchDefaultHeaders() throws Exception {
    corsTest.shouldAcceptPreflightRequestOriginExactMatchDefaultHeaders();
  }

  @Test
  public void shouldRejectPreflightRequestOriginExactMatchDefaultHeaders() throws Exception {
    corsTest.shouldRejectPreflightRequestOriginExactMatchDefaultHeaders();
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedHeaders() throws Exception {
    corsTest.shouldAcceptPreflightRequestOriginExactMatchSpecifiedHeaders();
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedMethods() throws Exception {
    corsTest.shouldAcceptPreflightRequestOriginExactMatchSpecifiedMethods();
  }

  @Test
  public void shouldCallAllCorsTests() {
    corsTest.shouldCallAllCorsTests(this.getClass());
  }

  private void shouldReturn200ForHealthcheckEndpoint(final String subPath) {
    // When:
    init();
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

  private void init() {
    init(Collections.emptyMap());
  }

  private WebClient init(final Map<String, String> configs) {
    checker = new PreconditionChecker(
        () -> ImmutableMap.<String, String>builder()
            .putAll(PROPERTIES)
            .putAll(configs).build(),
        serverState
    );
    checker.startAsync();
    this.client = createClient();
    return client;
  }

  protected WebClient createClient() {
    return WebClient.create(vertx, createClientOptions());
  }

  protected WebClientOptions createClientOptions() {
    return new WebClientOptions()
        .setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false)
        .setDefaultHost(checker.getListeners().get(0).getHost())
        .setDefaultPort(checker.getListeners().get(0).getPort())
        .setReusePort(true);
  }
}
