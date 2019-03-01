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

package io.confluent.ksql.cli;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.TestKsqlRestApp;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.confluent.rest.RestConfig;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLHandshakeException;
import javax.ws.rs.ProcessingException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class SslFunctionalTest {

  private static final ImmutableMap<String, String> TRUST_STORE_PROPS = ImmutableMap
      .<String, String>builder()
      .putAll(ClientTrustStore.trustStoreProps())
      .build();

  private static final EmbeddedSingleNodeKafkaCluster CLUSTER = EmbeddedSingleNodeKafkaCluster
      .newBuilder()
      .build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(CLUSTER::bootstrapServers)
      .withProperties(ServerKeyStore.keyStoreProps())
      .withProperty(RestConfig.LISTENERS_CONFIG, "https://localhost:0")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(CLUSTER).around(REST_APP);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private Map<String, String> clientProps;

  @Before
  public void setUp() {
    clientProps = Collections.emptyMap();
  }

  @Test
  public void shouldNotBeAbleToUseCliWithIfClientDoesNotTrustServerCert() {
    // Then:
    expectedException.expect(KsqlRestClientException.class);
    expectedException.expectCause(is(instanceOf(ProcessingException.class)));
    expectedException.expectCause(hasCause(is(instanceOf(SSLHandshakeException.class))));

    // When:
    canMakeCliRequest();
  }

  @Test
  public void shouldBeAbleToUseCliOverHttps() {
    // Given:
    givenTrustStoreConfigured();

    // When:
    final Code result = canMakeCliRequest();

    // Then:
    assertThat(result, is(Code.OK));
  }

  @Test
  public void shouldNotTableAbleToUseWssIfClientDoesNotTrustServerCert() throws Exception {
    // Then:
    expectedException.expect(SSLHandshakeException.class);
    expectedException.expectCause(hasCause(hasMessage(containsString(
        "unable to find valid certification path to requested target"))));

    // When:
    makeWsRequest(false);
  }

  @Test
  public void shouldBeAbleToUseWss() throws Exception {
    // When:
    final Code result = makeWsRequest(true);

    // Then:
    assertThat(result, is(Code.OK));
  }

  private void givenTrustStoreConfigured() {
    clientProps = TRUST_STORE_PROPS;
  }

  private Code canMakeCliRequest() {
    final String serverAddress = REST_APP.getHttpsListener().toString();

    try (KsqlRestClient restClient = new KsqlRestClient(serverAddress, emptyMap(), clientProps)) {

      final RestResponse<?> response = restClient.makeKsqlRequest("show topics;");
      if (response.isSuccessful()) {
        return Code.OK;
      }

      return HttpStatus.getCode(response.getErrorMessage().getErrorCode());
    }
  }

  private static Code makeWsRequest(final boolean setUpTrustStore) throws Exception {
    final SslContextFactory sslContextFactory = new SslContextFactory();
    if (setUpTrustStore) {
      sslContextFactory.setTrustStorePath(ClientTrustStore.trustStorePath());
      sslContextFactory.setTrustStorePassword(ClientTrustStore.trustStorePassword());
      sslContextFactory.setEndpointIdentificationAlgorithm("");
    }

    final HttpClient httpClient = new HttpClient(sslContextFactory);
    httpClient.start();
    final WebSocketClient wsClient = new WebSocketClient(httpClient);
    wsClient.start();

    try {
      final ClientUpgradeRequest request = new ClientUpgradeRequest();
      final WebSocketListener listener = new WebSocketListener();
      final URI wsUri = REST_APP.getWssListener().resolve("/ws/query");

      wsClient.connect(listener, wsUri, request);

      assertThat("Response received",
          listener.latch.await(30, TimeUnit.SECONDS), is(true));

      final Throwable error = listener.error.get();
      return error == null ? Code.OK : extractStatusCode(error);
    } finally {
      wsClient.stop();
      httpClient.destroy();
    }
  }

  private static Code extractStatusCode(final Throwable t) throws Exception {
    if (t instanceof UpgradeException) {
      final Code code = HttpStatus.getCode(((UpgradeException) t).getResponseStatusCode());
      if (code == null) {
        throw new RuntimeException(t.getCause());
      }
      return code;
    }

    if (t instanceof Exception) {
      throw (Exception) t;
    }

    throw new RuntimeException(t);
  }

  @SuppressWarnings("unused")
  @WebSocket
  public static class WebSocketListener {

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<>();

    @OnWebSocketConnect
    public void onConnect(final Session session) {
      session.close();
      latch.countDown();
    }

    @OnWebSocketError
    public void onError(final Throwable t) {
      error.compareAndSet(null, t);
      latch.countDown();
    }
  }
}
