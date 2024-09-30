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

package io.confluent.ksql.rest.client;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponseDetail;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.confluent.ksql.util.VertxSslOptionsFactory;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.net.JksOptions;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscription;

public class KsqlClientTest {

  private static final ServerKeyStore SERVER_KEY_STORE = new ServerKeyStore();

  private Vertx vertx;
  private FakeApiServer server;
  private KsqlClient ksqlClient;
  private String deploymentId;
  private Map<String, Object> properties;
  private URI serverUri;

  @Before
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
    startServer(new HttpServerOptions().setPort(0).setHost("localhost"));
    properties = ImmutableMap.of("auto.offset.reset", "earliest");
    serverUri = URI.create("http://localhost:" + server.getPort());
    createClient(Optional.empty());
  }

  @After
  public void tearDown() throws Exception {
    ksqlClient.close();
    stopServer();
    vertx.close();
  }

  @Test
  public void shouldSendKsqlRequest() {

    // Given:
    String ksql = "some ksql";
    Object expectedResponse = setupExpectedResponse();

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<KsqlEntityList> resp = target.postKsqlRequest(ksql, Collections.emptyMap(), Optional.of(123L));

    // Then:
    assertThat(resp.get(), is(expectedResponse));
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));
    assertThat(server.getPath(), is("/ksql"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(getKsqlRequest(),
        is(new KsqlRequest(ksql, properties, Collections.emptyMap(), 123L)));
  }

  @Test
  public void shouldSendBasicAuthHeader() {

    // Given:
    setupExpectedResponse();

    BasicCredentials credentials = BasicCredentials.of("tim", "socks");
    ksqlClient.close();
    createClient(Optional.of(credentials));

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    target.postKsqlRequest("some ksql", Collections.emptyMap(), Optional.of(123L));

    // Then:
    assertThat(server.getHeaders().get("Authorization"), is(toAuthHeader(credentials)));

  }

  @Test
  public void shouldOverrideAuthHeader() {

    // Given:
    setupExpectedResponse();

    // When:
    KsqlTarget target = ksqlClient.target(serverUri).authorizationHeader("other auth");
    target.postKsqlRequest("some ksql", Collections.emptyMap(), Optional.of(123L));

    // Then:
    assertThat(server.getHeaders().get("Authorization"), is("other auth"));
  }

  @Test
  public void shouldOverrideProperties() {

    // Given:
    setupExpectedResponse();
    Map<String, Object> props = new HashMap<>();
    props.put("enable.auto.commit", true);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri).properties(props);
    target.postKsqlRequest("some ksql", Collections.emptyMap(), Optional.of(123L));

    // Then:
    assertThat(getKsqlRequest().getConfigOverrides(), is(props));
  }

  @Test
  public void shouldSendHeartbeatRequest() throws Exception {

    // Given:
    KsqlHostInfoEntity entity = new KsqlHostInfoEntity(serverUri.getHost(), serverUri.getPort());
    long timestamp = System.currentTimeMillis();
    server.setResponseObject(new HeartbeatResponse(true));

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    target.postAsyncHeartbeatRequest(entity, timestamp);

    Buffer body = server.waitForRequestBody();
    HeartbeatMessage hbm = KsqlClientUtil.deserialize(body, HeartbeatMessage.class);

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));
    assertThat(server.getPath(), is("/heartbeat"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(hbm, is(new HeartbeatMessage(entity, timestamp)));
  }

  @Test
  public void shouldRequestServerInfo() {

    // Given:
    ServerInfo expectedResponse = new ServerInfo("someversion",
        "kafkaclusterid", "ksqlserviceid", "status");
    server.setResponseObject(expectedResponse);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<ServerInfo> response = target.getServerInfo();

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.GET));
    assertThat(server.getBody(), nullValue());
    assertThat(server.getPath(), is("/info"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(response.get(), is(expectedResponse));
  }

  @Test
  public void shouldRequestServerHealthcheck() {

    // Given:
    Map<String, HealthCheckResponseDetail> map = new HashMap<>();
    map.put("foo", new HealthCheckResponseDetail(true));
    HealthCheckResponse healthCheckResponse = new HealthCheckResponse(true, map);
    server.setResponseObject(healthCheckResponse);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<HealthCheckResponse> response = target.getServerHealth();

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.GET));
    assertThat(server.getBody(), nullValue());
    assertThat(server.getPath(), is("/healthcheck"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(response.get(), is(healthCheckResponse));
  }

  @Test
  public void shouldRequestClusterStatus() {

    // Given:
    ClusterStatusResponse clusterStatusResponse = new ClusterStatusResponse(new HashMap<>());
    server.setResponseObject(clusterStatusResponse);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<ClusterStatusResponse> response = target.getClusterStatus();

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.GET));
    assertThat(server.getBody(), nullValue());
    // Yikes - this is camel case!
    assertThat(server.getPath(), is("/clusterStatus"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(response.get(), is(clusterStatusResponse));
  }

  @Test
  public void shouldRequestStatuses() {

    // Given:
    CommandStatuses commandStatuses = new CommandStatuses(new HashMap<>());
    server.setResponseObject(commandStatuses);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<CommandStatuses> response = target.getStatuses();

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.GET));
    assertThat(server.getBody(), nullValue());
    assertThat(server.getPath(), is("/status"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(response.get(), is(commandStatuses));
  }

  @Test
  public void shouldRequestStatus() {

    // Given:
    CommandStatus commandStatus = new CommandStatus(Status.SUCCESS, "msg");
    server.setResponseObject(commandStatus);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<CommandStatus> response = target.getStatus("foo");

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.GET));
    assertThat(server.getBody(), nullValue());
    assertThat(server.getPath(), is("/status/foo"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(response.get(), is(commandStatus));
  }

  @Test
  public void shouldPostQueryRequest() {

    // Given:
    List<StreamedRow> expectedResponse = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRow row = GenericRow.genericRow("foo", 123, true);
      StreamedRow sr = StreamedRow.pushRow(row);
      expectedResponse.add(sr);
    }
    server.setResponseBuffer(createResponseBuffer(expectedResponse));
    String sql = "some sql";

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<List<StreamedRow>> response = target.postQueryRequest(
        sql, Collections.emptyMap(), Optional.of(321L));

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));

    assertThat(server.getPath(), is("/query"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(getKsqlRequest(), is(new KsqlRequest(sql, properties, Collections.emptyMap(), 321L)));
    assertThat(response.getResponse(), is(expectedResponse));
  }

  @Test
  public void shouldNotTrimTrailingZerosOnDecimalDeserialization() {
    // Given:
    server.setResponseBuffer(Buffer.buffer(""
        + "["
        + "{\"row\": {\"columns\": [1.000, 12.100]}}"
        + "]"
    ));

    // When:
    final KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<List<StreamedRow>> response = target.postQueryRequest(
        "some sql", Collections.emptyMap(), Optional.of(321L));

    // Then:
    assertThat(response.getResponse(), is(ImmutableList.of(
        StreamedRow.pushRow(GenericRow.genericRow(new BigDecimal("1.000"), new BigDecimal("12.100")))
    )));
  }

  @Test
  public void shouldPostQueryRequestStreamed() throws Exception {

    // Given:

    int numRows = 10;
    List<StreamedRow> expectedResponse = setQueryStreamResponse(numRows, false);
    String sql = "some sql";

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<StreamPublisher<StreamedRow>> response = target
        .postQueryRequestStreamed(sql, Collections.emptyMap(), Optional.of(321L));

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));

    assertThat(server.getPath(), is("/query"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(getKsqlRequest(), is(new KsqlRequest(sql, properties, Collections.emptyMap(), 321L)));

    List<StreamedRow> rows = getElementsFromPublisher(numRows, response.getResponse());
    assertThat(rows, is(expectedResponse));
  }

  @Test
  public void shouldPostQueryRequestStreamedWithLimit() throws Exception {

    // Given:

    int numRows = 10;
    List<StreamedRow> expectedResponse = setQueryStreamResponse(numRows, true);
    String sql = "whateva";

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<StreamPublisher<StreamedRow>> response = target
        .postQueryRequestStreamed(sql, Collections.emptyMap(), Optional.of(321L));

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));

    assertThat(server.getPath(), is("/query"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(getKsqlRequest(), is(new KsqlRequest(sql, properties, Collections.emptyMap(), 321L)));

    List<StreamedRow> rows = getElementsFromPublisher(numRows + 1, response.getResponse());
    assertThat(rows, is(expectedResponse));
  }

  @Test
  public void shouldCloseConnectionWhenQueryStreamIsClosed() throws Exception {

    // Given:
    setQueryStreamResponse(1, false);
    String sql = "some sql";

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<StreamPublisher<StreamedRow>> response = target
        .postQueryRequestStreamed(sql, Collections.emptyMap(), Optional.of(321L));

    // Then:
    assertThat(getKsqlRequest(), is(new KsqlRequest(sql, properties, Collections.emptyMap(), 321L)));

    // When:
    response.getResponse().close();

    // Then:
    assertThatEventually(() -> server.isConnectionClosed(), is(true));
  }

  @Test
  public void shouldPostQueryRequestStreamedHttp2() throws Exception {

    // Given:

    int numRows = 10;
    List<List<?>> expectedResponse = setQueryStreamDelimitedResponse(numRows);
    String sql = "whateva";

    // When:
    KsqlTarget target = ksqlClient.targetHttp2(serverUri);
    CompletableFuture<RestResponse<StreamPublisher<StreamedRow>>> future = target
        .postQueryRequestStreamedAsync(sql, ImmutableMap.of());
    RestResponse<StreamPublisher<StreamedRow>> response = future.get();

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));

    assertThat(server.getPath(), is("/query-stream"));
    assertThat(server.getHeaders().get("Accept"), is("application/vnd.ksqlapi.delimited.v1"));
    assertThat(getQueryStreamArgs(), is(new QueryStreamArgs(sql, properties,
        Collections.emptyMap(), Collections.emptyMap())));

    List<StreamedRow> rows = getElementsFromPublisher(numRows + 1, response.getResponse());
    int i = 0;
    for (StreamedRow row : rows) {
      assertThat(row.getRow().isPresent(), is(true));
      assertThat(row.getRow().get().getColumns(), is(expectedResponse.get(i++)));
    }
  }

  @Test
  public void shouldFailWithNonHttp2Options() {
    // When:
    final IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> new KsqlClient(new HashMap<>(), Optional.empty(),
            new LocalProperties(properties),
            new HttpClientOptions().setVerifyHost(false),
            Optional.of(new HttpClientOptions()))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expecting http2 protocol version"
    ));
  }

  @Test
  public void shouldExecutePrintTopic() throws Exception {

    // Given:
    int numRows = 10;
    List<String> expectedResponse = setupPrintTopicResponse(numRows);
    String command = "print topic whatever";

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<StreamPublisher<String>> response = target
        .postPrintTopicRequest(command, Optional.of(123L));

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));

    assertThat(server.getPath(), is("/query"));
    assertThat(server.getHeaders().get("Accept"), is("application/json"));
    assertThat(getKsqlRequest(), is(new KsqlRequest(command, properties, Collections.emptyMap(), 123L)));

    List<String> lines = getElementsFromPublisher(numRows, response.getResponse());
    assertThat(lines, is(expectedResponse));
  }

  @Test
  public void shouldCloseConnectionWhenPrintTopicPublisherIsClosed() throws Exception {

    // Given:
    setupPrintTopicResponse(1);
    String command = "print topic whatever";

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<StreamPublisher<String>> response = target
        .postPrintTopicRequest(command, Optional.of(123L));

    // Then:
    assertThat(getKsqlRequest(), is(new KsqlRequest(command, properties, Collections.emptyMap(), 123L)));

    // When:
    response.getResponse().close();

    // Then:
    assertThatEventually(() -> server.isConnectionClosed(), is(true));
  }

  @Test
  public void shouldPerformRequestWithTls() throws Exception {
    ksqlClient.close();
    stopServer();

    // Given:
    startServerWithTls();
    startClientWithTls();

    KsqlEntityList expectedResponse = setupExpectedResponse();

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<KsqlEntityList> resp = target.postKsqlRequest("ssl test", Collections.emptyMap(), Optional.of(123L));

    // Then:
    assertThat(getKsqlRequest().getUnmaskedKsql(), is("ssl test"));
    assertThat(expectedResponse, is(resp.getResponse()));
  }

  @Test
  public void shouldFailToStartClientRequestWithNullKeystorePassword() throws Exception {
    ksqlClient.close();
    stopServer();

    // Given:
    startServerWithTls();

    // When:
    final KsqlRestClientException e = assertThrows(
        KsqlRestClientException.class,
        () -> startClientWithTlsAndTruststorePassword(null)
    );

    // Then:
    assertThat(e.getCause().getMessage(), containsString(
        "java.io.IOException: Keystore was tampered with, or password was incorrect"
    ));
  }


  @Test
  public void shouldFailToStartClientRequestWithInvalidKeystorePassword() throws Exception {
    ksqlClient.close();
    stopServer();

    // Given:
    startServerWithTls();

    // When:
    final KsqlRestClientException e = assertThrows(
        KsqlRestClientException.class,
        () -> startClientWithTlsAndTruststorePassword("iquwhduiqhwd")
    );

    // Then:
    assertThat(e.getCause().getMessage(), containsString(
        "java.io.IOException: Keystore was tampered with, or password was incorrect"
    ));
  }

  @Test
  public void shouldFailtoMakeHttpRequestWhenServerIsHttps() throws Exception {
    ksqlClient.close();
    stopServer();

    // Given:
    startServerWithTls();
    startClientWithTls();

    // When:
    URI uri = URI.create("http://localhost:" + server.getPort());
    KsqlTarget target = ksqlClient.target(uri);
    final KsqlRestClientException e = assertThrows(
        KsqlRestClientException.class,
        () ->target.postKsqlRequest("ssl test", Collections.emptyMap(), Optional.of(123L))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Error issuing POST to KSQL server. path:/ksql"));
  }

  @Test
  public void shouldFailtoMakeHttpsRequestWhenServerIsHttp() {
    // When:
    URI uri = URI.create("https://localhost:" + server.getPort());
    KsqlTarget target = ksqlClient.target(uri);
    final KsqlRestClientException e = assertThrows(
        KsqlRestClientException.class,
        () ->  target.postKsqlRequest("ssl test", Collections.emptyMap(), Optional.of(123L))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Error issuing POST to KSQL server. path:/ksql"
    ));
  }

  @Test
  public void shouldHandleUnauthorizedOnPostRequests() {

    // Given:
    server.setErrorCode(401);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<KsqlEntityList> response = target.postKsqlRequest("sql", Collections.emptyMap(), Optional.of(123L));

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));
    assertThat(response.isErroneous(), is(true));
    assertThat(response.getErrorMessage().getMessage(),
        is("Could not authenticate successfully with the supplied credentials."));
  }

  @Test
  public void shouldHandleUnauthorizedOnGetRequests() {

    // Given:
    server.setErrorCode(401);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<ServerInfo> response = target.getServerInfo();

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.GET));
    assertThat(response.isErroneous(), is(true));
    assertThat(response.getErrorMessage().getMessage(),
        is("Could not authenticate successfully with the supplied credentials."));
  }

  @Test
  public void shouldHandleErrorMessageOnPostRequests() {
    // Given:
    KsqlErrorMessage ksqlErrorMessage = new KsqlErrorMessage(40000, "ouch");
    server.setResponseObject(ksqlErrorMessage);
    server.setErrorCode(400);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<KsqlEntityList> response = target.postKsqlRequest("sql", Collections.emptyMap(), Optional.of(123L));

    // Then:
    assertThat(response.getStatusCode(), is(400));
    assertThat(response.getErrorMessage().getErrorCode(), is(40000));
    assertThat(response.getErrorMessage().getMessage(), is("ouch"));
  }

  @Test
  public void shouldHandleErrorMessageOnGetRequests() {
    // Given:
    server.setResponseObject(new KsqlErrorMessage(40000, "ouch"));
    server.setErrorCode(400);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<ServerInfo> response = target.getServerInfo();

    // Then:
    assertThat(response.getStatusCode(), is(400));
    assertThat(response.getErrorMessage().getErrorCode(), is(40000));
    assertThat(response.getErrorMessage().getMessage(), is("ouch"));
  }

  @Test
  public void shouldHandleForbiddenOnPostRequests() {

    // Given:
    server.setErrorCode(403);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<KsqlEntityList> response = target.postKsqlRequest("sql", Collections.emptyMap(), Optional.of(123L));

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));
    assertThat(response.isErroneous(), is(true));
    assertThat(response.getErrorMessage().getMessage(),
        is("You are forbidden from using this cluster."));
  }

  @Test
  public void shouldHandleForbiddenOnGetRequests() {

    // Given:
    server.setErrorCode(403);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<ServerInfo> response = target.getServerInfo();

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.GET));
    assertThat(response.isErroneous(), is(true));
    assertThat(response.getErrorMessage().getMessage(),
        is("You are forbidden from using this cluster."));
  }

  @Test
  public void shouldHandleArbitraryErrorsOnPostRequests() {

    // Given:
    server.setErrorCode(417);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<KsqlEntityList> response = target.postKsqlRequest("sql", Collections.emptyMap(), Optional.of(123L));

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.POST));
    assertThat(response.isErroneous(), is(true));
    assertThat(response.getErrorMessage().getMessage(),
        is("The server returned an unexpected error: Expectation Failed"));
  }

  @Test
  public void shouldHandleArbitraryErrorsOnGetRequests() {

    // Given:
    server.setErrorCode(417);

    // When:
    KsqlTarget target = ksqlClient.target(serverUri);
    RestResponse<ServerInfo> response = target.getServerInfo();

    // Then:
    assertThat(server.getHttpMethod(), is(HttpMethod.GET));
    assertThat(response.isErroneous(), is(true));
    assertThat(response.getErrorMessage().getMessage(),
        is("The server returned an unexpected error: Expectation Failed"));
  }

  private List<StreamedRow> setQueryStreamResponse(int numRows, boolean limitReached) {
    List<StreamedRow> expectedResponse = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      GenericRow row = GenericRow.genericRow("foo", 123, true);
      StreamedRow sr = StreamedRow.pushRow(row);
      expectedResponse.add(sr);
    }
    if (limitReached) {
      expectedResponse.add(StreamedRow.finalMessage("Limit reached"));
    }
    server.setResponseBuffer(createResponseBuffer(expectedResponse));
    return expectedResponse;
  }

  private List<List<?>> setQueryStreamDelimitedResponse(int numRows) {
    List<List<?>> expectedResponse = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      GenericRow row = GenericRow.genericRow("foo", 123, true);
      expectedResponse.add(row.values());
    }
    server.setResponseBuffer(createDelimitedResponseBuffer(expectedResponse));
    return expectedResponse;
  }

  private List<String> setupPrintTopicResponse(int numRows) {
    List<String> expectedResponse = new ArrayList<>();
    Buffer responseBuffer = Buffer.buffer();
    for (int i = 0; i < numRows; i++) {
      String line = "this is row " + i;
      expectedResponse.add(line);
      if (i % 2 == 0) {
        // The server can include empty lines - we need to test this too
        responseBuffer.appendString("\n");
      }
      responseBuffer.appendString(line).appendString("\n");
    }
    server.setResponseBuffer(responseBuffer);
    return expectedResponse;
  }

  private void startClientWithTls() {
    Map<String, String> props = new HashMap<>();
    props.putAll(ClientTrustStore.trustStoreProps());
    createClient(props);
  }

  private void startClientWithTlsAndTruststorePassword(final String password) {
    Map<String, String> props = new HashMap<>();
    props.putAll(ClientTrustStore.trustStoreProps());
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, password);
    createClient(props);
    ksqlClient.target(serverUri).getServerInfo().get();
  }

  private void startServerWithTls() throws Exception {
    final Optional<JksOptions> keyStoreOptions = VertxSslOptionsFactory.buildJksKeyStoreOptions(
    SERVER_KEY_STORE.keyStoreProps(), Optional.ofNullable(SERVER_KEY_STORE.getKeyAlias()));

    HttpServerOptions serverOptions = new HttpServerOptions().setPort(0)
        .setHost("localhost")
        .setSsl(true)
        .setKeyStoreOptions(keyStoreOptions.get());

    startServer(serverOptions);
    serverUri = URI.create("https://localhost:" + server.getPort());
  }

  private void startServer(HttpServerOptions httpServerOptions) throws Exception {
    server = new FakeApiServer(httpServerOptions);
    VertxCompletableFuture<String> deployFuture = new VertxCompletableFuture<>();
    vertx.deployVerticle(server, deployFuture);
    deploymentId = deployFuture.get();
  }

  private void stopServer() throws Exception {
    VertxCompletableFuture<Void> undeployFuture = new VertxCompletableFuture<>();
    vertx.undeploy(deploymentId, undeployFuture);
    undeployFuture.get();
  }

  private <T> List<T> getElementsFromPublisher(int numElements, StreamPublisher<T> publisher)
      throws Exception {
    CompletableFuture<List<T>> future = new CompletableFuture<>();
    CollectSubscriber<T> subscriber = new CollectSubscriber<>(vertx.getOrCreateContext(),
        future, numElements);
    publisher.subscribe(subscriber);
    return future.get();
  }

  private KsqlEntityList setupExpectedResponse() {
    TopicDescription topicDescription = new TopicDescription("statement", "name",
        "kafkatopic", "format", "schemaString");
    List<KsqlEntity> entities = new ArrayList<>();
    entities.add(topicDescription);
    KsqlEntityList expectedResponse = new KsqlEntityList(entities);
    server.setResponseObject(expectedResponse);
    return expectedResponse;
  }

  private void createClient(Optional<BasicCredentials> credentials) {
    ksqlClient = new KsqlClient(new HashMap<>(), credentials,
        new LocalProperties(properties),
        new HttpClientOptions().setVerifyHost(false),
        Optional.of(new HttpClientOptions().setProtocolVersion(HttpVersion.HTTP_2)));
  }

  private void createClient(Map<String, String> clientProps) {
    ksqlClient = new KsqlClient(clientProps, Optional.empty(),
        new LocalProperties(properties),
        new HttpClientOptions().setVerifyHost(false),
        Optional.of(new HttpClientOptions().setProtocolVersion(HttpVersion.HTTP_2)));
  }

  private String toAuthHeader(BasicCredentials credentials) {
    return "Basic " + Base64.getEncoder()
        .encodeToString((credentials.username()
            + ":" + credentials.password()).getBytes(StandardCharsets.UTF_8));
  }

  private KsqlRequest getKsqlRequest() {
    return KsqlClientUtil.deserialize(server.getBody(), KsqlRequest.class);
  }

  private QueryStreamArgs getQueryStreamArgs() {
    return KsqlClientUtil.deserialize(server.getBody(), QueryStreamArgs.class);
  }

  private static Buffer createResponseBuffer(List<StreamedRow> rows) {
    // The old API returns rows in a strange format - it looks like a JSON array but it isn't.
    // There are newlines separating the elements which are illegal in JSON
    // And there can be an extra comma after the last element
    // There can also be random newlines in the response
    Buffer buffer = Buffer.buffer();
    buffer.appendString("[");
    for (int i = 0; i < rows.size(); i++) {
      buffer.appendBuffer(KsqlClientUtil.serialize(rows.get(i)));
      buffer.appendString(",\n");
      if (i == rows.size() / 2) {
        // insert a random newline for good measure - the server can actually do this
        buffer.appendString("\n");
      }
    }
    buffer.appendString("]");
    return buffer;
  }

  private static Buffer createDelimitedResponseBuffer(List<List<?>> rows) {
    // The old API returns rows in a strange format - it looks like a JSON array but it isn't.
    // There are newlines separating the elements which are illegal in JSON
    // And there can be an extra comma after the last element
    // There can also be random newlines in the response
    Buffer buffer = Buffer.buffer();
    for (int i = 0; i < rows.size(); i++) {
      buffer.appendBuffer(KsqlClientUtil.serialize(rows.get(i)));
      buffer.appendString(",\n");
      if (i == rows.size() / 2) {
        // insert a random newline for good measure - the server can actually do this
        buffer.appendString("\n");
      }
    }
    return buffer;
  }

  private static class CollectSubscriber<T> extends BaseSubscriber<T> {

    private final CompletableFuture<List<T>> future;
    private final List<T> list = new ArrayList<>();
    private final int numRows;

    public CollectSubscriber(final Context context, CompletableFuture<List<T>> future,
        final int numRows) {
      super(context);
      this.future = future;
      this.numRows = numRows;
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      makeRequest(1);
    }

    @Override
    protected void handleValue(final T value) {
      list.add(value);
      if (list.size() == numRows) {
        future.complete(new ArrayList<>(list));
      }
      makeRequest(1);
    }

    @Override
    protected void handleComplete() {
      future.complete(new ArrayList<>(list));
    }

    @Override
    protected void handleError(final Throwable t) {
      future.completeExceptionally(t);
    }
  }

}
