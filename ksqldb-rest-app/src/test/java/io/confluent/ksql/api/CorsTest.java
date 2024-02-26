/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class CorsTest extends BaseApiTest {

  private static final String ACCESS_CONTROL_ALLOW_ORIGIN_HEADER = "Access-Control-Allow-Origin";
  private static final String ACCESS_CONTROL_ALLOW_METHODS_HEADER = "Access-Control-Allow-Methods";
  private static final String ACCESS_CONTROL_ALLOW_HEADERS_HEADER = "Access-Control-Allow-Headers";
  private static final String ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER = "Access-Control-Allow-Credentials";

  private static final String ACCESS_CONTROL_REQUEST_METHOD_HEADER = "Access-Control-Request-Method";

  private static final String DEFAULT_ACCESS_CONTROL_ALLOW_HEADERS = "X-Requested-With,Content-Type,Accept,Origin";
  private static final String DEFAULT_ACCESS_CONTROL_ALLOW_METHODS = "GET,POST,HEAD";

  private static final String URI = "/query-stream";
  private static final String ORIGIN = "http://wibble.com";

  private final Map<String, Object> config = new HashMap<>();

  @Before
  public void setUp() {

    vertx = Vertx.vertx();
    vertx.exceptionHandler(t -> log.error("Unhandled exception in Vert.x", t));

    testEndpoints = new TestEndpoints();
    serverState = new ServerState();
    serverState.setReady();
    setDefaultRowGenerator();
  }

  @Test
  public void shouldNotBeCorsResponseIfNoCorsConfigured() throws Exception {

    // Given:
    init();

    // When
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.POST, ORIGIN);

    // Then:
    assertThat(response.statusCode(), is(200));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER), is(nullValue()));
  }

  @Test
  public void shouldExcludePath() throws Exception {

    // Given:
    init();
    config.put(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, ORIGIN);

    // When
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.POST, "/ws/foo", ORIGIN);

    // Then:
    // We should get a 404 as the security checks will be bypassed but the resource doesn't exist
    assertThat(response.statusCode(), is(404));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER), is(nullValue()));
  }

  @Test
  public void shouldNotBeCorsResponseIfNotCorsRequest() throws Exception {

    // Given:
    init();
    config.put(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, ORIGIN);

    // When
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .request(HttpMethod.POST, URI)
        .sendJsonObject(new JsonObject().put("sql", DEFAULT_PULL_QUERY), requestFuture);
    HttpResponse<Buffer> response = requestFuture.get();

    // Then:
    assertThat(response.statusCode(), is(200));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER), is(nullValue()));
  }

  @Test
  public void shouldAcceptCorsRequestOriginExactMatch() throws Exception {
    shouldAcceptCorsRequest(ORIGIN, ORIGIN);
  }

  @Test
  public void shouldRejectCorsRequestOriginExactMatch() throws Exception {
    shouldRejectCorsRequest("foo.com", ORIGIN);
  }

  @Test
  public void shouldAcceptCorsRequestOriginExactMatchOneOfList() throws Exception {
    shouldAcceptCorsRequest(ORIGIN, "foo.com," + ORIGIN);
  }

  @Test
  public void shouldRejectCorsRequestOriginExactMatchOenOfList() throws Exception {
    shouldRejectCorsRequest("foo.com", "wibble.com,blah.com");
  }

  @Test
  public void shouldAcceptCorsRequestAcceptAll() throws Exception {
    shouldAcceptCorsRequest(ORIGIN, "*");
  }

  @Test
  public void shouldAcceptCorsRequestWildcardMatch() throws Exception {
    shouldAcceptCorsRequest("foo.wibble.com", "*.wibble.com");
  }

  @Test
  public void shouldRejectCorsRequestWildcardMatch() throws Exception {
    shouldRejectCorsRequest("foo.wibble.com", "*.blibble.com");
  }

  @Test
  public void shouldAcceptCorsRequestWilcardMatchList() throws Exception {
    shouldAcceptCorsRequest("foo.wibble.com", "blah.com,*.wibble.com,foo.com");
  }

  @Test
  public void shouldRejectCorsRequestWilcardMatchList() throws Exception {
    shouldRejectCorsRequest("foo.wibble.com", "blah.com,*.blibble.com,foo.com");
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchDefaultHeaders() throws Exception {
    shouldAcceptPreflightRequest(ORIGIN, ORIGIN, DEFAULT_ACCESS_CONTROL_ALLOW_HEADERS,
        DEFAULT_ACCESS_CONTROL_ALLOW_METHODS);
  }

  @Test
  public void shouldRejectPreflightRequestOriginExactMatchDefaultHeaders() throws Exception {
    shouldRejectPreflightRequest(ORIGIN, "flibble.com");
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedHeaders() throws Exception {
    config.put(KsqlRestConfig.ACCESS_CONTROL_ALLOW_HEADERS, "foo-header,bar-header");
    shouldAcceptPreflightRequest(ORIGIN, ORIGIN,
        "foo-header,bar-header",
        DEFAULT_ACCESS_CONTROL_ALLOW_METHODS);
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedMethods() throws Exception {
    config.put(KsqlRestConfig.ACCESS_CONTROL_ALLOW_METHODS, "DELETE,PATCH");
    shouldAcceptPreflightRequest(ORIGIN, ORIGIN,
        DEFAULT_ACCESS_CONTROL_ALLOW_HEADERS,
        "DELETE,PATCH");
  }

  private void shouldAcceptCorsRequest(final String origin, final String allowedOrigins)
      throws Exception {

    // Given:
    config.put(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, allowedOrigins);
    init();

    // When
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.POST, origin);

    // Then:
    assertThat(response.statusCode(), is(200));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER),
        is(origin));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER), is("true"));
  }

  private void shouldRejectCorsRequest(final String origin, final String allowedOrigins)
      throws Exception {

    // Given:
    config.put(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, allowedOrigins);
    init();

    // When
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.POST, origin);

    // Then:
    assertThat(response.statusCode(), is(403));
  }

  private void shouldRejectPreflightRequest(final String origin, final String allowedOrigins)
      throws Exception {

    // Given:
    config.put(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, allowedOrigins);
    init();

    // When:
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.OPTIONS,
        MultiMap.caseInsensitiveMultiMap().add("origin", origin)
            .add(ACCESS_CONTROL_REQUEST_METHOD_HEADER, "POST"));

    // Then:
    assertThat(response.statusCode(), is(403));
  }

  private void shouldAcceptPreflightRequest(final String origin, final String allowedOrigins,
      final String accessControlAllowHeaders, final String accessControlAllowMethods)
      throws Exception {

    config.put(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, allowedOrigins);
    init();

    // Given
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.OPTIONS,
        MultiMap.caseInsensitiveMultiMap().add("origin", origin)
            .add(ACCESS_CONTROL_REQUEST_METHOD_HEADER, "POST"));

    assertThat(response.statusCode(), is(204));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER),
        is(origin));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER), is("true"));
    assertListsInAnyOrder(response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER),
        accessControlAllowHeaders);
    assertListsInAnyOrder(response.getHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER),
        accessControlAllowMethods);
  }

  private void assertListsInAnyOrder(String str1, String str2) {
    Set<String> set1 = new HashSet<>(Arrays.asList(str1.split(",")));
    Set<String> set2 = new HashSet<>(Arrays.asList(str2.split(",")));
    assertThat(set1, is(set2));
  }

  private void init() {
    config.put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0");
    KsqlRestConfig serverConfig = new KsqlRestConfig(config);
    createServer(serverConfig);
    this.client = createClient();
  }

  private HttpResponse<Buffer> sendCorsRequest(final HttpMethod httpMethod, final MultiMap headers)
      throws Exception {
    return sendCorsRequest(httpMethod, URI, headers);
  }

  private HttpResponse<Buffer> sendCorsRequest(final HttpMethod httpMethod, final String uri,
      final MultiMap headers)
      throws Exception {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .request(httpMethod, uri)
        .putHeaders(headers)
        .sendJsonObject(new JsonObject().put("sql", DEFAULT_PULL_QUERY), requestFuture);
    return requestFuture.get();
  }

  private HttpResponse<Buffer> sendCorsRequest(final HttpMethod httpMethod,
      final String origin)
      throws Exception {
    return sendCorsRequest(httpMethod, URI, origin);
  }

  private HttpResponse<Buffer> sendCorsRequest(final HttpMethod httpMethod,
      final String uri,
      final String origin)
      throws Exception {
    return sendCorsRequest(httpMethod, uri,
        MultiMap.caseInsensitiveMultiMap().add("origin", origin));
  }
  
}
