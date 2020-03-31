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

import io.confluent.ksql.api.server.ApiServerConfig;
import io.confluent.ksql.api.server.KSqlCorsHandler;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CorsTest extends BaseApiTest {

  private Map<String, Object> config = new HashMap<>();

  private static final String ACCESS_CONTROL_ALLOW_ORIGIN_HEADER = "Access-Control-Allow-Origin";
  private static final String ACCESS_CONTROL_ALLOW_METHODS_HEADER = "Access-Control-Allow-Methods";
  private static final String ACCESS_CONTROL_ALLOW_HEADERS_HEADER = "Access-Control-Allow-Headers";
  private static final String ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER = "Access-Control-Allow-Credentials";

  private static final String ACCESS_CONTROL_REQUEST_METHOD_HEADER = "Access-Control-Request-Method";

  private static final String DEFAULT_ACCESS_CONTROL_ALLOW_HEADERS = "X-Requested-With,Content-Type,Accept,Origin";
  private static final String DEFAULT_ACCESS_CONTROL_ALLOW_METHODS = "GET,POST,HEAD";

  @Before
  public void setUp() {

    vertx = Vertx.vertx();
    vertx.exceptionHandler(t -> log.error("Unhandled exception in Vert.x", t));

    testEndpoints = new TestEndpoints();
    setDefaultRowGenerator();
  }

  @After
  public void tearDown() {
    stopClient();
    stopServer();
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test
  public void shouldNotBeCorsResponseIfNoCorsConfigured() throws Exception {

    // Given:
    init();

    // When
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.POST, "wibble.com");

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
    config.put(ApiServerConfig.CORS_ALLOWED_ORIGINS, "wibble.com");
    KSqlCorsHandler.EXCLUDED_PATH_PREFIXES.add("/query-stream");

    // When
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.POST, "wibble.com");

    // Then:
    assertThat(response.statusCode(), is(200));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER), is(nullValue()));

    KSqlCorsHandler.EXCLUDED_PATH_PREFIXES
        .remove(KSqlCorsHandler.EXCLUDED_PATH_PREFIXES.size() - 1);
  }

  @Test
  public void shouldNotBeCorsResponseIfNotCorsRequest() throws Exception {

    // Given:
    init();
    config.put(ApiServerConfig.CORS_ALLOWED_ORIGINS, "wibble.com");

    // When
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .request(HttpMethod.POST, "/query-stream")
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
    shouldAcceptCorsRequest("wibble.com", "wibble.com");
  }

  @Test
  public void shouldRejectCorsRequestOriginExactMatch() throws Exception {
    shouldRejectCorsRequest("foo.com", "wibble.com");
  }

  @Test
  public void shouldAcceptCorsRequestOriginExactMatchOneOfList() throws Exception {
    shouldAcceptCorsRequest("wibble.com", "foo.com,wibble.com");
  }

  @Test
  public void shouldRejectCorsRequestOriginExactMatchOenOfList() throws Exception {
    shouldRejectCorsRequest("foo.com", "wibble.com,blah.com");
  }

  @Test
  public void shouldAcceptCorsRequestAcceptAll() throws Exception {
    shouldAcceptCorsRequest("wibble.com", "*");
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
    shouldAcceptPreflightRequest("wibble.com", "wibble.com", DEFAULT_ACCESS_CONTROL_ALLOW_HEADERS,
        DEFAULT_ACCESS_CONTROL_ALLOW_METHODS);
  }

  @Test
  public void shouldRejectPreflightRequestOriginExactMatchDefaultHeaders() throws Exception {
    shouldRejectPreflightRequest("wibble.com", "flibble.com");
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedHeaders() throws Exception {
    config.put(ApiServerConfig.CORS_ALLOWED_HEADERS, "foo-header,bar-header");
    shouldAcceptPreflightRequest("wibble.com", "wibble.com",
        "foo-header,bar-header,X-Requested-With,Content-Type,Accept,Origin",
        DEFAULT_ACCESS_CONTROL_ALLOW_METHODS);
  }

  @Test
  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedMethods() throws Exception {
    config.put(ApiServerConfig.CORS_ALLOWED_METHODS, "DELETE,PATCH");
    shouldAcceptPreflightRequest("wibble.com", "wibble.com",
        DEFAULT_ACCESS_CONTROL_ALLOW_HEADERS,
        "DELETE,PATCH,GET,POST,HEAD");
  }

  private void shouldAcceptCorsRequest(final String origin, final String allowedOrigins)
      throws Exception {

    // Given:
    config.put(ApiServerConfig.CORS_ALLOWED_ORIGINS, allowedOrigins);
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
    config.put(ApiServerConfig.CORS_ALLOWED_ORIGINS, allowedOrigins);
    init();

    // When
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.POST, origin);

    // Then:
    assertThat(response.statusCode(), is(403));
  }

  private void shouldRejectPreflightRequest(final String origin, final String allowedOrigins)
      throws Exception {

    // Given:
    config.put(ApiServerConfig.CORS_ALLOWED_ORIGINS, allowedOrigins);
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

    config.put(ApiServerConfig.CORS_ALLOWED_ORIGINS, allowedOrigins);
    init();

    // Given
    HttpResponse<Buffer> response = sendCorsRequest(HttpMethod.OPTIONS,
        MultiMap.caseInsensitiveMultiMap().add("origin", origin)
            .add(ACCESS_CONTROL_REQUEST_METHOD_HEADER, "POST"));

    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER),
        is(origin));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER), is("true"));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER),
        is(accessControlAllowHeaders));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER),
        is(accessControlAllowMethods));
  }

  private void init() {
    ApiServerConfig serverConfig = new ApiServerConfig(config);
    createServer(serverConfig);
    this.client = createClient();
  }

  private HttpResponse<Buffer> sendCorsRequest(final HttpMethod httpMethod, final MultiMap headers)
      throws Exception {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .request(httpMethod, "/query-stream")
        .putHeaders(headers)
        .sendJsonObject(new JsonObject().put("sql", DEFAULT_PULL_QUERY), requestFuture);
    return requestFuture.get();
  }

  private HttpResponse<Buffer> sendCorsRequest(final HttpMethod httpMethod,
      final String origin)
      throws Exception {
    return sendCorsRequest(httpMethod, MultiMap.caseInsensitiveMultiMap().add("origin", origin));
  }
}
