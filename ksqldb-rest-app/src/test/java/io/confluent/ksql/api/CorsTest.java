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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

public final class CorsTest {
  private static final String ACCESS_CONTROL_ALLOW_ORIGIN_HEADER = "Access-Control-Allow-Origin";
  private static final String ACCESS_CONTROL_ALLOW_METHODS_HEADER = "Access-Control-Allow-Methods";
  private static final String ACCESS_CONTROL_ALLOW_HEADERS_HEADER = "Access-Control-Allow-Headers";
  private static final String ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER = "Access-Control-Allow-Credentials";

  private static final String ACCESS_CONTROL_REQUEST_METHOD_HEADER = "Access-Control-Request-Method";

  private static final String DEFAULT_ACCESS_CONTROL_ALLOW_HEADERS = "X-Requested-With,Content-Type,Accept,Origin";
  private static final String DEFAULT_ACCESS_CONTROL_ALLOW_METHODS = "GET,POST,HEAD";

  protected static final String DEFAULT_PULL_QUERY = "select * from foo where rowkey='1234';";

  private static final String URI = "/query-stream";
  private static final String ORIGIN = "http://wibble.com";

  private final Function<Map<String, String>, WebClient> clientSupplier;
  private final int successStatus;

  public CorsTest(final Function<Map<String, String>, WebClient> clientSupplier) {
    this(clientSupplier, HttpStatus.OK_200);
  }

  public CorsTest(
      final Function<Map<String, String>, WebClient> clientSupplier,
      final int successStatus
  ) {
    this.clientSupplier = Objects.requireNonNull(clientSupplier, "clientSupplier");
    this.successStatus = successStatus;
  }

  public void shouldNotBeCorsResponseIfNoCorsConfigured() throws Exception {

    // Given:
    final WebClient client = clientSupplier.apply(Collections.emptyMap());

    // When
    HttpResponse<Buffer> response = sendCorsRequest(client, HttpMethod.POST, ORIGIN);

    // Then:
    assertThat(response.statusCode(), is(successStatus));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER), is(nullValue()));
  }

  public void shouldExcludePath(final int expectedCode) throws Exception {

    // Given:
    final WebClient client = clientSupplier.apply(
        ImmutableMap.of(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, ORIGIN));

    // When
    HttpResponse<Buffer> response = sendCorsRequest(client, HttpMethod.POST, "/ws/foo", ORIGIN);

    // Then:
    // We should get a 404 as the security checks will be bypassed but the resource doesn't exist
    assertThat(response.statusCode(), is(expectedCode));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER), is(nullValue()));
  }

  public void shouldNotBeCorsResponseIfNotCorsRequest() throws Exception {

    // Given:
    final WebClient client = clientSupplier.apply(
        ImmutableMap.of(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, ORIGIN));

    // When
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client.request(HttpMethod.POST, URI)
        .sendJsonObject(new JsonObject().put("sql", DEFAULT_PULL_QUERY), requestFuture);
    HttpResponse<Buffer> response = requestFuture.get();

    // Then:
    assertThat(response.statusCode(), is(successStatus));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_METHODS_HEADER), is(nullValue()));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_HEADERS_HEADER), is(nullValue()));
  }

  public void shouldAcceptCorsRequestOriginExactMatch() throws Exception {
    shouldAcceptCorsRequest(ORIGIN, ORIGIN);
  }

  public void shouldRejectCorsRequestOriginExactMatch() throws Exception {
    shouldRejectCorsRequest("foo.com", ORIGIN);
  }

  public void shouldAcceptCorsRequestOriginExactMatchOneOfList() throws Exception {
    shouldAcceptCorsRequest(ORIGIN, "foo.com," + ORIGIN);
  }

  public void shouldRejectCorsRequestOriginExactMatchOenOfList() throws Exception {
    shouldRejectCorsRequest("foo.com", "wibble.com,blah.com");
  }

  public void shouldAcceptCorsRequestAcceptAll() throws Exception {
    shouldAcceptCorsRequest(ORIGIN, "*");
  }

  public void shouldAcceptCorsRequestWildcardMatch() throws Exception {
    shouldAcceptCorsRequest("foo.wibble.com", "*.wibble.com");
  }

  public void shouldRejectCorsRequestWildcardMatch() throws Exception {
    shouldRejectCorsRequest("foo.wibble.com", "*.blibble.com");
  }

  public void shouldAcceptCorsRequestWilcardMatchList() throws Exception {
    shouldAcceptCorsRequest("foo.wibble.com", "blah.com,*.wibble.com,foo.com");
  }

  public void shouldRejectCorsRequestWilcardMatchList() throws Exception {
    shouldRejectCorsRequest("foo.wibble.com", "blah.com,*.blibble.com,foo.com");
  }

  public void shouldAcceptPreflightRequestOriginExactMatchDefaultHeaders() throws Exception {
    shouldAcceptPreflightRequest(ORIGIN, ORIGIN, DEFAULT_ACCESS_CONTROL_ALLOW_HEADERS,
        DEFAULT_ACCESS_CONTROL_ALLOW_METHODS);
  }

  public void shouldRejectPreflightRequestOriginExactMatchDefaultHeaders() throws Exception {
    shouldRejectPreflightRequest(ORIGIN, "flibble.com");
  }

  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedHeaders() throws Exception {
    shouldAcceptPreflightRequest(ORIGIN, ORIGIN,
        "foo-header,bar-header",
        DEFAULT_ACCESS_CONTROL_ALLOW_METHODS);
  }

  public void shouldAcceptPreflightRequestOriginExactMatchSpecifiedMethods() throws Exception {
    shouldAcceptPreflightRequest(ORIGIN, ORIGIN,
        DEFAULT_ACCESS_CONTROL_ALLOW_HEADERS,
        "DELETE,PATCH");
  }

  public void shouldCallAllCorsTests(Class<?> testClass) {
    final List<Method> testMethods = Arrays.stream(CorsTest.class.getDeclaredMethods())
        .filter(m -> Modifier.isPublic(m.getModifiers()))
        .filter(m -> m.getName().startsWith("should"))
        .collect(Collectors.toList());
    for (final Method testMethod : testMethods) {
      try {
        final Method method = testClass.getDeclaredMethod(testMethod.getName());
        assertThat("Test method " + method.getName() + " does not have test annotation",
            method.getAnnotation(Test.class),
            not(nullValue())
        );
      } catch (final NoSuchMethodException e) {
        Assert.fail("Could not find a cors test method with name " + testMethod.getName());
      }
    }
  }

  private void shouldAcceptCorsRequest(final String origin, final String allowedOrigins)
      throws Exception {

    // Given:
    final WebClient client = clientSupplier.apply(
        ImmutableMap.of(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, allowedOrigins));

    // When
    HttpResponse<Buffer> response = sendCorsRequest(client, HttpMethod.POST, origin);

    // Then:
    assertThat(response.statusCode(), is(successStatus));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_ORIGIN_HEADER),
        is(origin));
    assertThat(response.getHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS_HEADER), is("true"));
  }

  private void shouldRejectCorsRequest(final String origin, final String allowedOrigins)
      throws Exception {

    // Given:
    final WebClient client = clientSupplier.apply(
        ImmutableMap.of(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, allowedOrigins));

    // When
    HttpResponse<Buffer> response = sendCorsRequest(client, HttpMethod.POST, origin);

    // Then:
    assertThat(response.statusCode(), is(403));
  }

  private void shouldRejectPreflightRequest(final String origin, final String allowedOrigins)
      throws Exception {

    // Given:
    final WebClient client = clientSupplier.apply(
        ImmutableMap.of(KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, allowedOrigins));

    // When:
    HttpResponse<Buffer> response = sendCorsRequest(client, HttpMethod.OPTIONS,
        MultiMap.caseInsensitiveMultiMap().add("origin", origin)
            .add(ACCESS_CONTROL_REQUEST_METHOD_HEADER, "POST"));

    // Then:
    assertThat(response.statusCode(), is(403));
  }

  private void shouldAcceptPreflightRequest(final String origin, final String allowedOrigins,
      final String accessControlAllowHeaders, final String accessControlAllowMethods)
      throws Exception {

    final WebClient client = clientSupplier.apply(
        ImmutableMap.of(
            KsqlRestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG, allowedOrigins,
            KsqlRestConfig.ACCESS_CONTROL_ALLOW_HEADERS, accessControlAllowHeaders,
            KsqlRestConfig.ACCESS_CONTROL_ALLOW_METHODS, accessControlAllowMethods
        )
    );

    // Given
    HttpResponse<Buffer> response = sendCorsRequest(client, HttpMethod.OPTIONS,
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

  private HttpResponse<Buffer> sendCorsRequest(
      final WebClient client,
      final HttpMethod httpMethod,
      final MultiMap headers
  ) throws Exception {
    return sendCorsRequest(client, httpMethod, URI, headers);
  }

  private HttpResponse<Buffer> sendCorsRequest(
      final WebClient client,
      final HttpMethod httpMethod,
      final String uri,
      final MultiMap headers)
      throws Exception {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .request(httpMethod, uri)
        .putHeaders(headers)
       .sendJsonObject(new JsonObject().put("sql", DEFAULT_PULL_QUERY), requestFuture);
    return requestFuture.get();
  }

  private HttpResponse<Buffer> sendCorsRequest(
      final WebClient client,
      final HttpMethod httpMethod,
      final String origin)
      throws Exception {
    return sendCorsRequest(client, httpMethod, URI, origin);
  }

  private HttpResponse<Buffer> sendCorsRequest(
      final WebClient client,
      final HttpMethod httpMethod,
      final String uri,
      final String origin)
      throws Exception {
    return sendCorsRequest(client, httpMethod, uri,
        MultiMap.caseInsensitiveMultiMap().add("origin", origin));
  }
  
}
