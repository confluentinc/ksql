/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.services;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.CONNECT_REQUEST_TIMEOUT_DEFAULT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.connect.ConnectRequestHeadersExtension;
import io.confluent.ksql.security.KsqlPrincipal;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import io.vertx.core.http.HttpHeaders;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.attribute.FileTime;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.http.Header;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultConnectClientFactoryTest {

  private static final String USERNAME = "fred";
  private static final String PASSWORD = "pass";
  private static final String EXPECTED_HEADER = expectedBasicAuthHeader(USERNAME, PASSWORD);

  private static final String OTHER_USERNAME = "joey";
  private static final String OTHER_PASSWORD = "jpass";
  private static final String OTHER_EXPECTED_HEADER =
      expectedBasicAuthHeader(OTHER_USERNAME, OTHER_PASSWORD);
  
  private static final String AUTH_HEADER_NAME = HttpHeaders.AUTHORIZATION.toString();
  private static final Header[] EMPTY_HEADERS = new Header[]{};

  private static final Map<String, Object> DEFAULT_CONFIGS_WITH_PREFIX_OVERRIDE =
      new KsqlConfig(ImmutableMap.of()).valuesWithPrefixOverride(KsqlConfig.KSQL_CONNECT_PREFIX);
  private static final Map<String, Object> CONFIGS_WITH_HOSTNAME_VERIFICATION_ENABLED =
      new KsqlConfig(ImmutableMap.of(
          KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
          KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_HTTPS))
          .valuesWithPrefixOverride(KsqlConfig.KSQL_CONNECT_PREFIX);
  private static final Map<String, Object> CONFIGS_WITH_HOSTNAME_VERIFICATION_DISABLED =
      new KsqlConfig(ImmutableMap.of(
          KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
          KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_NONE))
          .valuesWithPrefixOverride(KsqlConfig.KSQL_CONNECT_PREFIX);
  private static final Map<String, Object> CONFIGS_WITH_HOSTNAME_VERIFICATION_EMPTY =
      new KsqlConfig(ImmutableMap.of(
          KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
          ""))
          .valuesWithPrefixOverride(KsqlConfig.KSQL_CONNECT_PREFIX);

  @Rule
  public TemporaryFolder folder = KsqlTestFolder.temporaryFolder();

  @Mock
  private KsqlConfig config;
  @Mock
  private ConnectRequestHeadersExtension requestHeadersExtension;
  @Mock
  private List<Entry<String, String>> incomingRequestHeaders;
  @Mock
  private KsqlPrincipal userPrincipal;

  private String credentialsPath;

  private DefaultConnectClientFactory connectClientFactory;

  @Before
  public void setUp() {
    credentialsPath = folder.getRoot().getPath() + "/connect_credentials.properties";

    when(config.getString(KsqlConfig.CONNECT_URL_PROPERTY)).thenReturn("http://localhost:8034");
    when(config.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY)).thenReturn("NONE");
    when(config.valuesWithPrefixOverride(KsqlConfig.KSQL_CONNECT_PREFIX)).thenReturn(DEFAULT_CONFIGS_WITH_PREFIX_OVERRIDE);
    when(config.getLong(KsqlConfig.CONNECT_REQUEST_TIMEOUT_MS)).thenReturn(CONNECT_REQUEST_TIMEOUT_DEFAULT);

    connectClientFactory = new DefaultConnectClientFactory(config);
  }

  @Test
  public void shouldBuildWithoutAuthHeader() {
    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty());

    // Then:
    assertThat(connectClient.getRequestHeaders(), is(EMPTY_HEADERS));
  }

  @Test
  public void shouldBuildAuthHeader() throws Exception {
    // Given:
    givenCustomBasicAuthHeader();
    givenValidCredentialsFile();

    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty());

    // Then:
    assertThat(connectClient.getRequestHeaders(),
        arrayContaining(header(AUTH_HEADER_NAME, EXPECTED_HEADER)));
  }

  @Test
  public void shouldBuildAuthHeaderOnlyOnce() throws Exception {
    // Given:
    givenCustomBasicAuthHeader();
    givenValidCredentialsFile();

    // When: get() is called twice
    connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty());
    connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty());

    // Then: only loaded the credentials once -- ideally we'd check the number of times the file
    //       was read but this is an acceptable proxy for this unit test
    verify(config, times(1)).getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY);
  }

  @Test
  public void shouldUseKsqlAuthHeaderIfNoAuthHeaderPresent() {
    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.of("some ksql request header"), Collections.emptyList(), Optional.empty());

    // Then:
    assertThat(connectClient.getRequestHeaders(),
        arrayContaining(header(AUTH_HEADER_NAME, "some ksql request header")));
  }

  @Test
  public void shouldNotFailOnUnreadableCredentials() throws Exception {
    // Given:
    givenCustomBasicAuthHeader();
    givenInvalidCredentialsFiles();

    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty());

    // Then:
    assertThat(connectClient.getRequestHeaders(), is(EMPTY_HEADERS));
  }

  @Test
  public void shouldNotFailOnMissingCredentials() {
    // Given:
    givenCustomBasicAuthHeader();
    // no credentials file present

    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty());

    // Then:
    assertThat(connectClient.getRequestHeaders(), is(EMPTY_HEADERS));
  }

  @Test
  public void shouldReloadCredentialsOnFileCreation() throws Exception {
    // Given:
    when(config.getBoolean(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_RELOAD_PROPERTY)).thenReturn(true);
    givenCustomBasicAuthHeader();
    // no credentials file present

    // verify that no auth header is present
    assertThat(connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty()).getRequestHeaders(),
        is(EMPTY_HEADERS));

    // When: credentials file is created
    waitForLastModifiedTick();
    givenValidCredentialsFile();

    // Then: auth header is present
    assertThatEventually(
        "Should load newly created credentials",
        () -> connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty()).getRequestHeaders(),
        arrayContaining(header(AUTH_HEADER_NAME, EXPECTED_HEADER)),
        TimeUnit.SECONDS.toMillis(1),
        TimeUnit.SECONDS.toMillis(1)
    );
  }

  @Test
  public void shouldReloadCredentialsOnFileChange() throws Exception {
    // Given:
    when(config.getBoolean(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_RELOAD_PROPERTY)).thenReturn(true);
    givenCustomBasicAuthHeader();
    givenValidCredentialsFile();

    // verify auth header is present
    assertThat(connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty()).getRequestHeaders(),
        arrayContaining(header(AUTH_HEADER_NAME, EXPECTED_HEADER)));

    // When: credentials file is modified
    waitForLastModifiedTick();
    writeNewCredentialsFile();

    // Then: new auth header is present
    assertThatEventually(
        "Should load updated credentials",
        () -> connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.empty()).getRequestHeaders(),
        arrayContaining(header(AUTH_HEADER_NAME, OTHER_EXPECTED_HEADER)),
        TimeUnit.SECONDS.toMillis(1),
        TimeUnit.SECONDS.toMillis(1)
    );
  }

  @Test
  public void shouldPassCustomRequestHeaders() {
    // Given:
    when(config.getConfiguredInstance(
            KsqlConfig.CONNECT_REQUEST_HEADERS_PLUGIN,
            ConnectRequestHeadersExtension.class))
        .thenReturn(requestHeadersExtension);
    // re-initialize client factory since request headers extension is configured in constructor
    connectClientFactory = new DefaultConnectClientFactory(config);

    when(requestHeadersExtension.getHeaders(Optional.of(userPrincipal)))
        .thenReturn(ImmutableMap.of("header", "value"));

    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.of(userPrincipal));

    // Then:
    assertThat(connectClient.getRequestHeaders(), arrayContaining(header("header", "value")));
  }

  @Test
  public void shouldPassCustomRequestHeadersInAdditionToDefaultBasicAuthHeader() throws Exception {
    // Given:
    givenCustomBasicAuthHeader();
    givenValidCredentialsFile();

    when(config.getConfiguredInstance(
        KsqlConfig.CONNECT_REQUEST_HEADERS_PLUGIN,
        ConnectRequestHeadersExtension.class))
        .thenReturn(requestHeadersExtension);
    // re-initialize client factory since request headers extension is configured in constructor
    connectClientFactory = new DefaultConnectClientFactory(config);

    when(requestHeadersExtension.getHeaders(Optional.of(userPrincipal)))
        .thenReturn(ImmutableMap.of("header", "value"));
    when(requestHeadersExtension.shouldUseCustomAuthHeader()).thenReturn(false);

    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.empty(), Collections.emptyList(), Optional.of(userPrincipal));

    // Then:
    assertThat(connectClient.getRequestHeaders(),
        arrayContaining(header(AUTH_HEADER_NAME, EXPECTED_HEADER), header("header", "value")));
    // should never be called since shouldUseCustomAuthHeader() is false
    verify(requestHeadersExtension, never()).getAuthHeader(any());
  }

  @Test
  public void shouldFavorCustomAuthHeaderOverBasicAuthHeader() throws Exception {
    // Given:
    givenCustomBasicAuthHeader();
    givenValidCredentialsFile();

    when(config.getConfiguredInstance(
        KsqlConfig.CONNECT_REQUEST_HEADERS_PLUGIN,
        ConnectRequestHeadersExtension.class))
        .thenReturn(requestHeadersExtension);
    // re-initialize client factory since request headers extension is configured in constructor
    connectClientFactory = new DefaultConnectClientFactory(config);

    when(requestHeadersExtension.getHeaders(Optional.of(userPrincipal)))
        .thenReturn(ImmutableMap.of("header", "value"));
    when(requestHeadersExtension.shouldUseCustomAuthHeader()).thenReturn(true);
    when(requestHeadersExtension.getAuthHeader(incomingRequestHeaders)).thenReturn(Optional.of("some custom auth"));

    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.empty(), incomingRequestHeaders, Optional.of(userPrincipal));

    // Then:
    assertThat(connectClient.getRequestHeaders(),
        arrayContaining(header(AUTH_HEADER_NAME, "some custom auth"), header("header", "value")));
  }

  @Test
  public void shouldEnableHostnameVerification() {
    // When / Then:
    assertThat(DefaultConnectClientFactory.shouldVerifySslHostname(CONFIGS_WITH_HOSTNAME_VERIFICATION_ENABLED),
        is(true));
  }

  @Test
  public void shouldDisableHostnameVerification() {
    // When / Then:
    assertThat(DefaultConnectClientFactory.shouldVerifySslHostname(CONFIGS_WITH_HOSTNAME_VERIFICATION_DISABLED),
        is(false));
  }

  @Test
  public void shouldDisableHostnameVerificationOnEmptyConfig() {
    // When / Then:
    assertThat(DefaultConnectClientFactory.shouldVerifySslHostname(CONFIGS_WITH_HOSTNAME_VERIFICATION_EMPTY),
        is(false));
  }

  private void givenCustomBasicAuthHeader() {
    when(config.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY)).thenReturn("FILE");
    when(config.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY)).thenReturn(credentialsPath);
  }

  private void givenValidCredentialsFile() throws Exception {
    final String content = String.format("username=%s%npassword=%s", USERNAME, PASSWORD);
    createCredentialsFile(content, true);
  }

  private void givenInvalidCredentialsFiles() throws Exception {
    createCredentialsFile("malformed credentials content", true);
  }

  private void writeNewCredentialsFile() throws Exception {
    final String content = String.format("username=%s%npassword=%s", OTHER_USERNAME, OTHER_PASSWORD);
    createCredentialsFile(content, false);
  }

  private void createCredentialsFile(final String content, final boolean newFile) throws IOException {
    if (newFile) {
      assertThat(new File(credentialsPath).createNewFile(), is(true));
    }
    PrintWriter out = new PrintWriter(credentialsPath, Charset.defaultCharset().name());
    out.println(content);
    out.close();
  }
  
  private static String expectedBasicAuthHeader(final String username, final String password) {
    final String userInfo = username + ":" + password;
    return "Basic " + Base64.getEncoder()
        .encodeToString(userInfo.getBytes(Charset.defaultCharset()));
  }

  private static Matcher<? super Header> header(
      final String name,
      final String value
  ) {
    return new TypeSafeDiagnosingMatcher<Header>() {
      @Override
      protected boolean matchesSafely(
          final Header actual,
          final Description mismatchDescription) {
        if (!name.equals(actual.getName())) {
          return false;
        }
        if (!value.equals(actual.getValue())) {
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(String.format("name: %s. value: %s.", name, value));
      }
    };
  }

  /**
   * Resolution of {@link FileTime} on some OS / JDKs can have only second resolution.
   * This can mean the watcher 'misses' an update to a file that was <i>created</i> before
   * the watcher was started and <i>updated</i> after, if the update results in the same
   * last modified time.
   *
   * <p>To ensure we stable test we must therefore wait for a second to ensure a different last
   * modified time.
   *
   * https://stackoverflow.com/questions/24804618/get-file-mtime-with-millisecond-resolution-from-java
   */
  private static void waitForLastModifiedTick() throws Exception {
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
  }
}