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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.connect.ConnectRequestHeadersExtension;
import io.confluent.ksql.security.KsqlPrincipal;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.attribute.FileTime;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigException;
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

  @Rule
  public TemporaryFolder folder = KsqlTestFolder.temporaryFolder();

  @Mock
  private KsqlConfig config;
  @Mock
  private ConnectRequestHeadersExtension requestHeadersExtension;
  @Mock
  private KsqlPrincipal userPrincipal;

  private String credentialsPath;

  private DefaultConnectClientFactory connectClientFactory;

  @Before
  public void setUp() {
    credentialsPath = folder.getRoot().getPath() + "/connect_credentials.properties";

    when(config.getString(KsqlConfig.CONNECT_URL_PROPERTY)).thenReturn("http://localhost:8034");
    when(config.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY)).thenReturn("NONE");

    connectClientFactory = new DefaultConnectClientFactory(config);
  }

  @Test
  public void shouldBuildWithoutAuthHeader() {
    // When:
    final DefaultConnectClient connectClient = connectClientFactory.get(Optional.empty(), Optional.empty());

    // Then:
    assertThat(connectClient.getAuthHeader(), is(Optional.empty()));
  }

  @Test
  public void shouldBuildAuthHeader() throws Exception {
    // Given:
    givenCustomBasicAuthHeader();
    givenValidCredentialsFile();

    // When:
    final DefaultConnectClient connectClient = connectClientFactory.get(Optional.empty(), Optional.empty());

    // Then:
    assertThat(connectClient.getAuthHeader(), is(Optional.of(EXPECTED_HEADER)));
  }

  @Test
  public void shouldBuildAuthHeaderOnlyOnce() throws Exception {
    // Given:
    givenCustomBasicAuthHeader();
    givenValidCredentialsFile();

    // When: get() is called twice
    connectClientFactory.get(Optional.empty(), Optional.empty());
    connectClientFactory.get(Optional.empty(), Optional.empty());

    // Then: only loaded the credentials once -- ideally we'd check the number of times the file
    //       was read but this is an acceptable proxy for this unit test
    verify(config, times(1)).getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY);
  }

  @Test
  public void shouldUseKsqlAuthHeaderIfNoAuthHeaderPresent() {
    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.of("some ksql request header"), Optional.empty());

    // Then:
    assertThat(connectClient.getAuthHeader(), is(Optional.of("some ksql request header")));
  }

  @Test
  public void shouldFailOnUnreadableCredentials() throws Exception {
    // Given:
    givenCustomBasicAuthHeader(true);
    givenInvalidCredentialsFiles();

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> connectClientFactory.get(Optional.empty(), Optional.empty()));

    // Then:
    assertThat(e.getMessage(),
        containsString("Provided credentials file doesn't provide username and password"));
  }

  @Test
  public void shouldNotFailOnUnreadableCredentialsIfConfigured() throws Exception {
    // Given:
    givenCustomBasicAuthHeader(false);
    givenInvalidCredentialsFiles();

    // When:
    final DefaultConnectClient connectClient = connectClientFactory.get(Optional.empty(), Optional.empty());

    // Then:
    assertThat(connectClient.getAuthHeader(), is(Optional.empty()));
  }

  @Test
  public void shouldFailOnMissingCredentials() {
    // Given:
    givenCustomBasicAuthHeader(true);
    // no credentials file present

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> connectClientFactory.get(Optional.empty(), Optional.empty()));

    // Then:
    assertThat(e.getMessage(),
        containsString("No such file or directory"));
  }

  @Test
  public void shouldNotFailOnMissingCredentialsIfConfigured() {
    // Given:
    givenCustomBasicAuthHeader(false);
    // no credentials file present

    // When:
    final DefaultConnectClient connectClient = connectClientFactory.get(Optional.empty(), Optional.empty());

    // Then:
    assertThat(connectClient.getAuthHeader(), is(Optional.empty()));
  }

  @Test
  public void shouldReloadCredentialsOnFileCreation() throws Exception {
    // Given:
    when(config.getBoolean(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_RELOAD_PROPERTY)).thenReturn(true);
    givenCustomBasicAuthHeader(false);
    // no credentials file present

    // verify that no auth header is present
    assertThat(connectClientFactory.get(Optional.empty(), Optional.empty()).getAuthHeader(),
        is(Optional.empty()));

    // When: credentials file is created
    waitForLastModifiedTick();
    givenValidCredentialsFile();

    // Then: auth header is present
    assertThatEventually(
        "Should load newly created credentials",
        () -> connectClientFactory.get(Optional.empty(), Optional.empty()).getAuthHeader(),
        is(Optional.of(EXPECTED_HEADER)),
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
    assertThat(connectClientFactory.get(Optional.empty(), Optional.empty()).getAuthHeader(),
        is(Optional.of(EXPECTED_HEADER)));

    // When: credentials file is modified
    waitForLastModifiedTick();
    writeNewCredentialsFile();

    // Then: new auth header is present
    assertThatEventually(
        "Should load updated credentials",
        () -> connectClientFactory.get(Optional.empty(), Optional.empty()).getAuthHeader(),
        is(Optional.of(OTHER_EXPECTED_HEADER)),
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
    final DefaultConnectClient connectClient = connectClientFactory.get(Optional.empty(), Optional.of(userPrincipal));

    // Then:
    assertThat(connectClient.getAdditionalRequestHeaders(), is(ImmutableMap.of("header", "value")));
  }

  private void givenCustomBasicAuthHeader() {
    givenCustomBasicAuthHeader(true);
  }

  private void givenCustomBasicAuthHeader(final boolean failOnUnreadableCreds) {
    when(config.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY)).thenReturn("FILE");
    when(config.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY)).thenReturn(credentialsPath);
    when(config.getBoolean(KsqlConfig.CONNECT_BASIC_AUTH_FAIL_ON_UNREADABLE_CREDENTIALS)).thenReturn(failOnUnreadableCreds);
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