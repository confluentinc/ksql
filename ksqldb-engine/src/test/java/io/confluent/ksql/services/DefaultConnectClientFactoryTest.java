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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Optional;
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

  @Rule
  public TemporaryFolder folder = KsqlTestFolder.temporaryFolder();

  @Mock
  private KsqlConfig config;

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
    final DefaultConnectClient connectClient = connectClientFactory.get(Optional.empty());

    // Then:
    assertThat(connectClient.getAuthHeader(), is(Optional.empty()));
  }

  @Test
  public void shouldBuildAuthHeader() throws Exception {
    // Given:
    givenCustomBasicAuthHeader();
    givenValidCredentialsFile();

    // When:
    final DefaultConnectClient connectClient = connectClientFactory.get(Optional.empty());

    // Then:
    assertThat(connectClient.getAuthHeader(), is(Optional.of(EXPECTED_HEADER)));
  }

  @Test
  public void shouldBuildAuthHeaderOnlyOnce() throws Exception {
    // Given:
    givenCustomBasicAuthHeader();
    givenValidCredentialsFile();

    // When: get() is called twice
    connectClientFactory.get(Optional.empty());
    connectClientFactory.get(Optional.empty());

    // Then: only loaded the credentials once -- ideally we'd check the number of times the file
    //       was read but this is an acceptable proxy for this unit test
    verify(config, times(1)).getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY);
  }

  @Test
  public void shouldUseKsqlAuthHeaderIfNoAuthHeaderPresent() {
    // When:
    final DefaultConnectClient connectClient =
        connectClientFactory.get(Optional.of("some ksql request header"));

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
        () -> connectClientFactory.get(Optional.empty()));

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
    final DefaultConnectClient connectClient = connectClientFactory.get(Optional.empty());

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
        () -> connectClientFactory.get(Optional.empty()));

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
    final DefaultConnectClient connectClient = connectClientFactory.get(Optional.empty());

    // Then:
    assertThat(connectClient.getAuthHeader(), is(Optional.empty()));
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
    final String content = String.format("username=%s\npassword=%s", USERNAME, PASSWORD);
    createCredentialsFile(content);
  }

  private void givenInvalidCredentialsFiles() throws Exception {
    createCredentialsFile("malformed credentials content");
  }

  private void createCredentialsFile(final String content) throws IOException {
    assertThat(new File(credentialsPath).createNewFile(), is(true));
    PrintWriter out = new PrintWriter(credentialsPath, Charset.defaultCharset().name());
    out.println(content);
    out.close();
  }
  
  private static String expectedBasicAuthHeader(final String username, final String password) {
    final String userInfo = username + ":" + password;
    return "Basic " + Base64.getEncoder()
        .encodeToString(userInfo.getBytes(Charset.defaultCharset()));
  }
}