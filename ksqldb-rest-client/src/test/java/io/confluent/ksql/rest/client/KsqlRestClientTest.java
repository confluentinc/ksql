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

import static io.confluent.ksql.rest.client.KsqlRestClient.CCLOUD_CONNECT_PASSWORD_HEADER;
import static io.confluent.ksql.rest.client.KsqlRestClient.CCLOUD_CONNECT_USERNAME_HEADER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.client.KsqlRestClient.KsqlClientSupplier;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestClientTest {

  private static final String SOME_SERVER_ADDRESS = "http://singleServer:8088";
  private static final Map<String, ?> LOCAL_PROPS = ImmutableMap.of("auto.offset.reset", "earliest");
  private static final Map<String, String> CLIENT_PROPS = ImmutableMap.of("foo", "bar");

  private static final String CCLOUD_API_KEY = "api_key";
  private static final String CCLOUD_API_SECRET = "api_secret";

  @Mock
  private KsqlClient client;
  @Mock
  private KsqlTarget target;
  @Mock
  private KsqlClientSupplier clientSupplier;

  @Before
  public void setUp() {
    when(clientSupplier.get(any(), any(), any())).thenReturn(client);
    when(client.target(any(), any())).thenReturn(target);
  }

  @Test
  public void shouldThrowOnInvalidServerAddress() {
    // When:
    final Exception e = assertThrows(
        KsqlRestClientException.class,
        () -> clientWithServerAddresses("timbuktu")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The supplied serverAddress is invalid: timbuktu"));
  }

  @Test
  public void shouldParseSingleServerAddress() throws Exception {
    // Given:
    final String singleServerAddress = SOME_SERVER_ADDRESS;
    final URI singleServerURI = new URI(singleServerAddress);

    // When:
    KsqlRestClient ksqlRestClient = clientWithServerAddresses(singleServerAddress);

    // Then:
    assertThat(ksqlRestClient.getServerAddress(), is(singleServerURI));
  }

  @Test
  public void shouldParseMultipleServerAddresses() throws Exception {
    // Given:
    final String firstServerAddress = "http://firstServer:8088";
    final String multipleServerAddresses = firstServerAddress + ",http://secondServer:8088";
    final URI firstServerURI = new URI(firstServerAddress);

    // When:
    KsqlRestClient ksqlRestClient = clientWithServerAddresses(multipleServerAddresses);

    // Then:
    assertThat(ksqlRestClient.getServerAddress(), is(firstServerURI));
  }

  @Test
  public void shouldThrowIfAnyServerAddressIsInvalid() {
    // When:
    final Exception e = assertThrows(
        KsqlRestClientException.class,
        () -> clientWithServerAddresses("http://firstServer:8088,secondBuggyServer.8088")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The supplied serverAddress is invalid: secondBuggyServer.8088"));
  }

  @Test
  public void shouldParseHttpsAddress() throws Exception {
    // Given:
    final String serverAddress = "https://singleServer:8088";
    final URI serverURI = new URI(serverAddress);

    // When:
    KsqlRestClient ksqlRestClient = clientWithServerAddresses(serverAddress);

    // Then:
    assertThat(ksqlRestClient.getServerAddress(), is(serverURI));
  }

  @Test
  public void shouldParseHttpAddressWithoutPort() throws Exception {
    // Given:
    final String serverAddress = "http://singleServer";
    final URI serverURI = new URI(serverAddress.concat(":80"));

    // When:
    KsqlRestClient ksqlRestClient = clientWithServerAddresses(serverAddress);

    // Then:
    assertThat(ksqlRestClient.getServerAddress(), is(serverURI));
  }

  @Test
  public void shouldParseHttpsAddressWithoutPort() throws Exception {
    // Given:
    final String serverAddress = "https://singleServer";
    final URI serverURI = new URI(serverAddress.concat(":443"));

    // When:
    KsqlRestClient ksqlRestClient = clientWithServerAddresses(serverAddress);

    // Then:
    assertThat(ksqlRestClient.getServerAddress(), is(serverURI));
  }

  @Test
  public void shouldIncludeAdditionalHeadersForCCloudConnectorRequest() throws Exception {
    // Given:
    final KsqlRestClient ksqlRestClient = clientWithServerAddresses(
        SOME_SERVER_ADDRESS,
        Optional.of(BasicCredentials.of(CCLOUD_API_KEY, CCLOUD_API_SECRET)));
    ksqlRestClient.setIsCCloudServer(true);

    final Map<String, String> expectedHeaders = ImmutableMap.of(
        CCLOUD_CONNECT_USERNAME_HEADER, CCLOUD_API_KEY,
        CCLOUD_CONNECT_PASSWORD_HEADER, CCLOUD_API_SECRET
    );

    // When:
    ksqlRestClient.makeConnectorRequest("create connector ... ;", 0L);

    // Then:
    verify(client).target(new URI(SOME_SERVER_ADDRESS), expectedHeaders);
    verify(target).postKsqlRequest("create connector ... ;", Collections.emptyMap(), Optional.of(0L));
  }

  @Test
  public void shouldNotIncludeAdditionalHeadersForOnPremConnectorRequest() throws Exception {
    // Given:
    final KsqlRestClient ksqlRestClient = clientWithServerAddresses(
        SOME_SERVER_ADDRESS,
        Optional.of(BasicCredentials.of(CCLOUD_API_KEY, CCLOUD_API_SECRET)));
    // even though a cloud apikey has been provided above, it will be ignored (not sent
    // with any requests) because the server connected to is not a ccloud server
    ksqlRestClient.setIsCCloudServer(false);

    // When:
    ksqlRestClient.makeConnectorRequest("create connector ... ;", 0L);

    // Then:
    verify(client).target(new URI(SOME_SERVER_ADDRESS), Collections.emptyMap());
    verify(target).postKsqlRequest("create connector ... ;", Collections.emptyMap(), Optional.of(0L));
  }

  @Test
  public void shouldNotIncludeAdditionalHeadersForCCloudNonConnectorRequest() throws Exception {
    // Given:
    final KsqlRestClient ksqlRestClient = clientWithServerAddresses(
        SOME_SERVER_ADDRESS,
        Optional.of(BasicCredentials.of(CCLOUD_API_KEY, CCLOUD_API_SECRET)));
    ksqlRestClient.setIsCCloudServer(true);

    // When:
    ksqlRestClient.makeKsqlRequest("some ksql;", 0L);

    // Then:
    verify(client).target(new URI(SOME_SERVER_ADDRESS), Collections.emptyMap());
    verify(target).postKsqlRequest("some ksql;", Collections.emptyMap(), Optional.of(0L));
  }

  @Test
  public void shouldNotIncludeAdditionalHeadersForOnPremNonConnectorRequest() throws Exception {
    // Given:
    final KsqlRestClient ksqlRestClient = clientWithServerAddresses(SOME_SERVER_ADDRESS);
    ksqlRestClient.setIsCCloudServer(false);

    // When:
    ksqlRestClient.makeKsqlRequest("some ksql;", 0L);

    // Then:
    verify(client).target(new URI(SOME_SERVER_ADDRESS), Collections.emptyMap());
    verify(target).postKsqlRequest("some ksql;", Collections.emptyMap(), Optional.of(0L));
  }

  @Test
  public void shouldAllowCallingCreateWithoutNeedingACCloudApiKey() {
    // This is a backwards compatibility check

    final KsqlRestClient client = KsqlRestClient.create(SOME_SERVER_ADDRESS, LOCAL_PROPS, CLIENT_PROPS, Optional.empty());
    assertThat(client, is(instanceOf(KsqlRestClient.class)));

    // Also with new signature
    final KsqlRestClient ccloudClient = KsqlRestClient.create(SOME_SERVER_ADDRESS, LOCAL_PROPS, CLIENT_PROPS, Optional.empty(), Optional.empty());
    assertThat(ccloudClient, is(instanceOf(KsqlRestClient.class)));
  }

  private KsqlRestClient clientWithServerAddresses(final String serverAddresses) {
    return clientWithServerAddresses(serverAddresses, Optional.empty());
  }

  private KsqlRestClient clientWithServerAddresses(
      final String serverAddresses,
      final Optional<BasicCredentials> ccloudApiKey
  ) {
    return KsqlRestClient.create(
        serverAddresses,
        LOCAL_PROPS,
        CLIENT_PROPS,
        Optional.empty(),
        ccloudApiKey,
        clientSupplier
    );
  }

}