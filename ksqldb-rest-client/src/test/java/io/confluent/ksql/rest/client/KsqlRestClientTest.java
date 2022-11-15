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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.client.KsqlRestClient.KsqlClientSupplier;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestClientTest {

  private static final Map<String, ?> LOCAL_PROPS = ImmutableMap.of("auto.offset.reset", "earliest");
  private static final Map<String, String> CLIENT_PROPS = ImmutableMap.of("foo", "bar");

  @Mock
  private KsqlClient client;
  @Mock
  private KsqlClientSupplier clientSupplier;

  @Before
  public void setUp() {
    when(clientSupplier.get(any(), any(), any())).thenReturn(client);
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
    final String singleServerAddress = "http://singleServer:8088";
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

  private KsqlRestClient clientWithServerAddresses(final String serverAddresses) {
    return KsqlRestClient.create(
        serverAddresses,
        LOCAL_PROPS,
        CLIENT_PROPS,
        Optional.empty(),
        clientSupplier
    );
  }

}