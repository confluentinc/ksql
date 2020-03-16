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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.properties.LocalProperties;
import java.net.URI;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestClientTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlClient client;
  @Mock
  private LocalProperties localProps;

  @Test
  public void shouldThrowOnInvalidServerAddress() {
    // Then:
    expectedException.expect(KsqlRestClientException.class);
    expectedException.expectMessage("The supplied serverAddress is invalid: timbuktu");

    // When:
    new KsqlRestClient(client, "timbuktu", localProps);
  }

  @Test
  public void shouldParseSingleServerAddress() throws Exception {
    final String singleServerAddress = "http://singleServer:8088";
    final URI singleServerURI = new URI(singleServerAddress);

    KsqlRestClient ksqlRestClient = new KsqlRestClient(client, singleServerAddress, localProps);
    assertThat(ksqlRestClient.getServerAddress(), is(singleServerURI));
  }

  @Test
  public void shouldParseMultipleServerAddresses() throws Exception {
    final String firstServerAddress = "http://firstServer:8088";
    final String multipleServerAddresses = firstServerAddress + ",http://secondServer:8088";
    final URI firstServerURI = new URI(firstServerAddress);
    KsqlRestClient ksqlRestClient = new KsqlRestClient(client, multipleServerAddresses, localProps);
    assertThat(ksqlRestClient.getServerAddress(), is(firstServerURI));
  }

  @Test
  public void shouldThrowIfAnyServerAddressIsInvalid() {
    expectedException.expect(KsqlRestClientException.class);
    expectedException
        .expectMessage("The supplied serverAddress is invalid: secondBuggyServer.8088");
    new KsqlRestClient(client, "http://firstServer:8088,secondBuggyServer.8088", localProps);
  }

}