/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.OptionalMatchers;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import org.apache.http.HttpStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class DefaultConnectClientTest {

  private static final ObjectMapper MAPPER = JsonMapper.INSTANCE.mapper;
  private static final ConnectorInfo SAMPLE_INFO = new ConnectorInfo(
      "foo",
      ImmutableMap.of("key", "value"),
      ImmutableList.of(new ConnectorTaskId("foo", 1)),
      ConnectorType.SOURCE
  );

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(
      WireMockConfiguration.wireMockConfig().dynamicPort());

  private ConnectClient client;

  @Before
  public void setup() {
    client = new DefaultConnectClient("http://localhost:" + wireMockRule.port());
  }

  @Test
  public void testCreate() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.post(WireMock.urlEqualTo("/connectors"))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_CREATED)
                .withBody(MAPPER.writeValueAsString(SAMPLE_INFO)))
    );

    // When:
    final ConnectResponse<ConnectorInfo> response =
        client.create("foo", ImmutableMap.of());

    // Then:
    assertThat(response.datum(), OptionalMatchers.of(is(SAMPLE_INFO)));
    assertThat("Expected no error!", !response.error().isPresent());
  }

  @Test
  public void testCreateWithError() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.post(WireMock.urlEqualTo("/connectors"))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_BAD_REQUEST)
                .withBody("Oh no!"))
    );

    // When:
    final ConnectResponse<ConnectorInfo> response =
        client.create("foo", ImmutableMap.of());

    // Then:
    assertThat("Expected no datum!", !response.datum().isPresent());
    assertThat(response.error(), OptionalMatchers.of(is("Oh no!")));
  }

}