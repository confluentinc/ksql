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

import static io.confluent.ksql.util.KsqlConfig.CONNECT_REQUEST_TIMEOUT_DEFAULT;
import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.test.util.OptionalMatchers;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.apache.http.HttpStatus;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.ConnectorState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.TaskState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.PluginInfo;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class DefaultConnectClientTest {

  private static final ObjectMapper MAPPER = ConnectJsonMapper.INSTANCE.get();
  private static final ConnectorInfo SAMPLE_INFO = new ConnectorInfo(
      "foo",
      ImmutableMap.of("key", "value"),
      ImmutableList.of(new ConnectorTaskId("foo", 1)),
      ConnectorType.SOURCE
  );
  private static final ConnectorStateInfo SAMPLE_STATUS = new ConnectorStateInfo(
      "foo",
      new ConnectorState("state", "worker", "msg"),
      ImmutableList.of(
          new TaskState(0, "taskState", "worker", "taskMsg")
      ),
      ConnectorType.SOURCE
  );
  private static final PluginInfo SAMPLE_PLUGIN = new PluginInfo(
      "io.confluent.connect.replicator.ReplicatorSourceConnector",
      PluginType.SOURCE,
      "1.0"
  );
  private static final String AUTH_HEADER = "Basic FOOBAR";

  private static final Map<String, ActiveTopicsInfo> SAMPLE_TOPICS = ImmutableMap.of(
      "foo",
      new ActiveTopicsInfo(
          "foo",
          ImmutableList.of("topicA", "topicB"))
  );

  private static final String CUSTOM_HEADER_NAME = "custom_header";
  private static final String CUSTOM_HEADER_VALUE = "foo";

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(
      WireMockConfiguration.wireMockConfig()
          .dynamicPort()
  );

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Parameterized.Parameters(name = "{1}")
  public static Collection<String[]> pathPrefixes() {
    return ImmutableList.of(new String[]{"", "no path prefix"}, new String[]{"/some/path/prefix", "with path prefix"});
  }

  @Mock
  private SSLContext sslContext;
  @Mock
  private SSLSocketFactory sslSocketFactory;

  private final String pathPrefix;

  private ConnectClient client;

  public DefaultConnectClientTest(final String pathPrefix, final String testName) {
    this.pathPrefix = pathPrefix;
  }

  @Before
  public void setup() {
    when(sslContext.getSocketFactory()).thenReturn(sslSocketFactory);

    client = new DefaultConnectClient(
        "http://localhost:" + wireMockRule.port() + pathPrefix,
        Optional.of(AUTH_HEADER),
        ImmutableMap.of(CUSTOM_HEADER_NAME, CUSTOM_HEADER_VALUE),
        Optional.of(sslContext),
        false,
        CONNECT_REQUEST_TIMEOUT_DEFAULT);
  }

  @Test
  public void testCreate() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.post(WireMock.urlEqualTo(pathPrefix + "/connectors"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
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
  public void testValidate() throws JsonProcessingException {
    // Given:
    final String plugin = SAMPLE_PLUGIN.className();
    final String url = String.format(pathPrefix + "/connector-plugins/%s/config/validate", plugin);
    final ConfigInfos body = new ConfigInfos(
        plugin,
        1,
        ImmutableList.of("Common"),
        ImmutableList.of(new ConfigInfo(new ConfigKeyInfo(
            "file",
            "STRING",
            true,
            "",
            "HIGH",
            "Destination filename.",
            null,
            -1,
            "NONE",
            "file",
            Collections.emptyList()),
            new ConfigValueInfo(
                "file",
                null,
                Collections.emptyList(),
                ImmutableList.of(
                    "Missing required configuration \"file\" which has no default value."),
                true)
            )));

    WireMock.stubFor(
        WireMock.put(WireMock.urlEqualTo(url))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_OK)
                .withBody(MAPPER.writeValueAsString(body)))
    );

    // When:
    final Map<String, String> config = ImmutableMap.of(
        "connector.class", plugin,
        "tasks.max", "1",
        "topics", "test-topic"
    );
    final ConnectResponse<ConfigInfos> response = client.validate(plugin, config);

    // Then:
    assertThat(response.datum(), OptionalMatchers.of(is(body)));
    assertThat("Expected no error!", !response.error().isPresent());
  }

  @Test
  public void testValidateWithError() throws JsonProcessingException {
    // Given:
    final String plugin = SAMPLE_PLUGIN.className();
    final String url = String.format(pathPrefix + "/connector-plugins/%s/config/validate", plugin);
    WireMock.stubFor(
        WireMock.put(WireMock.urlEqualTo(url))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                .withBody("Oh no!"))
    );

    // When:
    final ConnectResponse<ConfigInfos> response =
        client.validate(plugin, ImmutableMap.of());

    // Then:
    assertThat("Expected no datum!", !response.datum().isPresent());
    assertThat(response.error(), OptionalMatchers.of(is("Oh no!")));
  }

  @Test
  public void testCreateWithError() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.post(WireMock.urlEqualTo(pathPrefix + "/connectors"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                .withBody("Oh no!"))
    );

    // When:
    final ConnectResponse<ConnectorInfo> response =
        client.create("foo", ImmutableMap.of());

    // Then:
    assertThat("Expected no datum!", !response.datum().isPresent());
    assertThat(response.error(), OptionalMatchers.of(is("Oh no!")));
  }

  @Test
  public void testList() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.get(WireMock.urlEqualTo(pathPrefix + "/connectors"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_OK)
                .withBody(MAPPER.writeValueAsString(ImmutableList.of("one", "two"))))
    );

    // When:
    final ConnectResponse<List<String>> response = client.connectors();

    // Then:
    assertThat(response.datum(), OptionalMatchers.of(is(ImmutableList.of("one", "two"))));
    assertThat("Expected no error!", !response.error().isPresent());
  }

  @Test
  public void testListPlugins() throws JsonProcessingException {
    // Given:

    MAPPER.writeValueAsString(ImmutableList.of(SAMPLE_PLUGIN));
    WireMock.stubFor(
        WireMock.get(WireMock.urlEqualTo(pathPrefix + "/connector-plugins"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_OK)
                .withBody(MAPPER.writeValueAsString(ImmutableList.of(SAMPLE_PLUGIN))))
    );

    // When:
    final ConnectResponse<List<PluginInfo>> response = client.connectorPlugins();

    // Then:
    assertThat(response.datum(), OptionalMatchers.of(is(ImmutableList.of(SAMPLE_PLUGIN))));
    assertThat("Expected no error!", !response.error().isPresent());
  }

  @Test
  public void testDescribe() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.get(WireMock.urlEqualTo(pathPrefix + "/connectors/foo"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_OK)
                .withBody(MAPPER.writeValueAsString(SAMPLE_INFO)))
    );

    // When:
    final ConnectResponse<ConnectorInfo> response = client.describe("foo");

    // Then:
    assertThat(response.datum(), OptionalMatchers.of(is(SAMPLE_INFO)));
    assertThat("Expected no error!", !response.error().isPresent());
  }

  @Test
  public void testStatus() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.get(WireMock.urlEqualTo(pathPrefix + "/connectors/foo/status"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_OK)
                .withBody(MAPPER.writeValueAsString(SAMPLE_STATUS)))
    );

    // When:
    final ConnectResponse<ConnectorStateInfo> response = client.status("foo");

    // Then:
    final ConnectorStateInfo connectorStateInfo = response.datum().orElseThrow(AssertionError::new);
    // equals is not implemented on ConnectorStateInfo
    assertThat(connectorStateInfo.name(), is(SAMPLE_STATUS.name()));
    assertThat(connectorStateInfo.type(), is(SAMPLE_STATUS.type()));
    assertThat(connectorStateInfo.connector().state(), is(SAMPLE_STATUS.connector().state()));
    assertThat(connectorStateInfo.connector().workerId(), is(SAMPLE_STATUS.connector().workerId()));
    assertThat(connectorStateInfo.connector().trace(), is(SAMPLE_STATUS.connector().trace()));
    assertThat(connectorStateInfo.tasks().size(), is(SAMPLE_STATUS.tasks().size()));
    assertThat(connectorStateInfo.tasks().get(0).id(), is(SAMPLE_STATUS.tasks().get(0).id()));
    assertThat("Expected no error!", !response.error().isPresent());
  }

  @Test
  public void testDeleteWithStatusNoContentResponse() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.delete(WireMock.urlEqualTo(pathPrefix + "/connectors/foo"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_NO_CONTENT))
    );

    // When:
    final ConnectResponse<String> response = client.delete("foo");

    // Then:
    assertThat(response.datum(), OptionalMatchers.of(is("foo")));
    assertThat("Expected no error!", !response.error().isPresent());
  }

  @Test
  public void testDeleteWithStatusOKResponse() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.delete(WireMock.urlEqualTo(pathPrefix + "/connectors/foo"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_OK)
                .withBody("{\"error\":null}"))
    );

    // When:
    final ConnectResponse<String> response = client.delete("foo");

    // Then:
    assertThat(response.datum(), OptionalMatchers.of(is("foo")));
    assertThat("Expected no error!", !response.error().isPresent());
  }

  @Test
  public void testTopics() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.get(WireMock.urlEqualTo(pathPrefix + "/connectors/foo/topics"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_OK)
                .withBody(MAPPER.writeValueAsString(SAMPLE_TOPICS)))
    );

    // When:
    final ConnectResponse<Map<String, Map<String, List<String>>>> response = client.topics("foo");

    // Then:
    final Map<String, Map<String, List<String>>> activeTopics = response.datum()
        .orElseThrow(AssertionError::new);
    // equals is not implemented on ConnectorStateInfo
    assertThat(activeTopics.keySet(), is(ImmutableSet.of("foo")));
    assertThat(activeTopics.get("foo").get("topics"), is(ImmutableList.of("topicA", "topicB")));
    assertThat("Expected no error!", !response.error().isPresent());
  }

  @Test
  public void testListShouldRetryOnFailure() throws JsonProcessingException {
    // Given:
    WireMock.stubFor(
        WireMock.get(WireMock.urlEqualTo(pathPrefix + "/connectors"))
            .withHeader(AUTHORIZATION.toString(), new EqualToPattern(AUTH_HEADER))
            .withHeader(CUSTOM_HEADER_NAME, new EqualToPattern(CUSTOM_HEADER_VALUE))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                .withBody("Encountered an error!"))
            .willReturn(WireMock.aResponse()
                .withStatus(HttpStatus.SC_OK)
                .withBody(MAPPER.writeValueAsString(ImmutableList.of("one", "two"))))
    );

    // When:
    final ConnectResponse<List<String>> response = client.connectors();

    // Then:
    assertThat(response.datum(), OptionalMatchers.of(is(ImmutableList.of("one", "two"))));
    assertThat("Expected no error!", !response.error().isPresent());
  }

}