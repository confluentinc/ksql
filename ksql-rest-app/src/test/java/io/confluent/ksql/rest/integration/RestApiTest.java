/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.integration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.common.utils.TestUtils;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.rest.RestConfig;
import io.confluent.rest.validation.JacksonMessageBodyProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static io.confluent.ksql.rest.server.utils.TestUtils.randomFreeLocalPort;

@Category({IntegrationTest.class})
public class RestApiTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestApiTest.class);
  private static final IntegrationTestHarness testHarness = new IntegrationTestHarness();
  private static final String PAGE_VIEW_TOPIC = "pageviews";
  private static final String PAGE_VIEW_STREAM = "pageviews_original";

  private static final int NUM_RETRIES = 5;
  private static KsqlRestApplication restApplication;

  private static String serverAddress;


  private Client restClient;

  @BeforeClass
  public static void setUpClass() throws Exception {
    final Map<String, Object> config = new HashMap<>();
    config.put(KsqlRestConfig.INSTALL_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    config.put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "rest_api_test_service");

    testHarness.start(config);

    startRestServer(testHarness.allConfigs());

    testHarness.createTopic(PAGE_VIEW_TOPIC);

    testHarness.publishTestData(PAGE_VIEW_TOPIC, new PageViewDataProvider(),
                                System.currentTimeMillis());

    createStreams();
  }

  @Before
  public void setUp() {
    restClient = buildClient();
  }

  @Test
  public void shouldExecuteStreamingQueryWithV1ContentType() {
    final KsqlRequest request = new KsqlRequest(String.format("SELECT * from %s;",
                                                              PAGE_VIEW_STREAM),
                                                Collections.emptyMap());
    try (final Response response = restClient.target(serverAddress)
        .path("query")
        .request(Versions.KSQL_V1_JSON)
        .header("Content-Type", Versions.KSQL_V1_JSON)
        .post(Entity.json(request))) {
      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
  }

  @Test
  public void shouldExecuteStreamingQueryWithJsonContentType() {
    final KsqlRequest request = new KsqlRequest(String.format("SELECT * from %s;",
                                                              PAGE_VIEW_STREAM),
                                                Collections.emptyMap());
    try (final Response response = restClient.target(serverAddress)
        .path("query")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header("Content-Type", MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(request))) {

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
  }

  @After
  public void cleanUp() {
    restClient.close();
  }

  @AfterClass
  public static void cleanUpClass() throws Exception {
    restApplication.stop();
    testHarness.stop();
  }

  private static class DummyVersionCheckerAgent implements VersionCheckerAgent {
    @Override
    public void start(final KsqlModuleType moduleType, final Properties ksqlProperties) {
      // do nothing;
    }
  }

  private static Client buildClient() {
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    objectMapper.registerModule(new Jdk8Module());
    final JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(objectMapper);
    return ClientBuilder.newBuilder().register(jsonProvider).build();

  }

  private static void createStreams() {
    try (final KsqlRestClient ksqlRestClient = new KsqlRestClient(serverAddress)) {
      final RestResponse createStreamResponse =
          ksqlRestClient
              .makeKsqlRequest(String.format("CREATE STREAM %s (viewtime bigint, pageid varchar, "
                                             + "userid varchar) WITH (kafka_topic='pageviews',"
                                             + " value_format='json');", PAGE_VIEW_STREAM));
      assertTrue(createStreamResponse.isSuccessful());
    }
  }

  private static void startRestServer(Map<String, Object> configs) throws Exception {
    int retries = NUM_RETRIES;
    while (0 < retries) {
      try {
        final int port = randomFreeLocalPort();
        serverAddress = "http://localhost:" + port;
        configs.put(RestConfig.LISTENERS_CONFIG, serverAddress);
        restApplication = KsqlRestApplication.buildApplication(new KsqlRestConfig(configs),
                                                               new DummyVersionCheckerAgent());
        restApplication.start();
        return;
      } catch (BindException e) {
        LOGGER.info("Failed to start rest server due to bind exception.", e);
        retries--;
        if (retries == 0) {
          LOGGER.error("Could not start server after {} retries", NUM_RETRIES);
        }
      }
    }
  }
}
