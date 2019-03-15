/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static org.junit.Assert.assertEquals;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import java.util.Collections;
import java.util.Optional;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
public class RestApiTest {

  private static final String PAGE_VIEW_TOPIC = "pageviews";
  private static final String PAGE_VIEW_STREAM = "pageviews_original";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  private Client restClient;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    createStreams();
  }

  @Before
  public void setUp() {
    restClient = TestKsqlRestApp.buildClient();
  }

  @After
  public void cleanUp() {
    restClient.close();
  }

  @Test
  public void shouldExecuteStreamingQueryWithV1ContentType() {
    try (final Response response = makeStreamingRequest(
        "SELECT * from " + PAGE_VIEW_STREAM + ";",
        Versions.KSQL_V1_JSON,
        Versions.KSQL_V1_JSON
    )) {
      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
  }

  @Test
  public void shouldExecuteStreamingQueryWithJsonContentType() {
    try (final Response response = makeStreamingRequest(
        "SELECT * from " + PAGE_VIEW_STREAM + "; ",
        MediaType.APPLICATION_JSON_TYPE.toString(),
        MediaType.APPLICATION_JSON_TYPE.toString()
    )) {
      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
  }

  private Response makeStreamingRequest(
      final String sql,
      final String mediaType,
      final String contentType
  ) {
    return makeRequest("query", sql, mediaType, Optional.ofNullable(contentType));
  }

  private Response makeRequest(
      final String path,
      final String sql,
      final String mediaType,
      final Optional<String> contentType
  ) {
    Builder builder = restClient
        .target(REST_APP.getHttpListener())
        .path(path)
        .request(mediaType);

    if (contentType.isPresent()) {
      builder = builder.header("Content-Type", contentType.get());
    }

    return builder.post(ksqlRequest(sql));
  }

  private static void createStreams() {
    final Client client = TestKsqlRestApp.buildClient();

    try (final Response response = client
        .target(REST_APP.getHttpListener())
        .path("ksql")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(ksqlRequest(
            "CREATE STREAM " + PAGE_VIEW_STREAM + " "
                + "(viewtime bigint, pageid varchar, userid varchar) "
                + "WITH (kafka_topic='" + PAGE_VIEW_TOPIC + "', value_format='json');"))) {

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    } finally {
      client.close();
    }
  }

  private static Entity<?> ksqlRequest(final String sql) {
    return Entity.json(new KsqlRequest(sql, Collections.emptyMap(), null));
  }
}
