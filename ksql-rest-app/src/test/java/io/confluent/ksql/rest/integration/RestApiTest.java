/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.serde.DataSource.DataSourceSerDe.JSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.test.util.TestKsqlRestApp;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.Collections;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

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

    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, new PageViewDataProvider(), JSON, System::currentTimeMillis);

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
    final KsqlRequest request = new KsqlRequest(
        String.format("SELECT * from %s;", PAGE_VIEW_STREAM), Collections.emptyMap(), null);

    try (final Response response = restClient.target(REST_APP.getHttpListener())
        .path("query")
        .request(Versions.KSQL_V1_JSON)
        .header("Content-Type", Versions.KSQL_V1_JSON)
        .post(Entity.json(request))) {
      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
  }

  @Test
  public void shouldExecuteStreamingQueryWithJsonContentType() {
    final KsqlRequest request = new KsqlRequest(
        String.format("SELECT * from %s;", PAGE_VIEW_STREAM), Collections.emptyMap(), null);

    try (final Response response = restClient.target(REST_APP.getHttpListener())
        .path("query")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header("Content-Type", MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(request))) {

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
  }

  private static void createStreams() {
    try (final KsqlRestClient ksqlRestClient =
        new KsqlRestClient(REST_APP.getHttpListener().toString())
    ) {
      final RestResponse createStreamResponse = ksqlRestClient
              .makeKsqlRequest(String.format(
                  "CREATE STREAM %s (viewtime bigint, pageid varchar, userid varchar)"
                      + " WITH (kafka_topic='pageviews', value_format='json');", PAGE_VIEW_STREAM),
                  null);
      assertTrue(createStreamResponse.isSuccessful());
    }
  }
}
