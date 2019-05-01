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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.serde.Format.JSON;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
public class ClusterTerminationTest {

  private static final String PAGE_VIEW_TOPIC = "pageviews";
  private static final String PAGE_VIEW_STREAM = "pageviews_original";
  private static final PageViewDataProvider PAGE_VIEW_DATA_PROVIDER = new PageViewDataProvider();

  private static final String SINK_TOPIC = "sink_topic";
  private static final String SINK_STREAM = "sink_stream";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withServiceContext(TEST_HARNESS::getServiceContext)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  private KsqlRestClient restClient;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    RestIntegrationTestUtil.createStreams(REST_APP, PAGE_VIEW_STREAM, PAGE_VIEW_TOPIC);
  }

  @Before
  public void setUp() {
    restClient = REST_APP.buildKsqlClient();
  }

  @After
  public void cleanUp() {
    restClient.close();
  }

  @Test
  public void shouldCleanUpSinkTopicsAndSchemasDuringClusterTermination() throws Exception {
    // Given:
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP,
        restClient,
        "CREATE STREAM " + SINK_STREAM
            + " WITH (kafka_topic='" + SINK_TOPIC + "',value_format='avro')"
            + " AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );

    TEST_HARNESS.waitForTopicsToBePresent(SINK_TOPIC);

    // Produce to stream so that schema is registered by AvroConverter
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEW_DATA_PROVIDER, JSON, System::currentTimeMillis);

    TEST_HARNESS.waitForSubjectToBePresent(SINK_TOPIC + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);

    // When:
    terminateCluster(ImmutableList.of(SINK_TOPIC));

    // Then:
    TEST_HARNESS.waitForTopicsToBeAbsent(SINK_TOPIC);

    TEST_HARNESS.waitForSubjectToBeAbsent(SINK_TOPIC + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }

  private static void terminateCluster(final List<String> deleteTopicList) {
    final Client client = TestKsqlRestApp.buildClient();

    try (final Response response = client
        .target(REST_APP.getHttpListener())
        .path("ksql/terminate")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(terminateClusterRequest(deleteTopicList))) {

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    } finally {
      client.close();
    }
  }

  private static Entity<?> terminateClusterRequest(final List<String> deleteTopicList) {
    return Entity.json(new ClusterTerminateRequest(deleteTopicList));
  }
}
