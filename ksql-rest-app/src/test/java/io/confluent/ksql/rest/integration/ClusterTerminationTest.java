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

import static io.confluent.ksql.serde.DataSource.DataSourceSerDe.JSON;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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
      .buildWithServiceContext(TEST_HARNESS::serviceContext);

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  private KsqlRestClient restClient;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    createStreams(PAGE_VIEW_STREAM);
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
    makeKsqlRequest("CREATE STREAM " + SINK_STREAM
        + " WITH (kafka_topic='" + SINK_TOPIC + "',value_format='avro')"
        + " AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );

    // Produce to stream so that schema is registered by AvroConverter
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEW_DATA_PROVIDER, JSON, System::currentTimeMillis);

    TEST_HARNESS.verifySubjectPresent(SINK_TOPIC + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);

    // When:
    terminateCluster(ImmutableList.of(SINK_TOPIC));

    // Then:
    TEST_HARNESS.verifySubjectAbsent(SINK_TOPIC + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }

  private static List<KsqlEntity> awaitResults(final List<KsqlEntity> pending) {
    try (final KsqlRestClient ksqlRestClient = REST_APP.buildKsqlClient()) {
      return pending.stream()
          .map(e -> awaitResult(e, ksqlRestClient))
          .collect(Collectors.toList());
    }
  }

  private static KsqlEntity awaitResult(
      final KsqlEntity e,
      final KsqlRestClient ksqlRestClient
  ) {
    if (!(e instanceof CommandStatusEntity)) {
      return e;
    }

    CommandStatusEntity cse = (CommandStatusEntity) e;
    final String commandId = cse.getCommandId().toString();

    while (cse.getCommandStatus().getStatus() != Status.ERROR
        && cse.getCommandStatus().getStatus() != Status.SUCCESS) {

      final RestResponse<CommandStatus> res = ksqlRestClient.makeStatusRequest(commandId);

      throwOnError(res);

      cse = new CommandStatusEntity(
          cse.getStatementText(),
          cse.getCommandId(),
          res.getResponse(),
          cse.getCommandSequenceNumber()
      );
    }

    return cse;
  }

  @SuppressWarnings("unchecked")
  private List<KsqlEntity> makeKsqlRequest(final String sql) {
    final RestResponse<KsqlEntityList> res = restClient.makeKsqlRequest(sql);

    throwOnError(res);

    return awaitResults(res.getResponse());
  }

  private static void throwOnError(final RestResponse<?> res) {
    if (res.isErroneous()) {
      throw new AssertionError("Failed to await result."
          + "msg: " + res.getErrorMessage());
    }
  }

  private static void createStreams(final String streamName) {
    final Client client = TestKsqlRestApp.buildClient();

    try (final Response response = client
        .target(REST_APP.getHttpListener())
        .path("ksql")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(ksqlRequest(
            "CREATE STREAM " + streamName + " "
                + "(viewtime bigint, pageid varchar, userid varchar) "
                + "WITH (kafka_topic='" + PAGE_VIEW_TOPIC + "', value_format='json');"))) {

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    } finally {
      client.close();
    }
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

  private static Entity<?> ksqlRequest(final String sql) {
    return Entity.json(new KsqlRequest(sql, Collections.emptyMap(), null));
  }

  private static Entity<?> terminateClusterRequest(final List<String> deleteTopicList) {
    return Entity.json(new ClusterTerminateRequest(deleteTopicList));
  }
}
