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

import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PageViewDataProvider;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.HttpResponse;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class ClusterTerminationTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();

  private static final String SINK_TOPIC = "sink_topic";
  private static final String SINK_STREAM = "sink_stream";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    RestIntegrationTestUtil.createStream(REST_APP, PAGE_VIEWS_PROVIDER);
  }

  @Test
  public void shouldCleanUpSinkTopicsAndSchemasDuringClusterTermination() throws Exception {
    // Given:
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP,
        "CREATE STREAM " + SINK_STREAM
            + " WITH (kafka_topic='" + SINK_TOPIC + "',format='avro')"
            + " AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );

    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(SINK_TOPIC);

    // Produce to stream so that schema is registered by AvroConverter
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, KAFKA, JSON, System::currentTimeMillis);

    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(SINK_TOPIC, true));
    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(SINK_TOPIC, false));

    // When:
    terminateCluster(ImmutableList.of(SINK_TOPIC));

    // Then:
    TEST_HARNESS.getKafkaCluster().waitForTopicsToBeAbsent(SINK_TOPIC);

    TEST_HARNESS.waitForSubjectToBeAbsent(KsqlConstants.getSRSubject(SINK_TOPIC, true));
    TEST_HARNESS.waitForSubjectToBeAbsent(KsqlConstants.getSRSubject(SINK_TOPIC, false));

    assertThat(
        "Should not delete non-sink topics",
        TEST_HARNESS.topicExists(PAGE_VIEW_TOPIC),
        is(true)
    );

    // Then:
    shouldReturn50303WhenTerminating();
  }

  private void shouldReturn50303WhenTerminating() {
    // Given: TERMINATE CLUSTER has been issued

    // When:
    final KsqlErrorMessage error = RestIntegrationTestUtil.makeKsqlRequestWithError(REST_APP, "SHOW STREAMS;");

    // Then:
    assertThat(error.getErrorCode(), is(Errors.ERROR_CODE_SERVER_SHUTTING_DOWN));
  }

  private static void terminateCluster(final List<String> deleteTopicList) {

    HttpResponse<Buffer> resp = RestIntegrationTestUtil
        .rawRestRequest(REST_APP, HttpVersion.HTTP_1_1, HttpMethod.POST, "/ksql/terminate",
                        new ClusterTerminateRequest(deleteTopicList), Optional.empty());

    assertThat(resp.statusCode(), is(OK.code()));
  }

}
