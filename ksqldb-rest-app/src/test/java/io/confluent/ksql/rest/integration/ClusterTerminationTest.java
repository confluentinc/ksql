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
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
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
import io.confluent.ksql.rest.server.KsqlRestConfig;
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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
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
  private static final String AGG_TABLE = "agg_table";
  private static final String AGG_TOPIC = "agg_topic";
  private static final String INTERNAL_TOPIC_AGG =
      "_confluent-ksql-default_query_CTAS_AGG_TABLE_3-Aggregate-Aggregate-Materialize-changelog";
  private static final String INTERNAL_TOPIC_GROUPBY =
      "_confluent-ksql-default_query_CTAS_AGG_TABLE_3-Aggregate-GroupBy-repartition";
  private static final String ALL_TOPICS = ".*";
  private static final long WAIT_FOR_TOPIC_TIMEOUT_MS = 500;
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .build();

  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8089")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS));

  @Rule
  public final RuleChain CHAIN_TEST = RuleChain
      .outerRule(TEST_HARNESS)
      .around(REST_APP_0)
      .around(REST_APP_1);

  @Before
  public void setUp() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);

    // Given
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM " + SINK_STREAM
            + " WITH (kafka_topic='" + SINK_TOPIC + "',format='avro')"
            + " AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(SINK_TOPIC);
    // Produce to stream so that schema is registered by AvroConverter
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, KAFKA, JSON, System::currentTimeMillis);

    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(SINK_TOPIC, true));
    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(SINK_TOPIC, false));
  }

  @Test
  public void shouldCleanUpSinkTopicsAndSchemasDuringClusterTermination() throws Exception {
    // When:
    terminateCluster(ImmutableList.of(SINK_TOPIC), REST_APP_0);

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
    shouldReturn50304WhenTerminating();
  }

  @Test
  public void shouldTerminateAllTopicsWithStarInBody() throws InterruptedException {
    // Given
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE TABLE " + AGG_TABLE
            + " WITH (kafka_topic='" + AGG_TOPIC + "',format='avro')"
            + " AS SELECT USERID, COUNT(*) FROM " + PAGE_VIEW_STREAM + " GROUP BY USERID;"
    );

    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(AGG_TOPIC);

    // Produce to stream so that schema is registered by AvroConverter
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, KAFKA, JSON, System::currentTimeMillis);

    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(INTERNAL_TOPIC_AGG);
    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(INTERNAL_TOPIC_GROUPBY);

    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(AGG_TOPIC, true));
    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(AGG_TOPIC, false));

    // When:
    terminateCluster(ImmutableList.of(ALL_TOPICS), REST_APP_0);

    // Then:
    waitForTopicsToBeAbsentWithTimeout(AGG_TOPIC);
    waitForTopicsToBeAbsentWithTimeout(INTERNAL_TOPIC_AGG);
    waitForTopicsToBeAbsentWithTimeout(INTERNAL_TOPIC_GROUPBY);

    TEST_HARNESS.waitForSubjectToBeAbsent(KsqlConstants.getSRSubject(AGG_TOPIC, true));
    TEST_HARNESS.waitForSubjectToBeAbsent(KsqlConstants.getSRSubject(AGG_TOPIC, false));

    assertThat(
        "Should not delete non-sink topics",
        TEST_HARNESS.topicExists(PAGE_VIEW_TOPIC),
        is(true)
    );

    // should only be the pageview topic left
    assertThat(TEST_HARNESS.getKafkaCluster().getTopics().size(), is(1));

    // Then:
    shouldReturn50304WhenTerminating();
  }

  private void waitForTopicsToBeAbsentWithTimeout(final String topic) {
    assertThatEventually(
        () -> "expected topics to be deleted",
        () -> {
          try {
            TEST_HARNESS.getKafkaCluster().waitForTopicsToBeAbsent(topic);
            return true;
          } catch (AssertionError e) {
            return false;
          }
        },
        is(true),
        WAIT_FOR_TOPIC_TIMEOUT_MS,
        TimeUnit.MILLISECONDS,
        500,
        30000);
  }

  @Test
  public void shouldTerminateEvenWithMultipleServers(){
    // When:
    terminateCluster(ImmutableList.of(ALL_TOPICS), REST_APP_1);

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
    shouldReturn50304WhenTerminating();
  }

  private void shouldReturn50304WhenTerminating() {
    // Given: TERMINATE CLUSTER has been issued

    // When:
    final KsqlErrorMessage error = RestIntegrationTestUtil.makeKsqlRequestWithError(REST_APP_0, "SHOW STREAMS;");

    // Then:
    assertThatEventually(() -> error.getErrorCode(), is(Errors.ERROR_CODE_SERVER_SHUT_DOWN));
  }

  private static void terminateCluster(final List<String> deleteTopicList, final TestKsqlRestApp app) {

    HttpResponse<Buffer> resp = RestIntegrationTestUtil
        .rawRestRequest(app, HttpVersion.HTTP_1_1, HttpMethod.POST, "/ksql/terminate",
                        new ClusterTerminateRequest(deleteTopicList), Optional.empty());

    assertThat(resp.statusCode(), is(OK.code()));
  }
}
