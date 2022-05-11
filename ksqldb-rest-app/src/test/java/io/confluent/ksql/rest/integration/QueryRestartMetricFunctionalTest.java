/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.ksql.util.QueryMetadataImpl;
import io.confluent.ksql.util.QueryMetricsUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

@Ignore
public class QueryRestartMetricFunctionalTest {

  private static final String TEST_TOPIC_NAME = "test";
  private static final String TEST_TOPIC_NAME2 = "test-topic";
  private static final ImmutableMap<String, String> METRICS_TAGS = ImmutableMap.of(
	  "cluster.id", "cluster-1"
  );
  private static final String METRICS_TAGS_STRING = "cluster.id:cluster-1";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP_NO_SHARED_RUNTIME = TestKsqlRestApp
	  .builder(TEST_HARNESS::kafkaBootstrapServers)
	  .withProperty(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS, METRICS_TAGS_STRING)
	  .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
	  .withProperty(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, false)
	  .withProperty(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS, 0L)
	  .withProperty(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS, 300L)
	  .build();

  private static final TestKsqlRestApp REST_APP_SHARED_RUNTIME = TestKsqlRestApp
	  .builder(TEST_HARNESS::kafkaBootstrapServers)
	  .withProperty(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS, METRICS_TAGS_STRING)
	  .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
	  .withProperty(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, true)
	  .withProperty(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "another-id")
	  .withProperty(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS, 0L)
	  .withProperty(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS, 300L)
	  .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP_NO_SHARED_RUNTIME).around(REST_APP_SHARED_RUNTIME);

  private Metrics metricsNoSharedRuntime;
  private Metrics metricsSharedRuntime;

  @BeforeClass
  public static void setUpClass() throws InterruptedException {
	TEST_HARNESS.ensureTopics(TEST_TOPIC_NAME);
	TEST_HARNESS.ensureTopics(TEST_TOPIC_NAME2);

	RestIntegrationTestUtil.makeKsqlRequest(REST_APP_NO_SHARED_RUNTIME,
	"CREATE STREAM TEST (ID BIGINT, VALUE decimal(4,1)) WITH (kafka_topic=' " + TEST_TOPIC_NAME + "', value_format='DELIMITED');"
		+ "CREATE TABLE S1 as SELECT ID, sum(value) AS SUM FROM test group by id;"
	);

	RestIntegrationTestUtil.makeKsqlRequest(REST_APP_SHARED_RUNTIME,
	"CREATE STREAM TEST (ID BIGINT, VALUE decimal(4,1)) WITH (kafka_topic=' " + TEST_TOPIC_NAME2 + "', value_format='DELIMITED');"
		+ "CREATE TABLE S2 as SELECT ID, sum(value) AS SUM FROM test group by id;"
		+ "CREATE TABLE S3 as SELECT ID, sum(value) AS SUM FROM test group by id;"
	);
  }

  @Before
  public void setUp() {
	metricsNoSharedRuntime = ((KsqlEngine)REST_APP_NO_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
	metricsSharedRuntime = ((KsqlEngine)REST_APP_NO_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
  }

  @Test
  public void shouldVerifyMetricsOnNonSharedRuntimeServer() {
	// Given:
	final Map<String, String> metricsTagsForQuery = new HashMap<>(METRICS_TAGS);
	final List<String> listOfQueryId = RestIntegrationTestUtil.getQueryIds(REST_APP_NO_SHARED_RUNTIME);
	assertThat(listOfQueryId.size(), equalTo(1));
	metricsTagsForQuery.put("query-id", listOfQueryId.get(0));

	// When:
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME, null, "5,900.1");
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME, null, "5,900.1");
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME, null, "5,900.1");
	System.out.println("records done");

	metricsNoSharedRuntime = ((KsqlEngine)REST_APP_NO_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
	final KafkaMetric restartMetric1 = getKafkaMetric(metricsNoSharedRuntime, metricsTagsForQuery);

	// Then:
	assertThatEventually(() -> (Double) restartMetric1.metricValue(), greaterThan(8.0));
  }

  @Test
  public void shouldVerifyMetricsOnSharedRuntimeServer() throws InterruptedException {
	// Given:
	final Map<String, String> metricsTagsForQuery1 = new HashMap<>(METRICS_TAGS);
	final Map<String, String> metricsTagsForQuery2 = new HashMap<>(METRICS_TAGS);
	final List<String> listOfQueryId = RestIntegrationTestUtil.getQueryIds(REST_APP_SHARED_RUNTIME);
	assertThat(listOfQueryId.size(), equalTo(2));
	for (final String queryId:listOfQueryId) {
	  if (queryId.toLowerCase().contains("s2")) {
		metricsTagsForQuery1.put("query-id", queryId);
	  } else if (queryId.toLowerCase().contains("s3")) {
		metricsTagsForQuery2.put("query-id", queryId);
	  }
	}

	// When:
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME2, null, "5,900.1");
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME2, null, "5,900.1");
	metricsSharedRuntime = ((KsqlEngine)REST_APP_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
	final KafkaMetric restartMetric1 = getKafkaMetric(metricsSharedRuntime, metricsTagsForQuery1);
	final KafkaMetric restartMetric2 = getKafkaMetric(metricsSharedRuntime, metricsTagsForQuery2);

	// Then:
	assertThatEventually(() -> (Double) restartMetric1.metricValue(), greaterThanOrEqualTo(1.0));
	assertThatEventually(() -> (Double) restartMetric2.metricValue(), greaterThanOrEqualTo(1.0));
  }

  private KafkaMetric getKafkaMetric(final Metrics metrics, final Map<String, String> metricsTags) {
	return metrics.metric(new MetricName(
		QueryMetadataImpl.QUERY_RESTART_METRIC_NAME,
		QueryMetadataImpl.QUERY_RESTART_METRIC_GROUP_NAME,
		QueryMetadataImpl.QUERY_RESTART_METRIC_DESCRIPTION,
		metricsTags
	));
  }
}
