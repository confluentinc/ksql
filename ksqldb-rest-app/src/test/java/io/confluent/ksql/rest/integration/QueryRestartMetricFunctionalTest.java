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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
  private static final String TEST_TOPIC_NAME3 = "test-topic-3";
  private static final String TEST_TOPIC_NAME4 = "test-topic-4";
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
	  .withProperty(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS, 3000L)
	  .build();

  private static final TestKsqlRestApp REST_APP_SHARED_RUNTIME = TestKsqlRestApp
	  .builder(TEST_HARNESS::kafkaBootstrapServers)
	  .withProperty(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS, METRICS_TAGS_STRING)
	  .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
	  .withProperty(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, true)
	  .withProperty(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "another-id")
	  .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP_NO_SHARED_RUNTIME).around(REST_APP_SHARED_RUNTIME);

  private Metrics metricsNoSharedRuntime;
  private Metrics metricsSharedRuntime;

  @BeforeClass
  public static void setUpClass() throws InterruptedException {
	TEST_HARNESS.ensureTopics(TEST_TOPIC_NAME);
	TEST_HARNESS.ensureTopics(TEST_TOPIC_NAME2);
	TEST_HARNESS.ensureTopics(TEST_TOPIC_NAME3);
	TEST_HARNESS.ensureTopics(TEST_TOPIC_NAME4);

	RestIntegrationTestUtil.makeKsqlRequest(REST_APP_NO_SHARED_RUNTIME,
		"CREATE STREAM test_stream (f BIGINT) with (KAFKA_TOPIC='" + TEST_TOPIC_NAME + "', VALUE_FORMAT='json');"
			+ "CREATE STREAM test_stream2 (f BIGINT) with (KAFKA_TOPIC='" + TEST_TOPIC_NAME2 + "', VALUE_FORMAT='json');"
	);
	RestIntegrationTestUtil.makeKsqlRequest(REST_APP_NO_SHARED_RUNTIME,
		"create stream test_addition_5 as select f+5 from test_stream;"
			+ "create stream test_addition_10 as select f+10 from test_stream;"
			+ "create stream test_addition_20 as select f+20 from test_stream2;"
	);

	RestIntegrationTestUtil.makeKsqlRequest(REST_APP_SHARED_RUNTIME,
		"CREATE STREAM test_stream (f BIGINT) with (KAFKA_TOPIC='" + TEST_TOPIC_NAME3 + "', VALUE_FORMAT='json');"
			+ "CREATE STREAM test_stream2 (f BIGINT) with (KAFKA_TOPIC='" + TEST_TOPIC_NAME4 + "', VALUE_FORMAT='json');"
	);
  }

  @Before
  public void setUp() {
	metricsNoSharedRuntime = ((KsqlEngine)REST_APP_NO_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
	metricsSharedRuntime = ((KsqlEngine)REST_APP_NO_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
  }

  @Test
  public void shouldVerifyMetricsOnNonSharedRuntimeServer() throws InterruptedException {
	// Given:
	final Map<String, String> metricsTagsForQuery1 = new HashMap<>(METRICS_TAGS);
	final Map<String, String> metricsTagsForQuery2 = new HashMap<>(METRICS_TAGS);
	final Map<String, String> metricsTagsForQuery3 = new HashMap<>(METRICS_TAGS);
	final List<String> listOfQueryId = RestIntegrationTestUtil.getQueryIds(REST_APP_NO_SHARED_RUNTIME);
	assertThat(listOfQueryId.size(), equalTo(3));
	for (final String queryId:listOfQueryId) {
	  if (queryId.toLowerCase().contains("test_addition_5")) {
		metricsTagsForQuery1.put("query_id", queryId);
	  } else if (queryId.toLowerCase().contains("test_addition_10")) {
		metricsTagsForQuery2.put("query_id", queryId);
	  } else if (queryId.toLowerCase().contains("test_addition_20")) {
		metricsTagsForQuery3.put("query_id", queryId);
	  }
	}

	TEST_HARNESS.deleteTopics(Collections.singletonList(TEST_TOPIC_NAME));
	Thread.sleep(10000);
	REST_APP_NO_SHARED_RUNTIME.stop();

	// When:
	REST_APP_NO_SHARED_RUNTIME.start();
	Thread.sleep(15000);

	metricsNoSharedRuntime = ((KsqlEngine)REST_APP_NO_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
	final KafkaMetric restartMetric1 = getKafkaMetric(metricsNoSharedRuntime, metricsTagsForQuery1);
	final KafkaMetric restartMetric2 = getKafkaMetric(metricsNoSharedRuntime, metricsTagsForQuery2);
	final KafkaMetric restartMetric3 = getKafkaMetric(metricsNoSharedRuntime, metricsTagsForQuery3);

	// Then:
	assertThatEventually(() -> (Double) restartMetric1.metricValue(), greaterThan(12.0));
	assertThatEventually(() -> (Double) restartMetric2.metricValue(), greaterThan(12.0));
	assertThat(restartMetric3.metricValue(), equalTo(0.0));
  }

  @Ignore
  @Test
  public void shouldVerifyMetricsOnSharedRuntimeServer() throws InterruptedException {
// Given:
	final Map<String, String> metricsTagsForQuery1 = new HashMap<>(METRICS_TAGS);
	final Map<String, String> metricsTagsForQuery2 = new HashMap<>(METRICS_TAGS);
	final Map<String, String> metricsTagsForQuery3 = new HashMap<>(METRICS_TAGS);
	final List<String> listOfQueryId = RestIntegrationTestUtil.getQueryIds(REST_APP_SHARED_RUNTIME);
	assertThat(listOfQueryId.size(), equalTo(3));
	for (final String queryId:listOfQueryId) {
	  if (queryId.toLowerCase().contains("test_addition_5")) {
		metricsTagsForQuery1.put("query_id", queryId);
	  } else if (queryId.toLowerCase().contains("test_addition_10")) {
		metricsTagsForQuery2.put("query_id", queryId);
	  } else if (queryId.toLowerCase().contains("test_addition_20")) {
		metricsTagsForQuery3.put("query_id", queryId);
	  }
	}

	TEST_HARNESS.deleteTopics(Collections.singletonList(TEST_TOPIC_NAME3));
	Thread.sleep(15000);
	REST_APP_SHARED_RUNTIME.stop();
	Thread.sleep(30000);
	System.out.println("steven starting server up again");

	// When:
	REST_APP_SHARED_RUNTIME.start();
	Thread.sleep(20000);

	metricsSharedRuntime = ((KsqlEngine)REST_APP_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
	final KafkaMetric restartMetric1 = getKafkaMetric(metricsSharedRuntime, metricsTagsForQuery1);
	final KafkaMetric restartMetric2 = getKafkaMetric(metricsSharedRuntime, metricsTagsForQuery2);
	final KafkaMetric restartMetric3 = getKafkaMetric(metricsSharedRuntime, metricsTagsForQuery3);

	// Then:
	assertThatEventually(() -> (Double) restartMetric1.metricValue(), greaterThan(12.0));
	assertThatEventually(() -> (Double) restartMetric2.metricValue(), greaterThan(12.0));
	assertThat(restartMetric3.metricValue(), equalTo(0.0));
  }

  private KafkaMetric getKafkaMetric(final Metrics metrics, final Map<String, String> metricsTags) {
	final MetricName restartMetricName3 = new MetricName(
		"query-restart-total",
		"query-restart-metrics",
		"The total number of times that a query thread has failed and then been restarted.",
		metricsTags
	);
	return metrics.metric(restartMetricName3);
  }
}
