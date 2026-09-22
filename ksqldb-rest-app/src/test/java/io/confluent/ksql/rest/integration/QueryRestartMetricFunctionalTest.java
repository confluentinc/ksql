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
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.internal.QueryStateMetricsReportingListener;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;

@Ignore
public class QueryRestartMetricFunctionalTest {

  private static final String TEST_TOPIC_NAME = "test";
  private static final ImmutableMap<String, String> METRICS_TAGS = ImmutableMap.of(
	  "cluster.id", "cluster-1"
  );
  private static final String METRICS_TAGS_STRING = "cluster.id:cluster-1";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP_NO_SHARED_RUNTIME = TestKsqlRestApp
	  .builder(TEST_HARNESS::kafkaBootstrapServers)
	  .withProperty(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS, METRICS_TAGS_STRING)
	  .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
	  .withProperty(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS, 0L)
	  .withProperty(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS, 300L)
	  .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP_NO_SHARED_RUNTIME);

  private Metrics metricsNoSharedRuntime;

  @BeforeClass
  public static void setUpClass() throws InterruptedException {
	TEST_HARNESS.ensureTopics(TEST_TOPIC_NAME);

	RestIntegrationTestUtil.makeKsqlRequest(REST_APP_NO_SHARED_RUNTIME,
	"CREATE STREAM TEST (ID BIGINT, VALUE decimal(4,1)) WITH (kafka_topic=' " + TEST_TOPIC_NAME + "', value_format='DELIMITED');"
		+ "CREATE TABLE S1 as SELECT ID, sum(value) AS SUM FROM test group by id;"
	);
  }

  @Before
  public void setUp() {
	metricsNoSharedRuntime = ((KsqlEngine)REST_APP_NO_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
  }

  @Test
  public void shouldVerifyMetricsOnNonSharedRuntimeServer() {
	// Given:
	final Map<String, String> metricsTagsForQuery = new HashMap<>(METRICS_TAGS);
	final List<String> listOfQueryId = RestIntegrationTestUtil.getQueryIds(REST_APP_NO_SHARED_RUNTIME);
	assertThat(listOfQueryId.size(), equalTo(1));
	metricsTagsForQuery.put("query-id", listOfQueryId.get(0));
	metricsTagsForQuery.put("ksql_service_id", KsqlConfig.KSQL_SERVICE_ID_DEFAULT);

	// When:
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME, null, "5,900.1");
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME, null, "5,900.1");
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME, null, "5,900.1");

	metricsNoSharedRuntime = ((KsqlEngine)REST_APP_NO_SHARED_RUNTIME.getEngine()).getEngineMetrics().getMetrics();
	final KafkaMetric restartMetric = getKafkaMetric(metricsNoSharedRuntime, metricsTagsForQuery, "app-0-");

	// Then:
	assertThatEventually(() -> (Double) restartMetric.metricValue(), greaterThan(8.0));

	// should clean up metrics when queries are terminated
	for (final String queryId:listOfQueryId) {
	  RestIntegrationTestUtil.makeKsqlRequest(REST_APP_NO_SHARED_RUNTIME, "terminate " + queryId + ";");
	}

	final KafkaMetric restartMetricAfterTerminate = getKafkaMetric(metricsNoSharedRuntime, metricsTagsForQuery, "app-0-");
	assertThat(restartMetricAfterTerminate, nullValue());
  }

  private KafkaMetric getKafkaMetric(final Metrics metrics, final Map<String, String> metricsTags, final String metricPrefix) {
	return metrics.metric(new MetricName(
		QueryStateMetricsReportingListener.QUERY_RESTART_METRIC_NAME,
		metricPrefix + "ksql-queries",
		QueryStateMetricsReportingListener.QUERY_RESTART_METRIC_DESCRIPTION,
		metricsTags
	));
  }
}
