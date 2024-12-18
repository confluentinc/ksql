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
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.logging.processing.MeteredProcessingLoggerFactory;
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
public class ProcessingLogErrorMetricFunctionalTest {

  private static final String TEST_TOPIC_NAME = "test";
  private static final String TEST_TOPIC_NAME2 = "test-topic";
  private static final ImmutableMap<String, String> METRICS_TAGS = ImmutableMap.of(
	  "cluster.id", "cluster-1"
  );

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
	  .builder(TEST_HARNESS::kafkaBootstrapServers)
	  .withProperty(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS, "cluster.id:cluster-1")
	  .withProperty("auto.offset.reset", "earliest")
	  .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  @Rule
  public final Timeout timeout = Timeout.seconds(60);

  private Metrics metrics;

  @BeforeClass
  public static void setUpClass() {
	TEST_HARNESS.ensureTopics(TEST_TOPIC_NAME);
	TEST_HARNESS.ensureTopics(TEST_TOPIC_NAME2);

	RestIntegrationTestUtil.makeKsqlRequest(REST_APP,
		"CREATE STREAM test_stream (f BIGINT) with (KAFKA_TOPIC='" + TEST_TOPIC_NAME + "', VALUE_FORMAT='json');"
			+ "CREATE STREAM test_stream2 (f BIGINT) with (KAFKA_TOPIC='" + TEST_TOPIC_NAME2 + "', VALUE_FORMAT='json');"
	);
	RestIntegrationTestUtil.makeKsqlRequest(REST_APP,
		"create stream test_addition_10 as select f+10 from test_stream;"
			+ "create stream test_addition_20 as select f+20 from test_stream2;"
	);
  }

  @Before
  public void setUp() {
	metrics = ((KsqlEngine)REST_APP.getEngine()).getEngineMetrics().getMetrics();
  }

  @Test
  public void shouldVerifyMetrics() {

	// Given:
	final Map<String, String> metricsTagsForQuery1DeserializerLog = new HashMap<>(METRICS_TAGS);
	final Map<String, String> metricsTagsForQuery2ProjectLog = new HashMap<>(METRICS_TAGS);

	final List<String> listOfQueryId = RestIntegrationTestUtil.getQueryIds(REST_APP);
	assertThat(listOfQueryId.size(), equalTo(2));
	for (final String queryId:listOfQueryId) {
	  if (queryId.toLowerCase().contains("test_addition_10")) {
		metricsTagsForQuery1DeserializerLog.put("query-id", queryId);
		metricsTagsForQuery1DeserializerLog.put("logger-id", queryId + ".KsqlTopic.Source.deserializer");
	  } else if (queryId.toLowerCase().contains("test_addition_20")) {
		metricsTagsForQuery2ProjectLog.put("query-id", queryId);
		metricsTagsForQuery2ProjectLog.put("logger-id", queryId + ".Project");
	  }
	}

	RestIntegrationTestUtil.makeKsqlRequest(REST_APP,
		"show queries;"
	);
	final MetricName processingLogErrorMetricName1 = new MetricName(
		MeteredProcessingLoggerFactory.PROCESSING_LOG_ERROR_METRIC_NAME,
		MeteredProcessingLoggerFactory.PROCESSING_LOG_METRICS_GROUP_NAME,
		MeteredProcessingLoggerFactory.PROCESSING_LOG_METRIC_DESCRIPTION,
		metricsTagsForQuery1DeserializerLog
	);
	final KafkaMetric processingLogErrorMetric1 = metrics.metric(processingLogErrorMetricName1);

	final MetricName processingLogErrorMetricName2 = new MetricName(
		MeteredProcessingLoggerFactory.PROCESSING_LOG_ERROR_METRIC_NAME,
		MeteredProcessingLoggerFactory.PROCESSING_LOG_METRICS_GROUP_NAME,
		MeteredProcessingLoggerFactory.PROCESSING_LOG_METRIC_DESCRIPTION,
		metricsTagsForQuery2ProjectLog
	);
	final KafkaMetric processingLogErrorMetric2 = metrics.metric(processingLogErrorMetricName2);

	// When:

	// Bad records
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME, null, "{\"f\":\"string_value\"}");
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME, null, "{\"f\":5");
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME2, null, "{\"f\": null}");

	// Then:
	assertThatEventually(() -> (Double) processingLogErrorMetric1.metricValue(), equalTo(2.0));
	assertThatEventually(() -> (Double) processingLogErrorMetric2.metricValue(), equalTo(1.0));

	// Good records shouldn't change metrics
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME, null, "{\"f\":5}");
	TEST_HARNESS.produceRecord(TEST_TOPIC_NAME2, null, "{\"f\":5}");
	assertThatEventually(() -> (Double) processingLogErrorMetric1.metricValue(), equalTo(2.0));
	assertThatEventually(() -> (Double) processingLogErrorMetric2.metricValue(), equalTo(1.0));

	// should clean up metrics when queries are terminated
	for (final String queryId:listOfQueryId) {
	  RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "terminate " + queryId + ";");
	}

	final KafkaMetric cleanedUpMetric1 = metrics.metric(processingLogErrorMetricName1);
	final KafkaMetric cleanedUpMetric2 = metrics.metric(processingLogErrorMetricName2);
	assertThat(cleanedUpMetric1, nullValue());
	assertThat(cleanedUpMetric2, nullValue());
  }
}
