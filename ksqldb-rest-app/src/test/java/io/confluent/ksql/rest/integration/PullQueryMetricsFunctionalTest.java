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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
public class PullQueryMetricsFunctionalTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();
  private static final Format KEY_FORMAT = FormatFactory.KAFKA;
  private static final Format VALUE_FORMAT = FormatFactory.JSON;
  private static final String AGG_TABLE = "AGG_TABLE";
  private static final String AN_AGG_KEY = "USER_1";
  private static final String A_STREAM_KEY = "PAGE_1";

  private static final PhysicalSchema AGGREGATE_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
          .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
          .build(),
      SerdeFeatures.of(),
      SerdeFeatures.of()
  );

  private static final ImmutableMap<String, String> TABLE_TAGS = ImmutableMap.of(
      "ksql_service_id", "default_",
      "query_source", "non_windowed",
      "query_plan_type", "key_lookup",
      "query_routing_type", "source_node"
  );

  private static final ImmutableMap<String, String> STREAMS_TAGS = ImmutableMap.of(
      "ksql_service_id", "default_",
      "query_source", "non_windowed_stream",
      "query_plan_type", "unknown",
      "query_routing_type", "source_node"
  );

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED, true)
      .withProperty("auto.offset.reset", "earliest")
      .build();

  private static final MetricName recordsReturnedTable = new MetricName(
      "pull-query-requests-rows-returned-total",
      "_confluent-ksql-pull-query",
      "Number of rows returned - non_windowed-key_lookup-source_node",
      TABLE_TAGS
  );

  private static final MetricName latencyTable = new MetricName(
      "pull-query-requests-detailed-latency-min",
      "_confluent-ksql-pull-query",
      "Min time for a pull query request - non_windowed-key_lookup-source_node",
      TABLE_TAGS
  );

  private static final MetricName responseSizeTable = new MetricName(
      "pull-query-requests-detailed-response-size",
      "_confluent-ksql-pull-query",
      "Size in bytes of pull query response - non_windowed-key_lookup-source_node",
      TABLE_TAGS
  );

  private static final MetricName totalRequestsTable = new MetricName(
      "pull-query-requests-detailed-total",
      "_confluent-ksql-pull-query",
      "Total number of pull query request - non_windowed-key_lookup-source_node",
      TABLE_TAGS
  );

  private static final MetricName requestDistributionTable = new MetricName(
      "pull-query-requests-detailed-distribution-90",
      "_confluent-ksql-pull-query",
      "Latency distribution - non_windowed-key_lookup-source_node",
      TABLE_TAGS
  );

  private static final MetricName recordsReturnedStream = new MetricName(
      "pull-query-requests-rows-returned-total",
      "_confluent-ksql-pull-query",
      "Number of rows returned - non_windowed_stream-unknown-source_node",
      STREAMS_TAGS
  );

  private static final MetricName latencyStream = new MetricName(
      "pull-query-requests-detailed-latency-min",
      "_confluent-ksql-pull-query",
      "Min time for a pull query request - non_windowed_stream-unknown-source_node",
      STREAMS_TAGS
  );

  private static final MetricName responseSizeStream = new MetricName(
      "pull-query-requests-detailed-response-size",
      "_confluent-ksql-pull-query",
      "Size in bytes of pull query response - non_windowed_stream-unknown-source_node",
      STREAMS_TAGS
  );

  private static final MetricName totalRequestsStream = new MetricName(
      "pull-query-requests-detailed-total",
      "_confluent-ksql-pull-query",
      "Total number of pull query request - non_windowed_stream-unknown-source_node",
      STREAMS_TAGS
  );

  private static final MetricName requestDistributionStream = new MetricName(
      "pull-query-requests-detailed-distribution-90",
      "_confluent-ksql-pull-query",
      "Latency distribution - non_windowed_stream-unknown-source_node",
      TABLE_TAGS
  );

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  @Rule
  public final Retry retry = Retry.of(5, AssertionError.class, 3, TimeUnit.SECONDS);

  @Rule
  public final Timeout timeout = Timeout.seconds(60);

  private Metrics metrics;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.KAFKA, FormatFactory.JSON);

    RestIntegrationTestUtil.createStream(REST_APP, PAGE_VIEWS_PROVIDER);

    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT USERID, COUNT(1) AS COUNT FROM " + PAGE_VIEW_STREAM + " GROUP BY USERID;"
    );

    TEST_HARNESS.verifyAvailableUniqueRows(
        AGG_TABLE,
        5,
        KEY_FORMAT,
        VALUE_FORMAT,
        AGGREGATE_SCHEMA
    );

    TEST_HARNESS.verifyAvailableUniqueRows(
        PAGE_VIEW_TOPIC,
        5,
        KEY_FORMAT,
        VALUE_FORMAT,
        AGGREGATE_SCHEMA
    );
  }

  @Before
  public void setUp() {
    metrics = ((KsqlEngine)REST_APP.getEngine()).getEngineMetrics().getMetrics();
  }

  @Test
  public void shouldVerifyMetrics() {

    // Given:
    final KafkaMetric recordsReturnedTableMetric = metrics.metric(recordsReturnedTable);
    final KafkaMetric latencyTableMetric = metrics.metric(latencyTable);
    final KafkaMetric responseSizeTableMetric = metrics.metric(responseSizeTable);
    final KafkaMetric totalRequestsTableMetric = metrics.metric(totalRequestsTable);
    final KafkaMetric requestDistributionTableMetric = metrics.metric(requestDistributionTable);

    final KafkaMetric recordsReturnedStreamMetric = metrics.metric(recordsReturnedStream);
    final KafkaMetric latencyStreamMetric = metrics.metric(latencyStream);
    final KafkaMetric responseSizeStreamMetric = metrics.metric(responseSizeStream);
    final KafkaMetric totalRequestsStreamMetric = metrics.metric(totalRequestsStream);
    final KafkaMetric requestDistributionStreamMetric = metrics.metric(requestDistributionStream);

    // When:
    final List<StreamedRow> tableRows = RestIntegrationTestUtil.makeQueryRequest(
        REST_APP,
        "SELECT COUNT, USERID from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
        Optional.empty());

    assertThat(tableRows.size(), is(2));

    final List<StreamedRow> streamRows = RestIntegrationTestUtil.makeQueryRequest(
        REST_APP,
        "SELECT * from " + PAGE_VIEW_STREAM + " WHERE PAGEID='" + A_STREAM_KEY + "';",
        Optional.empty());

    assertThat(streamRows.size(), is(2));

    // Then:
    assertThat(recordsReturnedTableMetric.metricValue(), is(1.0));
    assertThat((Double)latencyTableMetric.metricValue(), greaterThan(1.0));
    assertThat((Double)responseSizeTableMetric.metricValue(), greaterThan(1.0));
    assertThat(totalRequestsTableMetric.metricValue(), is(1.0));
    assertThat((Double)requestDistributionTableMetric.metricValue(), greaterThan(1.0));

    assertThat(recordsReturnedStreamMetric.metricValue(), is(1.0));
    assertThat((Double)latencyStreamMetric.metricValue(), greaterThan(1.0));
    assertThat((Double)responseSizeStreamMetric.metricValue(), greaterThan(1.0));
    assertThat(totalRequestsStreamMetric.metricValue(), is(1.0));
    assertThat((Double)requestDistributionStreamMetric.metricValue(), greaterThan(1.0));
  }
}
