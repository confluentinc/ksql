/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.metrics;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.SystemTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConsumerCollectorTest {

  private static final String TEST_TOPIC = "testtopic";
  @BeforeClass
  public static void setUp() {
    MetricCollectors.initialize();
    ConsumerCollector.configureTotalBytesSum(MetricCollectors.getMetrics());;
  }

  @AfterClass
  public static void tearDown() {
    MetricCollectors.cleanUp();
  }

  @Test
  public void shouldDisplayRateThroughput() {

    final ConsumerCollector collector = new ConsumerCollector();
    collector.configure(new Metrics(), "group", new SystemTime());

    for (int i = 0; i < 100; i++){

      final Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
              new TopicPartition(TEST_TOPIC, 1), Arrays.asList(
                      new ConsumerRecord<>(TEST_TOPIC, 1, i, 1L, TimestampType.CREATE_TIME, 1L, 10, 10, "key", "1234567890")) );
      final ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);

      collector.onConsume(consumerRecords);
    }

    final Collection<TopicSensors.Stat> stats = collector.stats(TEST_TOPIC, false);
    assertNotNull(stats);

    assertThat( stats.toString(), containsString("name=consumer-messages-per-sec,"));
    assertThat( stats.toString(), containsString("total-messages, value=100.0"));
  }

  @Test
  public void shouldDisplayByteThroughputAcrossAllTopics() {

    final ConsumerCollector collector = new ConsumerCollector();
    final Metrics metrics = MetricCollectors.getMetrics();
    collector.configure(metrics, "group", new SystemTime());

    for (int i = 0; i < 100; i++){

      final Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records1 = ImmutableMap.of(
          new TopicPartition(TEST_TOPIC, 1), Collections.singletonList(
              new ConsumerRecord<>(TEST_TOPIC, 1, i, 1L, TimestampType.CREATE_TIME, 1L, 10, 10,
                  "key", "1234567890")));

      final Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records2 = ImmutableMap.of(
          new TopicPartition(TEST_TOPIC, 1), Collections.singletonList(
              new ConsumerRecord<>(TEST_TOPIC, 1, i, 1L, TimestampType.CREATE_TIME, 1L, 10, 10,
                  "key", "1234567890")));

      final ConsumerRecords<Object, Object> consumerRecords1 = new ConsumerRecords<>(records1);
      final ConsumerRecords<Object, Object> consumerRecords2 = new ConsumerRecords<>(records2);

      collector.onConsume(consumerRecords1);
      collector.onConsume(consumerRecords2);
    }

    final MetricName metricName = metrics.metricName(
            ConsumerCollector.CONSUMER_ALL_TOTAL_BYTES_SUM,
            ConsumerCollector.CONSUMER_COLLECTOR_METRICS_GROUP_NAME);
    final KafkaMetric metric = metrics.metric(metricName);
    final Object metricValue = metric.metricValue();
    final String metricValueAsString = metricValue.toString();
    assertThat(Double.parseDouble(metricValueAsString), equalTo(4000.0));
  }
}
