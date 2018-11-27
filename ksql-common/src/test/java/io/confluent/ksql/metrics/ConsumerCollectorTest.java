/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.metrics;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.SystemTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

public class ConsumerCollectorTest {

  private static final String TEST_TOPIC = "testtopic";

  @Test
  public void shouldDisplayRateThroughput() {

    final ConsumerCollector collector = new ConsumerCollector();//
    collector.configure(new Metrics(), "group", new SystemTime());

    for (int i = 0; i < 100; i++){

      final Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
              new TopicPartition(TEST_TOPIC, 1), Arrays.asList(
                      new ConsumerRecord<>(TEST_TOPIC, 1, i,  1l, TimestampType.CREATE_TIME,  1l, 10, 10, "key", "1234567890")) );
      final ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);

      collector.onConsume(consumerRecords);
    }

    final Collection<TopicSensors.Stat> stats = collector.stats(TEST_TOPIC, false);
    assertNotNull(stats);

    assertThat( stats.toString(), containsString("name=consumer-messages-per-sec,"));
    assertThat( stats.toString(), containsString("total-messages, value=100.0"));
  }
}
