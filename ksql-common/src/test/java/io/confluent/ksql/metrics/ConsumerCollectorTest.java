package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.SystemTime;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class ConsumerCollectorTest {

  private static final String TEST_TOPIC = "testtopic";

  @Test
  public void shouldDisplayRateThroughput() throws Exception {

    ConsumerCollector collector = new ConsumerCollector();//
    collector.configure(new Metrics(), "group", new SystemTime());

    for (int i = 0; i < 100; i++){

      Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
              new TopicPartition(TEST_TOPIC, 1), Arrays.asList(
                      new ConsumerRecord<>(TEST_TOPIC, 1, i,  1l, TimestampType.CREATE_TIME,  1l, 10, 10, "key", "1234567890")) );
      ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);

      collector.onConsume(consumerRecords);
    }

    Collection<TopicSensors.Stat> stats = collector.stats(TEST_TOPIC, false);
    assertNotNull(stats);

    assertThat( stats.toString(), containsString("name=consumer-messages-per-sec,"));
    assertThat( stats.toString(), containsString("total-messages, value=100.0"));
  }
}
