package io.confluent.ksql.rest.integration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaInterceptor<K, V> implements ConsumerInterceptor<K, V> {

  static Supplier<Boolean> PAUSE = () -> false;
  Map<TopicPartition, List<ConsumerRecord<K, V>>> recordsMap = new HashMap<>();

  public KafkaInterceptor() {
    System.out.println("blah");
  }

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> consumerRecords) {
    if (PAUSE.get()) {
      return ConsumerRecords.empty();
    }
//    for (TopicPartition partition : consumerRecords.partitions()) {
//      List<ConsumerRecord<K, V>> records = consumerRecords.records(partition);
//      recordsMap.put(partition, records);
//    }
    return consumerRecords;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(Map<String, ?> map) {
  }
}
