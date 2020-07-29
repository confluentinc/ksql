package io.confluent.ksql.rest.integration;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class FaultyKafkaConsumer<K, V> implements ConsumerInterceptor<K, V> {

  public FaultyKafkaConsumer() {}

  public int getPauseOffset() {
    return -1;
  }

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> consumerRecords) {
    int pauseOffset = getPauseOffset();
    if (pauseOffset >= 0) {
      for (TopicPartition topicPartition : consumerRecords.partitions()) {
        List<ConsumerRecord<K, V>> list = consumerRecords.records(topicPartition);
        long offset = list.stream()
            .mapToLong(record -> record.offset())
            .max()
            .orElse(0);
        if (offset >= pauseOffset) {
          for (int updatedPauseOffset = getPauseOffset();
              updatedPauseOffset >= 0 && offset >= updatedPauseOffset;
              updatedPauseOffset = getPauseOffset()) {
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      }
    }
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

  public static class FaultyKafkaConsumer0<K, V> extends FaultyKafkaConsumer<K, V> {
    private static Supplier<Integer> PAUSE_OFFSET = () -> -1;

    public static void setPauseOffset(Supplier<Integer> pauseOffset) {
      PAUSE_OFFSET = pauseOffset;
    }

    public int getPauseOffset() {
      return PAUSE_OFFSET.get();
    }
  }

  public static class FaultyKafkaConsumer1<K, V> extends FaultyKafkaConsumer<K, V> {
    private static Supplier<Integer> PAUSE_OFFSET = () -> -1;

    public static void setPauseOffset(Supplier<Integer> pauseOffset) {
      PAUSE_OFFSET = pauseOffset;
    }

    public int getPauseOffset() {
      return PAUSE_OFFSET.get();
    }
  }

  public static class FaultyKafkaConsumer2<K, V> extends FaultyKafkaConsumer<K, V> {
    private static Supplier<Integer> PAUSE_OFFSET = () -> -1;

    public static void setPauseOffset(Supplier<Integer> pauseOffset) {
      PAUSE_OFFSET = pauseOffset;
    }

    public int getPauseOffset() {
      return PAUSE_OFFSET.get();
    }
  }
}
