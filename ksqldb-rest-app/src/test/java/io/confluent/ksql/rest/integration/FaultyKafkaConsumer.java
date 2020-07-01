package io.confluent.ksql.rest.integration;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class FaultyKafkaConsumer<K, V> implements ConsumerInterceptor<K, V> {
  public FaultyKafkaConsumer() {}

  public boolean isDisabled() {
    return false;
  }

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> consumerRecords) {
    if (isDisabled()) {
      return ConsumerRecords.empty();
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
    private static Supplier<Boolean> DISABLE = () -> false;

    public static void setDisable(Supplier<Boolean> disable) {
      DISABLE = disable;
    }

    public boolean isDisabled() {
      return DISABLE.get();
    }
  }

  public static class FaultyKafkaConsumer1<K, V> extends FaultyKafkaConsumer<K, V> {
    private static Supplier<Boolean> DISABLE = () -> false;

    public static void setDisable(Supplier<Boolean> disable) {
      DISABLE = disable;
    }

    public boolean isDisabled() {
      return DISABLE.get();
    }
  }

  public static class FaultyKafkaConsumer2<K, V> extends FaultyKafkaConsumer<K, V> {
    private static Supplier<Boolean> DISABLE = () -> false;

    public static void setDisable(Supplier<Boolean> disable) {
      DISABLE = disable;
    }

    public boolean isDisabled() {
      return DISABLE.get();
    }
  }
}
