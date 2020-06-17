package io.confluent.ksql.rest.integration;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class FaultyKafka<K, V> implements ConsumerInterceptor<K, V> {
  public FaultyKafka() {
    System.out.println("blah");
  }

  public boolean isDisabled() {
    return false;
  }

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> consumerRecords) {
    System.out.println("Got onConsume " + this.getClass().getName() + " with pause " + isDisabled());
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

  public static class FaultyKafka0<K, V> extends FaultyKafka<K, V> {
    public static Supplier<Boolean> DISABLE = () -> false;

    public boolean isDisabled() {
      return DISABLE.get();
    }
  }

  public static class FaultyKafka1<K, V> extends FaultyKafka<K, V> {
    public static Supplier<Boolean> DISABLE = () -> false;

    public boolean isDisabled() {
      return DISABLE.get();
    }
  }

  public static class FaultyKafka2<K, V> extends FaultyKafka<K, V> {
    public static Supplier<Boolean> DISABLE = () -> false;

    public boolean isDisabled() {
      return DISABLE.get();
    }
  }
}
