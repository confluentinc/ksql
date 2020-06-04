package io.confluent.ksql.services;

import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.services.KafkaConsumerGroupClient.ConsumerGroupSummary;
import io.confluent.ksql.util.LimitedProxyBuilder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Methods invoked via reflection.
@SuppressWarnings("unused")  // Methods invoked via reflection.
public class SandboxedKafkaConsumerGroupClient {

  static KafkaConsumerGroupClient createProxy(final KafkaConsumerGroupClient delegate) {
    final SandboxedKafkaConsumerGroupClient sandbox = new SandboxedKafkaConsumerGroupClient(delegate);

    return LimitedProxyBuilder.forClass(KafkaConsumerGroupClient.class)
        .forward("describeConsumerGroup", methodParams(String.class), sandbox)
        .forward("listGroups", methodParams(), sandbox)
        .forward("listConsumerGroupOffsets", methodParams(String.class), sandbox)
        .build();
  }

  private final KafkaConsumerGroupClient delegate;

  private SandboxedKafkaConsumerGroupClient(final KafkaConsumerGroupClient delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }



  public ConsumerGroupSummary describeConsumerGroup(final String groupId) {
    return delegate.describeConsumerGroup(groupId);
  }

  public List<String> listGroups() {
    return delegate.listGroups();
  }

  public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String group) {
    return delegate.listConsumerGroupOffsets(group);
  }
}
