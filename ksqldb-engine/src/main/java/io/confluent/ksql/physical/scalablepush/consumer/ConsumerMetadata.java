package io.confluent.ksql.physical.scalablepush.consumer;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerMetadata implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerMetadata.class);

  private final int numPartitions;

  public ConsumerMetadata(final int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  @Override
  public void close() {
  }

  public interface ConsumerMetadataFactory {
    ConsumerMetadata create(
        final String topicName,
        final KafkaConsumer<?, GenericRow> consumer
    );
  }

  public static ConsumerMetadata create(
      final String topicName,
      final KafkaConsumer<?, GenericRow> consumer
  ) {
    Map<String, List<PartitionInfo>> partitionInfo = consumer.listTopics();
    while (!partitionInfo.containsKey(topicName)) {
      try {
        Thread.sleep(100);
        partitionInfo = consumer.listTopics();
      } catch (InterruptedException e) {
        LOG.error("Interrupted while looking for topic", e);
        Thread.currentThread().interrupt();
      }
    }
    int numPartitions = partitionInfo.get(topicName).size();
    return new ConsumerMetadata(numPartitions);
  }
}
