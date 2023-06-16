/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.scalablepush.consumer;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
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
        String topicName,
        KafkaConsumer<?, GenericRow> consumer
    );
  }

  public static ConsumerMetadata create(
      final String topicName,
      final KafkaConsumer<?, GenericRow> consumer
  ) {
    final Map<String, List<PartitionInfo>> partitionInfo = consumer.listTopics();
    if (!partitionInfo.containsKey(topicName)) {
      throw new KsqlException("Can't find expected topic " + topicName);
    }
    final int numPartitions = partitionInfo.get(topicName).size();
    return new ConsumerMetadata(numPartitions);
  }
}
