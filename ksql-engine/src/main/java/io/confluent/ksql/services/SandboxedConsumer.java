/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * A limited consumer that can be used while trying out operations.
 *
 * <p>The client will not allow any operation that changes the state of the Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
@SuppressWarnings("deprecation")
class SandboxedConsumer<K, V> implements Consumer<K, V> {

  SandboxedConsumer() {
  }

  @Override
  public Set<TopicPartition> assignment() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> subscription() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void subscribe(final Collection<String> topics) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void subscribe(final Pattern pattern, final ConsumerRebalanceListener callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void subscribe(final Pattern pattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void assign(final Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unsubscribe() {
    // No-op
  }

  @Override
  public ConsumerRecords<K, V> poll(final long timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConsumerRecords<K, V> poll(final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitSync() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitSync(final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets,
      final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitAsync() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitAsync(final OffsetCommitCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
      final OffsetCommitCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seek(final TopicPartition partition, final long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekToBeginning(final Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekToEnd(final Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long position(final TopicPartition partition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long position(final TopicPartition partition, final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OffsetAndMetadata committed(final TopicPartition partition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OffsetAndMetadata committed(final TopicPartition partition, final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PartitionInfo> partitionsFor(final String topic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PartitionInfo> partitionsFor(final String topic, final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<TopicPartition> paused() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void pause(final Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resume(final Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      final Map<TopicPartition, Long> timestampsToSearch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      final Map<TopicPartition, Long> timestampsToSearch, final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions,
      final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions,
      final Duration timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // No-op
  }

  @Override
  public void close(final long timeout, final TimeUnit unit) {
    // No-op
  }

  @Override
  public void close(final Duration timeout) {
    // No-op
  }

  @Override
  public void wakeup() {
    // No-op
  }
}
