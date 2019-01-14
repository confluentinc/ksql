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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

/**
 * A limited producer that can be used while trying out operations.
 *
 * <p>The client will not allow any operation that changes the state of the Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
class TryProducer<K, V> implements Producer<K, V> {

  // Todo(ac): tidy up & test.  Only close?

  private final Producer<byte[], byte[]> delegate;

  TryProducer(final Producer<byte[], byte[]> delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public void initTransactions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendOffsetsToTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
      final String consumerGroupId) throws ProducerFencedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<RecordMetadata> send(final ProducerRecord<K, V> record) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<RecordMetadata> send(final ProducerRecord<K, V> record, final Callback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PartitionInfo> partitionsFor(final String topic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void close(final long timeout, final TimeUnit unit) {
    delegate.close(timeout, unit);
  }
}
