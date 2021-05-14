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

package io.confluent.ksql.rest.server.resources.streaming;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;

@SuppressWarnings("UnstableApiUsage")
final class StreamingTestUtils {

  private StreamingTestUtils() { /* private constructor for utility class */ }

  static Iterator<ConsumerRecords<Bytes, Bytes>> generate(
      final String topic,
      final Function<Integer, Bytes> keyMapper,
      final Function<Integer, Bytes> valueMapper) {
    return IntStream.iterate(0, i -> i + 1)
        .mapToObj(
            i -> new ConsumerRecord<>(topic, 0, i, keyMapper.apply(i), valueMapper.apply(i)))
        .map(Lists::newArrayList)
        .map(crs -> (List<ConsumerRecord<Bytes, Bytes>>) crs)
        .map(crs -> ImmutableMap.of(new TopicPartition("topic", 0), crs))
        .map(map -> (Map<TopicPartition, List<ConsumerRecord<Bytes, Bytes>>>) map)
        .map(ConsumerRecords::new)
        .iterator();
  }

  static Iterator<ConsumerRecords<Bytes, Bytes>> partition(
      final Iterator<ConsumerRecords<Bytes, Bytes>> source,
      final int partitions) {
    return Streams.stream(Iterators.partition(source, partitions))
        .map(StreamingTestUtils::combine)
        .iterator();
  }

  private static <K, V> ConsumerRecords<K, V> combine(final List<ConsumerRecords<K,V>> recordsList) {
    final Map<TopicPartition, List<ConsumerRecord<K, V>>> recordMap = new HashMap<>();
    for (final ConsumerRecords<K, V> records : recordsList) {
      for (final TopicPartition tp : records.partitions()) {
        recordMap.computeIfAbsent(tp, ignored -> new ArrayList<>()).addAll(records.records(tp));
      }
    }
    return new ConsumerRecords<>(recordMap);
  }

  static PrintTopic printTopic(
      final String name,
      final boolean fromBeginning,
      final Integer interval,
      final Integer limit) {
    return new PrintTopic(
        Optional.empty(),
        name,
        fromBeginning,
        interval == null ? OptionalInt.empty() : OptionalInt.of(interval),
        limit == null ? OptionalInt.empty() : OptionalInt.of(limit)
    );
  }

  static class TestSubscriber<T> implements Subscriber<T> {

    private final CountDownLatch done = new CountDownLatch(1);
    private final List<T> elements = Lists.newLinkedList();
    private Throwable error = null;
    private LogicalSchema schema = null;
    private Subscription subscription;

    @Override
    public void onNext(final T item) {
      if (done.getCount() == 0) {
        throw new IllegalStateException("already done");
      }
      elements.add(item);
      subscription.request(1);
    }

    @Override
    public void onError(final Throwable e) {
      if (done.getCount() == 0) {
        throw new IllegalStateException("already done");
      }
      error = e;
      done.countDown();
    }

    @Override
    public void onComplete() {
      if (done.getCount() == 0) {
        throw new IllegalStateException("already done");
      }
      done.countDown();
    }

    @Override
    public void onSchema(final LogicalSchema s) {
      if (done.getCount() == 0) {
        throw new IllegalStateException("already done");
      }
      schema = s;
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
      this.subscription = subscription;
      subscription.request(1);
    }

    public Throwable getError() {
      return error;
    }

    public LogicalSchema getSchema() {
      return schema;
    }

    public List<T> getElements() {
      return elements;
    }

    public boolean await() {
      try {
        return done.await(10, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        throw new AssertionError("Test interrupted", e);
      }
    }
  }
}
