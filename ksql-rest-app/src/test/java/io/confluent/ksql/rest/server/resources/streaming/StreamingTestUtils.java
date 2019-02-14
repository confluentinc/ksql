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

package io.confluent.ksql.rest.server.resources.streaming;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Schema;

class StreamingTestUtils {

  private StreamingTestUtils() { /* private constructor for utility class */ }

  static Iterator<ConsumerRecords<String, Bytes>> generate(
      final String topic,
      final Function<Integer, String> keyMapper,
      final Function<Integer, Bytes> valueMapper) {
    return IntStream.iterate(0, i -> i + 1)
        .mapToObj(
            i -> new ConsumerRecord<>(topic, 0, i, keyMapper.apply(i), valueMapper.apply(i)))
        .map(Lists::newArrayList)
        .map(crs -> (List<ConsumerRecord<String, Bytes>>) crs)
        .map(crs -> ImmutableMap.of(new TopicPartition("topic", 0), crs))
        .map(map -> (Map<TopicPartition, List<ConsumerRecord<String, Bytes>>>) map)
        .map(ConsumerRecords::new)
        .iterator();
  }

  static Iterator<ConsumerRecords<String, Bytes>> partition(
      final Iterator<ConsumerRecords<String, Bytes>> source,
      final int partitions) {
    return Streams.stream(Iterators.partition(source, partitions))
        .map(StreamingTestUtils::combine)
        .iterator();
  }

  private static <K, V> ConsumerRecords<K, V> combine(List<ConsumerRecords<K,V>> recordsList) {
    final Map<TopicPartition, List<ConsumerRecord<K, V>>> recordMap = new HashMap<>();
    for (ConsumerRecords<K, V> records : recordsList) {
      for (TopicPartition tp : records.partitions()) {
        recordMap.computeIfAbsent(tp, ignored -> new ArrayList<>()).addAll(records.records(tp));
      }
    }
    return new ConsumerRecords<K, V>(recordMap);
  }

  static PrintTopic printTopic(
      final String name,
      final boolean fromBeginning,
      final Integer interval,
      final Integer limit) {
    return new PrintTopic(
        new NodeLocation(0, 1),
        QualifiedName.of(name),
        fromBeginning,
        interval == null ? OptionalInt.empty() : OptionalInt.of(interval),
        limit == null ? OptionalInt.empty() : OptionalInt.of(limit)
    );
  }

  static class TestSubscriber<T> implements Subscriber<T> {

    CountDownLatch done = new CountDownLatch(1);
    Throwable error = null;
    List<T> elements = Lists.newLinkedList();
    Schema schema = null;
    Subscription subscription;

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
    public void onSchema(final Schema s) {
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
  }
}
