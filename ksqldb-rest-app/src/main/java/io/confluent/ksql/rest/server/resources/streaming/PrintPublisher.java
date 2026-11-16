/*
 * Copyright 2018 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.services.ServiceContext;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("UnstableApiUsage")
public class PrintPublisher implements Flow.Publisher<Collection<String>> {

  private static final Logger log = LogManager.getLogger(PrintPublisher.class);

  private final ListeningScheduledExecutorService exec;
  private final ServiceContext serviceContext;
  private final Map<String, Object> consumerProperties;
  private final PrintTopic printTopic;

  public PrintPublisher(
      final ListeningScheduledExecutorService exec,
      final ServiceContext serviceContext,
      final Map<String, Object> consumerProperties,
      final PrintTopic printTopic) {
    this.exec = requireNonNull(exec, "exec");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.consumerProperties = requireNonNull(consumerProperties, "consumerProperties");
    this.printTopic = requireNonNull(printTopic, "printTopic");
  }

  @Override
  public void subscribe(final Flow.Subscriber<Collection<String>> subscriber) {
    final KafkaConsumer<Bytes, Bytes> topicConsumer =
        PrintTopicUtil.createTopicConsumer(serviceContext, consumerProperties, printTopic);

    subscriber.onSubscribe(
        new PrintSubscription(
            exec,
            printTopic,
            subscriber,
            topicConsumer,
            new RecordFormatter(
                serviceContext.getSchemaRegistryClient(),
                printTopic.getTopic()
            )
        )
    );
  }

  static class PrintSubscription extends PollingSubscription<Collection<String>> {

    private final PrintTopic printTopic;
    private final KafkaConsumer<Bytes, Bytes> topicConsumer;
    private final RecordFormatter formatter;
    private boolean closed = false;

    private int numPolled = 0;
    private int numWritten = 0;

    PrintSubscription(
        final ListeningScheduledExecutorService exec,
        final PrintTopic printTopic,
        final Subscriber<Collection<String>> subscriber,
        final KafkaConsumer<Bytes, Bytes> topicConsumer,
        final RecordFormatter formatter
    ) {
      super(exec, subscriber, null);
      this.printTopic = requireNonNull(printTopic, "printTopic");
      this.topicConsumer = requireNonNull(topicConsumer, "topicConsumer");
      this.formatter = requireNonNull(formatter, "formatter");
    }

    @Override
    public Collection<String> poll() {
      try {
        final ConsumerRecords<Bytes, Bytes> records = topicConsumer.poll(Duration.ZERO);
        if (records.isEmpty()) {
          return null;
        }

        final Collection<String> formatted = formatter.format(records);
        final Collection<String> limited = new LimitIntervalCollection<>(
            formatted,
            printTopic.getLimit().orElse(Integer.MAX_VALUE) - numWritten,
            printTopic.getIntervalValue(),
            numPolled % printTopic.getIntervalValue()
        );

        numPolled += formatted.size();
        numWritten += limited.size();

        if (printTopic.getLimit().isPresent()
            && numWritten >= printTopic.getLimit().getAsInt()) {
          setDone();
        }

        return limited;
      } catch (final Exception e) {
        setError(e);
        return null;
      }
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        log.info("Closing consumer for topic {}", printTopic.getTopic());
        closed = true;
        topicConsumer.close();
      }
    }
  }

  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE") // False positive
  private static final class LimitIntervalCollection<T> extends AbstractCollection<T> {

    private final Iterable<T> source;
    private final int limit;
    private final int interval;
    private final int size;

    private LimitIntervalCollection(
        final Collection<T> source,
        final int limit,
        final int interval,
        final int start) {
      Preconditions.checkArgument(interval > 0, "interval must be greater than 0");
      Preconditions.checkArgument(start >= 0, "start must be greater than or equal to 0");
      Preconditions.checkArgument(limit >= 0, "limit must be greater than or equal to 0");
      requireNonNull(source, "source");

      this.source = Iterables.skip(source, start);
      this.size = Math.min(
          IntMath.divide(source.size() - start, interval, RoundingMode.CEILING),
          limit);
      this.interval = interval;
      this.limit = limit;
    }

    @Override
    public @Nonnull Iterator<T> iterator() {
      return new Iterator<T>() {

        final Iterator<T> it = source.iterator();
        int remaining = limit;

        @Override
        public boolean hasNext() {
          return remaining > 0 && it.hasNext();
        }

        @Override
        public T next() {
          remaining--;
          final T next = it.next();
          Iterators.advance(it, interval - 1);
          return next;
        }
      };
    }

    @Override
    public int size() {
      return size;
    }
  }
}
