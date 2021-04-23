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

package io.confluent.ksql.physical.scalablepush;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.physical.scalablepush.locator.AllHostsLocator;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.vertx.core.impl.ConcurrentHashSet;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This registry is kept with every persistent query, peeking at the stream which is the output
 * of the topology. These rows are then fed to any registered ProcessingQueues where they are
 * eventually passed on to scalable push queries.
 */
public class ScalablePushRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ScalablePushRegistry.class);

  private final KStream<?, GenericRow> stream;
  private final LogicalSchema logicalSchema;
  private final PushLocator pushLocator;
  private final Set<ProcessingQueue> processingQueues = new ConcurrentHashSet<>();

  public ScalablePushRegistry(
      final KStream<?, GenericRow> stream,
      final LogicalSchema logicalSchema,
      final PushLocator pushLocator,
      final boolean windowed
  ) {
    this.stream = stream;
    this.logicalSchema = logicalSchema;
    this.pushLocator = pushLocator;
    registerPeek(windowed);
  }

  public void close() {
    for (ProcessingQueue queue : processingQueues) {
      queue.close();
    }
  }

  public void register(final ProcessingQueue processingQueue) {
    processingQueues.add(processingQueue);
  }

  public void unregister(final ProcessingQueue processingQueue) {
    processingQueues.remove(processingQueue);
  }

  public PushLocator getLocator() {
    return pushLocator;
  }

  @VisibleForTesting
  int numRegistered() {
    return processingQueues.size();
  }

  // Registers a peek processor which makes a copy of every row and passes it on to any registered
  // queues.
  @SuppressWarnings("unchecked")
  private void registerPeek(final boolean windowed) {
    final ProcessorSupplier<Object, GenericRow> peek = new Peek<>((key, value, timestamp) -> {
      for (ProcessingQueue queue : processingQueues) {
        try {
          final TableRow row;
          if (!windowed) {
            final GenericKey keyCopy = GenericKey.fromList(((GenericKey) key).values());
            final GenericRow valueCopy = GenericRow.fromList(value.values());
            row = Row.of(logicalSchema, keyCopy, valueCopy, timestamp);
          } else {
            final Windowed<GenericKey> windowedKey = (Windowed<GenericKey>) key;
            final Windowed<GenericKey> keyCopy =
                new Windowed<>(GenericKey.fromList(windowedKey.key().values()),
                    windowedKey.window());
            final GenericRow valueCopy = GenericRow.fromList(value.values());
            row = WindowedRow.of(logicalSchema, keyCopy, valueCopy, timestamp);
          }
          queue.offer(row);
        } catch (final Throwable t) {
          LOG.error("Error while offering row", t);
        }
      }
    });
    stream.process(peek);
  }

  // This Peek processors just passes the row to the given action.
  static class Peek<K, V> implements ProcessorSupplier<K, V> {
    private final ForeachAction<K, V> action;

    Peek(final ForeachAction<K, V> action) {
      this.action = action;
    }

    public Processor<K, V> get() {
      return new PeekProcessor();
    }

    private final class PeekProcessor implements Processor<K, V> {

      private ProcessorContext context;

      private PeekProcessor() {
      }

      public void init(final ProcessorContext context) {
        this.context = context;
      }

      public void process(final K key, final V value) {
        Peek.this.action.apply(key, value, this.context.timestamp());
        this.context.forward(key, value);
      }

      @Override
      public void close() {
      }
    }
  }

  public interface ForeachAction<K, V> {
    void apply(K key, V value, long timestamp);
  }

  public static Optional<ScalablePushRegistry> create(
      final KStream<?, GenericRow> stream,
      final LogicalSchema logicalSchema,
      final Supplier<List<PersistentQueryMetadata>> allPersistentQueries,
      final boolean windowed,
      final Map<String, Object> streamsProperties
  ) {
    final Object appServer = streamsProperties.get(StreamsConfig.APPLICATION_SERVER_CONFIG);
    if (appServer == null) {
      return Optional.empty();
    }

    if (!(appServer instanceof String)) {
      throw new IllegalArgumentException(StreamsConfig.APPLICATION_SERVER_CONFIG + " not String");
    }

    final URL localhost;
    try {
      localhost = new URL((String) appServer);
    } catch (final MalformedURLException e) {
      throw new IllegalArgumentException(StreamsConfig.APPLICATION_SERVER_CONFIG + " malformed: "
          + "'" + appServer + "'");
    }

    final PushLocator pushLocator = new AllHostsLocator(allPersistentQueries, localhost);
    return Optional.of(new ScalablePushRegistry(stream, logicalSchema, pushLocator, windowed));
  }
}
