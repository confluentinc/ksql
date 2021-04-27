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
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.kafka.streams.StreamsConfig;
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
public class ScalablePushRegistry implements ProcessorSupplier<Object, GenericRow> {

  private static final Logger LOG = LoggerFactory.getLogger(ScalablePushRegistry.class);

  private final PushLocator pushLocator;
  private final LogicalSchema logicalSchema;
  private final boolean windowed;
  // All mutable field accesses are protected with synchronized.  The exception is when
  // processingQueues is accessed to processed rows, in which case we want a weakly consistent
  // view of the map, so we just iterate over the ConcurrentHashMap directly.
  private final ConcurrentHashMap<QueryId, ProcessingQueue> processingQueues
      = new ConcurrentHashMap<>();
  private boolean closed = false;

  public ScalablePushRegistry(
      final PushLocator pushLocator,
      final LogicalSchema logicalSchema,
      final boolean windowed
  ) {
    this.pushLocator = pushLocator;
    this.logicalSchema = logicalSchema;
    this.windowed = windowed;
  }

  public synchronized void close() {
    for (ProcessingQueue queue : processingQueues.values()) {
      queue.close();
    }
    processingQueues.clear();
    closed = true;
  }

  public synchronized void register(final ProcessingQueue processingQueue) {
    if (closed) {
      throw new IllegalStateException("Shouldn't register after closing");
    }
    processingQueues.put(processingQueue.getQueryId(), processingQueue);
  }

  public synchronized void unregister(final ProcessingQueue processingQueue) {
    if (closed) {
      throw new IllegalStateException("Shouldn't unregister after closing");
    }
    processingQueues.remove(processingQueue.getQueryId());
  }

  public PushLocator getLocator() {
    return pushLocator;
  }

  @VisibleForTesting
  int numRegistered() {
    return processingQueues.size();
  }

  @SuppressWarnings("unchecked")
  private void handleRow(
      final Object key, final GenericRow value, final long timestamp) {
    for (ProcessingQueue queue : processingQueues.values()) {
      try {
        // The physical operators may modify the keys and values, so we make a copy to ensure
        // that there's no cross-query interference.
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
  }

  @Override
  public Processor<Object, GenericRow> get() {
    return new PeekProcessor();
  }

  private final class PeekProcessor implements Processor<Object, GenericRow> {

    private ProcessorContext context;

    private PeekProcessor() {
    }

    public void init(final ProcessorContext context) {
      this.context = context;
    }

    public void process(final Object key, final GenericRow value) {
      handleRow(key, value, this.context.timestamp());
      this.context.forward(key, value);
    }

    @Override
    public void close() {
    }
  }

  public static Optional<ScalablePushRegistry> create(
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
    return Optional.of(new ScalablePushRegistry(pushLocator, logicalSchema, windowed));
  }
}
