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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.physical.scalablepush.locator.AllHostsLocator;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This registry is kept with every persistent query, peeking at the stream which is the output
 * of the topology. These rows are then fed to any registered ProcessingQueues where they are
 * eventually passed on to scalable push queries.
 */
public class ScalablePushRegistry implements ProcessorSupplier<Object, GenericRow, Void, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(ScalablePushRegistry.class);

  private final PushLocator pushLocator;
  private final LogicalSchema logicalSchema;
  private final boolean isTable;
  private final boolean windowed;
  private final boolean newNodeContinuityEnforced;
  private final Map<String, Object> consumerProperties;
  private final KsqlTopic ksqlTopic;
  private final ServiceContext serviceContext;
  private final KsqlConfig ksqlConfig;
  private final ExecutorService executorService;
  private final ExecutorService executorServiceCatchup;
  // All mutable field accesses are protected with synchronized.  The exception is when
  // processingQueues is accessed to processed rows, in which case we want a weakly consistent
  // view of the map, so we just iterate over the ConcurrentHashMap directly.
  private final ConcurrentHashMap<QueryId, ProcessingQueue> processingQueues
      = new ConcurrentHashMap<>();
  private boolean closed = false;
  private volatile boolean hasReceivedData = false;

  private final AtomicReference<List<TopicPartition>> topicPartitions = new AtomicReference<>(null);
  private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> latestOffsets = new AtomicReference<>(null);
  private final AtomicReference<Integer> partitions = new AtomicReference<>(null);
  private final AtomicInteger blockers = new AtomicInteger(0);
  private final AtomicBoolean waiting = new AtomicBoolean(false);

  public ScalablePushRegistry(
      final PushLocator pushLocator,
      final LogicalSchema logicalSchema,
      final boolean isTable,
      final boolean windowed,
      final boolean newNodeContinuityEnforced,
      final Map<String, Object> consumerProperties,
      final KsqlTopic ksqlTopic,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig
  ) {
    this.pushLocator = pushLocator;
    this.logicalSchema = logicalSchema;
    this.isTable = isTable;
    this.windowed = windowed;
    this.newNodeContinuityEnforced = newNodeContinuityEnforced;
    this.consumerProperties = consumerProperties;
    this.ksqlTopic = ksqlTopic;
    this.serviceContext = serviceContext;
    this.ksqlConfig = ksqlConfig;
    this.executorService = Executors.newSingleThreadExecutor();
    this.executorServiceCatchup = Executors.newScheduledThreadPool(4);
    executorService.submit(this::runLatest);
  }

  public synchronized void close() {
    for (ProcessingQueue queue : processingQueues.values()) {
      queue.close();
    }
    processingQueues.clear();
    closed = true;
  }

  public synchronized void register(
      final ProcessingQueue processingQueue,
      final boolean expectingStartOfRegistryData,
      final Optional<String> token
  ) {
    if (closed) {
      throw new IllegalStateException("Shouldn't register after closing");
    }
    if (hasReceivedData && newNodeContinuityEnforced && expectingStartOfRegistryData) {
      throw new IllegalStateException("New node missed data");
    }
    processingQueues.put(processingQueue.getQueryId(), processingQueue);
    executorServiceCatchup.submit(() -> runCatchup(token));
  }

  public synchronized void unregister(final ProcessingQueue processingQueue) {
    if (closed) {
      throw new IllegalStateException("Shouldn't unregister after closing");
    }
    processingQueues.remove(processingQueue.getQueryId());
  }

  private void runLatest() {
    try (KafkaConsumer<GenericKey, GenericRow> consumer = createConsumer()) {
      Map<String, List<PartitionInfo>> partitionInfo = consumer.listTopics();
      while (!partitionInfo.containsKey(ksqlTopic.getKafkaTopicName())) {
        try {
          Thread.sleep(100);
          partitionInfo = consumer.listTopics();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      partitions.set(partitionInfo.get(ksqlTopic.getKafkaTopicName()).size());
      consumer.subscribe(ImmutableList.of(ksqlTopic.getKafkaTopicName()), new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
          topicPartitions.set(null);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
          topicPartitions.set(ImmutableList.copyOf(collection));
        }
      });


      Map<TopicPartition, Long> currentPositions = new HashMap<>();
      while (true) {
        final ConsumerRecords<GenericKey, GenericRow> records = consumer.poll(Duration.ofMillis(100));
        if (records.isEmpty()) {
          checkShouldWait();
          continue;
        }
        for (ConsumerRecord<GenericKey, GenericRow> rec : records) {
          System.out.println("Got record " + rec);
          System.out.println("KEY " + rec.key());
          System.out.println("VALUE " + rec.value());
          currentPositions.put(new TopicPartition(rec.topic(), rec.partition()), rec.offset());
          handleRow(rec.key(), rec.value(), rec.timestamp(), getToken(currentPositions, rec.topic(), partitions.get()));
        }
        consumer.commitSync();
        Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));
        latestOffsets.set(offsets);

        checkShouldWait();
      }
    }
  }

  private void checkShouldWait() {
    synchronized (blockers) {
      while (blockers.get() > 0) {
        try {
          System.out.println("WAITING TO BE WOKEN UP");
          waiting.set(true);
          blockers.wait();
          System.out.println("WAKING UP");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      waiting.set(false);
    }
  }

  private void runCatchup(Optional<String> token) {

    try (KafkaConsumer<GenericKey, GenericRow> consumer = createConsumer()) {
      while (!consumer.listTopics().containsKey(ksqlTopic.getKafkaTopicName())) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      while (true) {
        List<TopicPartition> tps = topicPartitions.get();
        while (tps == null) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          tps = topicPartitions.get();
        }

        consumer.assign(tps);

        Map<Integer, Long> startingOffsets = parseToken(token);
        for (TopicPartition tp : tps) {
          consumer.seek(tp, startingOffsets.containsKey(tp.partition()) ? startingOffsets.get(tp.partition()): 0);
        }
        Map<TopicPartition, Long> currentPositions = new HashMap<>();
        AtomicBoolean blocked = new AtomicBoolean(false);
        while (true) {
          final ConsumerRecords<GenericKey, GenericRow> records = consumer.poll(Duration.ofMillis(100));
          if (records.isEmpty()) {
            if (checkCaughtUp(consumer, blocked)) {
              return;
            }
            continue;
          }
          for (ConsumerRecord<GenericKey, GenericRow> rec : records) {
            System.out.println("Got catchup record " + rec);
            System.out.println("catchup KEY " + rec.key());
            System.out.println("catchup VALUE " + rec.value());
            currentPositions.put(new TopicPartition(rec.topic(), rec.partition()), rec.offset());
            if (handleRow(rec.key(), rec.value(), rec.timestamp(), getToken(currentPositions, rec.topic(), partitions.get()))) {
              try {
                System.out.println("Sleeping for a bit");
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }

          consumer.commitSync();

//          lastToken = getToken(offsets);

          if (checkCaughtUp(consumer, blocked)) {
            return;
          };
        }
      }
    }
  }

  private boolean checkCaughtUp(KafkaConsumer<GenericKey, GenericRow> consumer, AtomicBoolean blocked) {
    Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));
    if (caughtUp(latestOffsets.get(), offsets)) {
      System.out.println("CAUGHT UP!!");

      synchronized (blockers) {
        if (waiting.get() && caughtUp(latestOffsets.get(), offsets)) {
          System.out.println("TRANSFER");
          if (blocked.get()) {
            System.out.println("DECREMENTING BLOCKERS to " + blockers.get());
            blockers.decrementAndGet();
            blockers.notify();
          }
          return true;
        } else {
          System.out.println("INCREMENTING BLOCKERS to " + blockers.get());
          blocked.set(true);
          blockers.incrementAndGet();
        }
      }
    }
    return false;
  }

  private boolean caughtUp(Map<TopicPartition, OffsetAndMetadata> latestOffsets, Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (!latestOffsets.keySet().equals(offsets.keySet())) {
      return false;
    } else {
      for (TopicPartition tp : latestOffsets.keySet()) {
        OffsetAndMetadata latestOam = latestOffsets.get(tp);
        OffsetAndMetadata oam = offsets.get(tp);
        if (latestOam == null || oam == null) {
          return false;
        }
        long latestOffset = latestOam.offset();
        long offset = oam.offset();
        if (offset < latestOffset) {
          return false;
        }
      }
      return true;
    }
  }

  private String getToken(Map<TopicPartition, Long> offsets, String topic, int numPartitions) {
    List<String> offsetList = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      TopicPartition tp = new TopicPartition(topic, i);
      offsetList.add(Long.toString(offsets.containsKey(tp) ? offsets.get(tp) : 0));
    }
    return String.join(",", offsetList);
  }

  private Map<Integer, Long> parseToken(Optional<String> token) {
    if (token.isPresent()) {
      int i = 0;
      Map<Integer, Long> offsets = new HashMap<>();
      for (String str : token.get().split(",")) {
        offsets.put(i++, Long.parseLong(str));
      }
      return offsets;
    } else {
      return Collections.emptyMap();
    }
  }

  private KafkaConsumer<GenericKey, GenericRow> createConsumer() {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        logicalSchema,
        ksqlTopic.getKeyFormat().getFeatures(),
        ksqlTopic.getValueFormat().getFeatures()
    );
    final KeySerdeFactory keySerdeFactory = new GenericKeySerDe();
    final Serde<GenericKey> keySerde = keySerdeFactory.create(
        ksqlTopic.getKeyFormat().getFormatInfo(),
        physicalSchema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    );

    final ValueSerdeFactory valueSerdeFactory = new GenericRowSerDe();
    final Serde<GenericRow> valueSerde = valueSerdeFactory.create(
        ksqlTopic.getValueFormat().getFormatInfo(),
        physicalSchema.valueSchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    );
    return new KafkaConsumer<>(
        consumerConfig(),
        keySerde.deserializer(),
        valueSerde.deserializer()
    );
  }

  public PushLocator getLocator() {
    return pushLocator;
  }

  public boolean isTable() {
    return isTable;
  }

  public boolean isWindowed() {
    return windowed;
  }


  @VisibleForTesting
  public int numRegistered() {
    return processingQueues.size();
  }

  @SuppressWarnings("unchecked")
  private boolean handleRow(
      final Object key, final GenericRow value, final long timestamp, final String token) {
      hasReceivedData = true;
    // We don't currently handle null in either field
    if ((key == null && !logicalSchema.key().isEmpty()) || value == null) {
      return false;
    }
    for (ProcessingQueue queue : processingQueues.values()) {
      try {
        // The physical operators may modify the keys and values, so we make a copy to ensure
        // that there's no cross-query interference.
        final TableRow row;
        if (!windowed) {
          final GenericKey keyCopy = GenericKey.fromList(
              key != null ? ((GenericKey) key).values() : Collections.emptyList());
          final GenericRow valueCopy = GenericRow.fromList(value.values());
          row = Row.of(logicalSchema, keyCopy, valueCopy, timestamp, token);
        } else {
          final Windowed<GenericKey> windowedKey = (Windowed<GenericKey>) key;
          final Windowed<GenericKey> keyCopy =
              new Windowed<>(GenericKey.fromList(windowedKey.key().values()),
                  windowedKey.window());
          final GenericRow valueCopy = GenericRow.fromList(value.values());
          row = WindowedRow.of(logicalSchema, keyCopy, valueCopy, timestamp);
        }
        return queue.offer(row);
      } catch (final Throwable t) {
        LOG.error("Error while offering row", t);
      }
    }
    return false;
  }

  public synchronized void onError() {
    for (ProcessingQueue queue : processingQueues.values()) {
      queue.onError();
    }
  }

  @Override
  public Processor<Object, GenericRow, Void, Void> get() {
    return new PeekProcessor();
  }

  private final class PeekProcessor implements Processor<Object, GenericRow, Void, Void> {

    private PeekProcessor() {

    }

    public void init(final ProcessorContext context) {

    }

    public void process(final Record<Object, GenericRow> record) {
//      handleRow(record);
    }

    @Override
    public void close() {

    }
  }

  public static Optional<ScalablePushRegistry> create(
      final LogicalSchema logicalSchema,
      final Supplier<List<PersistentQueryMetadata>> allPersistentQueries,
      final boolean isTable,
      final boolean windowed,
      final Map<String, Object> streamsProperties,
      final boolean newNodeContinuityEnforced,
      final Map<String, Object> consumerProperties,
      final KsqlTopic ksqlTopic,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig
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
    return Optional.of(new ScalablePushRegistry(pushLocator, logicalSchema, isTable, windowed, newNodeContinuityEnforced, consumerProperties, ksqlTopic, serviceContext, ksqlConfig));
  }

  /**
   * Common consumer properties that tests will need.
   *
   * @return base set of consumer properties.
   */
  public Map<String, Object> consumerConfig() {
    final Map<String, Object> config = new HashMap<>(consumerProperties);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Try to keep consumer groups stable:
    config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 7_000);
    config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 20_000);
    config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 3_000);
    return config;
  }
}
