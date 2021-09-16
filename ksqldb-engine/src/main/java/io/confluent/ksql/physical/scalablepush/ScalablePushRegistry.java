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
import com.google.common.collect.ImmutableSet;
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
import io.confluent.ksql.rest.entity.ProgressToken;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
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
  private static final LogicalSchema EMPTY_SCHEMA = LogicalSchema.builder().build();
  private static final long LATEST_CONSUMER_OLDEST_COMMIT_AGE_MS = 10 * 60000;

  private final PushLocator pushLocator;
  private final LogicalSchema logicalSchema;
  private final boolean isTable;
  private final boolean windowed;
//  private final boolean newNodeContinuityEnforced;
  private final Map<String, Object> consumerProperties;
  private final KsqlTopic ksqlTopic;
  private final ServiceContext serviceContext;
  private final KsqlConfig ksqlConfig;
  private final ExecutorService executorService;
  private final ExecutorService executorServiceCatchup;
  // All mutable field accesses are protected with synchronized.  The exception is when
  // processingQueues is accessed to processed rows, in which case we want a weakly consistent
  // view of the map, so we just iterate over the ConcurrentHashMap directly.
//  private final ConcurrentHashMap<QueryId, ProcessingQueue> processingQueues
//      = new ConcurrentHashMap<>();
//  private final ConcurrentHashMap<QueryId, ProcessingQueue> catchupProcessingQueues
//      = new ConcurrentHashMap<>();
  private boolean closed = false;
//  private volatile boolean newAssignment = false;
  private boolean isLatestStarted = false;

  //private final AtomicReference<List<TopicPartition>> topicPartitions = new AtomicReference<>(null);
  //private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> latestOffsets = new AtomicReference<>(null);
//  private final AtomicReference<Integer> partitions = new AtomicReference<>(null);
  private final AtomicInteger blockers = new AtomicInteger(0);
  private final AtomicBoolean waiting = new AtomicBoolean(false);
  private AtomicReference<LatestConsumer> latestConsumer = new AtomicReference<>(null);
  private Map<QueryId, CatchupConsumer> catchupConsumers = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<ProcessingQueue> latestPendingQueues
      = new ConcurrentLinkedQueue<>();

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
//    this.newNodeContinuityEnforced = newNodeContinuityEnforced;
    this.consumerProperties = consumerProperties;
    this.ksqlTopic = ksqlTopic;
    this.serviceContext = serviceContext;
    this.ksqlConfig = ksqlConfig;
    this.executorService = Executors.newSingleThreadExecutor();
    this.executorServiceCatchup = Executors.newScheduledThreadPool(4);
//    executorService.submit(() -> {
//      try {
//        runLatest();
//      } catch (final Throwable t) {
//        t.printStackTrace();
//      }
//    });
  }

  public synchronized void close() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.close();
    }
    for (CatchupConsumer catchupConsumer : catchupConsumers.values()) {
      catchupConsumer.close();
    }
//    for (ProcessingQueue queue : processingQueues.values()) {
//      queue.close();
//    }
//    processingQueues.clear();
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
//    processingQueues.put(processingQueue.getQueryId(), processingQueue);
    if (token.isPresent()) {
      executorServiceCatchup.submit(() -> {
            try {
              runCatchup(processingQueue, token);
            } catch (Throwable t) {
              t.printStackTrace();
            }
          });

    } else if (isLatestStarted) {
      final LatestConsumer latestConsumer = this.latestConsumer.get();
      if (latestConsumer != null && !latestConsumer.isClosed()) {
        latestConsumer.register(processingQueue);
      } else {
        latestPendingQueues.add(processingQueue);
      }
    } else {
      isLatestStarted = true;
      latestPendingQueues.add(processingQueue);
      executorService.submit(() -> {
        try {
          runLatest();
        } catch (final Throwable t) {
          t.printStackTrace();
        }
      });
    }
  }

  public synchronized void unregister(final ProcessingQueue processingQueue) {
    if (closed) {
      throw new IllegalStateException("Shouldn't unregister after closing");
    }
    System.out.println("UNREGISTERING " + processingQueue);
//    processingQueues.remove(processingQueue.getQueryId());
//    catchupProcessingQueues.remove(processingQueue.getQueryId());
    latestPendingQueues.remove(processingQueue);
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.unregister(processingQueue);
      if (latestConsumer.numRegistered() == 0 && catchupConsumers.size() == 0) {
        latestConsumer.close();
      }
    }
    catchupConsumers.computeIfPresent(processingQueue.getQueryId(), (queryId, catchupConsumer) -> {
      catchupConsumer.unregister(processingQueue);
      catchupConsumer.close();
      return null;
    });
  }

//  private void runLatest2() {
//    try (KafkaConsumer<GenericKey, GenericRow> consumer = createConsumer(true)) {
//      Map<String, List<PartitionInfo>> partitionInfo = consumer.listTopics();
//      while (!partitionInfo.containsKey(ksqlTopic.getKafkaTopicName())) {
//        try {
//          Thread.sleep(100);
//          partitionInfo = consumer.listTopics();
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//      partitions.set(partitionInfo.get(ksqlTopic.getKafkaTopicName()).size());
//      consumer.subscribe(ImmutableList.of(ksqlTopic.getKafkaTopicName()), new ConsumerRebalanceListener() {
//        @Override
//        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
//          System.out.println("Revoked assignment" + collection);
//          topicPartitions.set(null);
//          newAssignment = true;
//        }
//
//        @Override
//        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
//          System.out.println("Got assignment " + collection);
//          topicPartitions.set(ImmutableList.copyOf(collection));
//          newAssignment = true;
//        }
//      });
//
//
//      Map<TopicPartition, Long> currentPositions = new HashMap<>();
//
//
//      while (topicPartitions.get() == null) {
//        try {
//          consumer.poll(Duration.ofMillis(100));
//          Thread.sleep(100);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//
////      //consumer.seekToEnd(topicPartitions.get());
////      for (TopicPartition tp : topicPartitions.get()) {
//////        final TopicPartition tp = new TopicPartition(ksqlTopic.getKafkaTopicName(), i);
////        currentPositions.put(tp, consumer.position(tp));
////      }
//
//      System.out.println("foo " + currentPositions);
//      while (true) {
//        final ConsumerRecords<GenericKey, GenericRow> records = consumer.poll(Duration.ofMillis(100));
//        if (records.isEmpty()) {
//          checkShouldWait();
//          continue;
//        }
//
//        if (newAssignment) {
//          newAssignment = false;
//          resetCurrentPosition(consumer, currentPositions);
//        }
//
//        String startToken = TokenUtils.getToken(currentPositions, ksqlTopic.getKafkaTopicName(), partitions.get());
//
//        for (ConsumerRecord<GenericKey, GenericRow> rec : records) {
//          System.out.println("Got record " + rec);
//          System.out.println("KEY " + rec.key());
//          System.out.println("VALUE " + rec.value());
//          //currentPositions.put(new TopicPartition(rec.topic(), rec.partition()), rec.offset());
//          handleRow(processingQueues, rec.key(), rec.value(), rec.timestamp(), TokenUtils.getToken(currentPositions, rec.topic(), partitions.get()));
//        }
////        for (int i = 0; i < partitions.get(); i++) {
////          final TopicPartition tp = new TopicPartition(ksqlTopic.getKafkaTopicName(), i);
////          currentPositions.put(tp, consumer.position(tp));
////        }
//        for (TopicPartition tp : topicPartitions.get()) {
//          currentPositions.put(tp, consumer.position(tp));
//        }
//
//        String endToken = TokenUtils.getToken(currentPositions, ksqlTopic.getKafkaTopicName(), partitions.get());
//
//        System.out.println("Sending tokens start " + startToken + " end " + endToken);
//        handleProgressToken(processingQueues, startToken, endToken);
//        consumer.commitSync();
//        //Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));
//
//
//        //latestOffsets.set(offsets);
//
//        checkShouldWait();
//      }
//    }
//  }

//  private void resetCurrentPosition(
//      KafkaConsumer<GenericKey, GenericRow> consumer,
//      Map<TopicPartition, Long> currentPositions
//  ) {
//    for (int i = 0; i < partitions.get(); i++) {
//      currentPositions.put(new TopicPartition(ksqlTopic.getKafkaTopicName(), i), 0L);
//    }
//    for (TopicPartition tp : topicPartitions.get()) {
//      currentPositions.put(tp, consumer.position(tp));
//    }
//  }

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

//  private void runCatchup2(final ProcessingQueue processingQueue, Optional<String> token) {
//
//    try (KafkaConsumer<GenericKey, GenericRow> consumer = createConsumer(false)) {
//      while (!consumer.listTopics().containsKey(ksqlTopic.getKafkaTopicName())) {
//        try {
//          Thread.sleep(100);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//      while (true) {
//        List<TopicPartition> tps = topicPartitions.get();
//        while (tps == null) {
//          try {
//            Thread.sleep(100);
//          } catch (InterruptedException e) {
//            e.printStackTrace();
//          }
//          tps = topicPartitions.get();
//        }
//
//        consumer.assign(tps);
//
//        Map<Integer, Long> startingOffsets = TokenUtils.parseToken(token);
//        for (TopicPartition tp : tps) {
//          consumer.seek(tp, startingOffsets.containsKey(tp.partition()) ? startingOffsets.get(tp.partition()): 0);
//        }
//        Map<TopicPartition, Long> currentPositions = new HashMap<>();
//        AtomicBoolean blocked = new AtomicBoolean(false);
//        while (true) {
//          final ConsumerRecords<GenericKey, GenericRow> records = consumer.poll(Duration.ofMillis(100));
//          if (records.isEmpty()) {
//            if (checkCaughtUp(consumer, blocked)) {
//              return;
//            }
//            continue;
//          }
//          for (ConsumerRecord<GenericKey, GenericRow> rec : records) {
//            System.out.println("Got catchup record " + rec);
//            System.out.println("catchup KEY " + rec.key());
//            System.out.println("catchup VALUE " + rec.value());
//            currentPositions.put(new TopicPartition(rec.topic(), rec.partition()), rec.offset());
//            if (handleRow(ImmutableMap.of(processingQueue.getQueryId(), processingQueue),
//                rec.key(), rec.value(), rec.timestamp(), TokenUtils.getToken(currentPositions, rec.topic(), partitions.get()))) {
//              try {
//                System.out.println("Sleeping for a bit");
//                Thread.sleep(1000);
//              } catch (InterruptedException e) {
//                e.printStackTrace();
//              }
//            }
//          }
//
//          consumer.commitSync();
//
////          lastToken = getToken(offsets);
//
//          if (checkCaughtUp(consumer, blocked)) {
//            return;
//          };
//        }
//      }
//    }
//  }
//
//  private boolean checkCaughtUp(KafkaConsumer<GenericKey, GenericRow> consumer, AtomicBoolean blocked) {
//    Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));
//    if (caughtUp(latestOffsets.get(), offsets)) {
//      System.out.println("CAUGHT UP!!");
//
//      synchronized (blockers) {
//        if (waiting.get() && caughtUp(latestOffsets.get(), offsets)) {
//          System.out.println("TRANSFER");
//          if (blocked.get()) {
//            System.out.println("DECREMENTING BLOCKERS to " + blockers.get());
//            blockers.decrementAndGet();
//            blockers.notify();
//          }
//          return true;
//        } else {
//          System.out.println("INCREMENTING BLOCKERS to " + blockers.get());
//          blocked.set(true);
//          blockers.incrementAndGet();
//        }
//      }
//    }
//    return false;
//  }

  private static boolean caughtUp(Map<TopicPartition, OffsetAndMetadata> latestOffsets, Map<TopicPartition, OffsetAndMetadata> offsets) {
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

  private KafkaConsumer<GenericKey, GenericRow> createConsumer(boolean latest) {
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
        consumerConfig(latest),
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
  public int latestNumRegistered() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      return latestConsumer.numRegistered();
    }
    return 0;
  }

  @VisibleForTesting
  public boolean latestHasAssignment() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      return latestConsumer.getAssignment() != null;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private boolean handleRow(
      final Map<QueryId, ProcessingQueue> processingQueues,
      final Object key, final GenericRow value, final long timestamp, final String token) {
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
          row = Row.of(logicalSchema, keyCopy, valueCopy, timestamp, Optional.empty());
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

  private void handleProgressToken(
      final Map<QueryId, ProcessingQueue> processingQueues,
      final String startToken,
      final String endToken) {
    for (ProcessingQueue queue : processingQueues.values()) {
      final TableRow row;
      if (!windowed) {
        row = Row.of(EMPTY_SCHEMA, GenericKey.genericKey(), GenericRow.genericRow(),
            System.currentTimeMillis(), Optional.of(new ProgressToken(startToken, endToken)));
      } else {
        row = WindowedRow.of(EMPTY_SCHEMA, new Windowed<>(GenericKey.genericKey(),
                new TimeWindow(0, 0)), GenericRow.genericRow(),
            System.currentTimeMillis());
      }
      queue.offer(row);
    }
  }

  public synchronized void onError() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.onError();
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
  public Map<String, Object> consumerConfig(boolean latest) {
    final Map<String, Object> config = new HashMap<>(consumerProperties);
    //UUID.randomUUID().toString()
    config.put(ConsumerConfig.GROUP_ID_CONFIG, latest ? "spq_latest1": "spq_catchup");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Try to keep consumer groups stable:
    config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 7_000);
    config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 20_000);
    config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 3_000);
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return config;
  }

  private ConsumerMetadata getMetadata(KafkaConsumer<GenericKey, GenericRow> consumer) {
    Map<String, List<PartitionInfo>> partitionInfo = consumer.listTopics();
    while (!partitionInfo.containsKey(ksqlTopic.getKafkaTopicName())) {
      try {
        Thread.sleep(100);
        partitionInfo = consumer.listTopics();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    int numPartitions = partitionInfo.get(ksqlTopic.getKafkaTopicName()).size();
    return new ConsumerMetadata(numPartitions);
  }

  class LatestConsumer extends Consumer {

    public LatestConsumer(int partitions, String topicName,
        KafkaConsumer<GenericKey, GenericRow> consumer) {
      super(partitions, topicName, consumer);
    }

    @Override
    public boolean onEmptyRecords() {
      checkShouldWait();
      return false;
    }

    @Override
    public boolean afterCommit() {
      checkShouldWait();
      return false;
    }

    @Override
    public void onNewAssignment() {
    }

    @Override
    public void afterFirstPoll() {
      final Set<TopicPartition> topicPartitions = this.topicPartitions.get();
      long timeMs = System.currentTimeMillis() - LATEST_CONSUMER_OLDEST_COMMIT_AGE_MS;
      HashMap<TopicPartition, Long> timestamps = new HashMap<>();
      for (TopicPartition tp : topicPartitions) {
        timestamps.put(tp, timeMs);
      }
      Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = consumer.offsetsForTimes(timestamps);
      Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = consumer.committed(topicPartitions);
      for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetAndTimestampMap.entrySet()) {
        OffsetAndMetadata metadata = offsetAndMetadataMap.get(entry.getKey());
        if (metadata != null && entry.getValue().offset() > metadata.offset()) {
          consumer.seekToEnd(topicPartitions);
          return;
        }
      }
    }
  }

  private void runLatest() {
    System.out.println("Starting Latest!");
    try (KafkaConsumer<GenericKey, GenericRow> consumer = createConsumer(true);
        ConsumerMetadata consumerMetadata = getMetadata(consumer);
        LatestConsumer latestConsumer = new  LatestConsumer(consumerMetadata.getNumPartitions(),
            ksqlTopic.getKafkaTopicName(), consumer)) {
      this.latestConsumer.set(latestConsumer);
      while (!latestPendingQueues.isEmpty()) {
        ProcessingQueue pq = latestPendingQueues.poll();
        latestConsumer.register(pq);
      }

      // Initial wait time, giving client connections a chance to be made to avoid having to do
      // any catchups.
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      consumer.subscribe(ImmutableList.of(ksqlTopic.getKafkaTopicName()),
          new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
              System.out.println("Revoked assignment" + collection + " from " + latestConsumer);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
              System.out.println("Got assignment " + collection + " from " + latestConsumer);
              latestConsumer.newAssignment(collection);
              for (CatchupConsumer catchupConsumer : catchupConsumers.values()) {
                catchupConsumer.newAssignment(collection);
              }
            }
          });

      latestConsumer.start();
    } finally {
      synchronized (this) {
        this.latestConsumer.set(null);
        this.isLatestStarted = false;
      }
    }
  }

  class CatchupConsumer extends Consumer {

    private AtomicBoolean blocked = new AtomicBoolean(false);
    private boolean noLatestMode = false;

    public CatchupConsumer(int partitions, String topicName,
        KafkaConsumer<GenericKey, GenericRow> consumer) {
      super(partitions, topicName, consumer);
    }

    @Override
    public boolean onEmptyRecords() {
      return checkCaughtUp(consumer, blocked);
    }

    @Override
    public boolean afterCommit() {
      if (checkNearEnd(consumer)) {
        return false;
      }
      return checkCaughtUp(consumer, blocked);
    }

    @Override
    public void onNewAssignment() {
      if (isNoLatestMode()) {
        return;
      }
      Set<TopicPartition> tps = waitForNewAssignmentFromLatestConsumer();

      consumer.assign(tps);
      newAssignment = false;
    }

    private Set<TopicPartition> waitForNewAssignmentFromLatestConsumer() {
      Set<TopicPartition> tps = this.topicPartitions.get();
      while (tps == null) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        tps = this.topicPartitions.get();
      }
      return tps;
    }

    @Override
    public void afterFirstPoll() {
    }

    private boolean checkNearEnd(KafkaConsumer<GenericKey, GenericRow> consumer) {
      if (!isNoLatestMode()) {
        return false;
      }
      System.out.println("CHECKING Near end!!");
      Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(topicPartitions.get());
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions.get());
      System.out.println("Comparing " + offsets + " and " + endOffsets);
      for (TopicPartition tp : endOffsets.keySet()) {
        OffsetAndMetadata latestOam = offsets.get(tp);
        long endOffset = endOffsets.get(tp);
        if (endOffset - latestOam.offset() < 1000) {
          this.topicPartitions.set(null);
          consumer.unsubscribe();
          synchronized (ScalablePushRegistry.this) {
            if (!isLatestStarted) {
              isLatestStarted = true;
              executorService.submit(() -> {
                try {
                  runLatest();
                } catch (final Throwable t) {
                  t.printStackTrace();
                }
              });
            }
            setNoLatestMode(false);
          }
          onNewAssignment();
          return true;
        }
      }
      return false;
    }

    private boolean checkCaughtUp(KafkaConsumer<GenericKey, GenericRow> consumer, AtomicBoolean blocked) {
      System.out.println("CHECKING CAUGHT UP!!");
      Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));
      LatestConsumer lc = latestConsumer.get();
      if (lc == null) {
        return false;
      }
      Map<TopicPartition, OffsetAndMetadata> latestOffsets = lc.getCurrentOffsets();
      if (latestOffsets == null) {
        return false;
      }
      System.out.println("Comparing " + offsets + " and " + latestOffsets);
      if (caughtUp(latestOffsets, offsets)) {
        System.out.println("CAUGHT UP!!");

        synchronized (blockers) {
          if (waiting.get() && caughtUp(latestOffsets, offsets)) {
            System.out.println("TRANSFER");
            if (blocked.get()) {
              System.out.println("DECREMENTING BLOCKERS to " + blockers.get());
              blockers.decrementAndGet();
              blockers.notify();
            }
            for (final ProcessingQueue processingQueue : processingQueues.values()) {
              latestConsumer.get().register(processingQueue);
            }
            close();
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

    public void setNoLatestMode(boolean noLatestMode) {
      this.noLatestMode = noLatestMode;
    }

    public boolean isNoLatestMode() {
      return noLatestMode;
    }
  }

//        while (!consumer.listTopics().containsKey(ksqlTopic.getKafkaTopicName())) {
//    try {
//      Thread.sleep(100);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
//  }

  private void runCatchup(ProcessingQueue processingQueue, Optional<String> token) {
    try (KafkaConsumer<GenericKey, GenericRow> consumer = createConsumer(false);
        ConsumerMetadata consumerMetadata = getMetadata(consumer);
        CatchupConsumer catchupConsumer
            = new CatchupConsumer(consumerMetadata.getNumPartitions(), ksqlTopic.getKafkaTopicName(), consumer)) {
      catchupConsumers.put(processingQueue.getQueryId(), catchupConsumer);
      catchupConsumer.register(processingQueue);

      synchronized (this) {
        if (latestConsumer.get() != null) {
          catchupConsumer.newAssignment(latestConsumer.get().getAssignment());
          catchupConsumer.setNoLatestMode(false);
        } else {
          catchupConsumer.setNoLatestMode(true);
        }
      }
      if (!catchupConsumer.isNoLatestMode()) {
        catchupConsumer.onNewAssignment();
        Map<Integer, Long> startingOffsets = TokenUtils.parseToken(token);
        for (TopicPartition tp : consumer.assignment()) {
          consumer.seek(tp, startingOffsets.containsKey(tp.partition()) ? startingOffsets.get(tp.partition()): 0);
        }
      } else {
        consumer.subscribe(ImmutableList.of(ksqlTopic.getKafkaTopicName()),
            new ConsumerRebalanceListener() {
              @Override
              public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("CATCHUP: Revoked assignment" + collection);
              }

              @Override
              public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("CATCHUP:  Got assignment " + collection);
                catchupConsumer.newAssignment(collection);
                Map<Integer, Long> startingOffsets = TokenUtils.parseToken(token);
                for (TopicPartition tp : consumer.assignment()) {
                  consumer.seek(tp, startingOffsets.containsKey(tp.partition()) ? startingOffsets.get(tp.partition()): 0);
                }
              }
            });
      }
      catchupConsumer.start();
      catchupConsumer.unregister(processingQueue);
      catchupConsumers.remove(processingQueue.getQueryId());
    }
  }

  public abstract class Consumer implements AutoCloseable {

    private final int partitions;
    private final String topicName;
    protected KafkaConsumer<GenericKey, GenericRow> consumer;
    private Map<TopicPartition, Long> currentPositions = new HashMap<>();
    protected volatile boolean newAssignment = false;
    protected final ConcurrentHashMap<QueryId, ProcessingQueue> processingQueues
        = new ConcurrentHashMap<>();
    private volatile boolean closed = false;
    private boolean firstPoll = true;

    protected AtomicReference<Set<TopicPartition>> topicPartitions = new AtomicReference<>();
    private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> latestOffsets = new AtomicReference<>(null);

    public Consumer(
        final int partitions,
        final String topicName,
        final KafkaConsumer<GenericKey, GenericRow> consumer
    ) {
      this.partitions = partitions;
      this.topicName = topicName;
      this.consumer = consumer;
    }


    public abstract boolean onEmptyRecords();

    public abstract boolean afterCommit();

    public abstract void onNewAssignment();

    public abstract void afterFirstPoll();

    public void newAssignment(final Collection<TopicPartition> tps) {
      newAssignment = true;
      topicPartitions.set(ImmutableSet.copyOf(tps));
    }


    private void resetCurrentPosition(
        KafkaConsumer<GenericKey, GenericRow> consumer,
        Map<TopicPartition, Long> currentPositions
    ) {
      for (int i = 0; i < partitions; i++) {
        currentPositions.put(new TopicPartition(topicName, i), 0L);
      }
      for (TopicPartition tp : topicPartitions.get()) {
        currentPositions.put(tp, consumer.position(tp));
      }
    }

    public void start() {
      System.out.println("foo " + currentPositions);
      while (true) {
        if (closed) {
          return;
        }
        final ConsumerRecords<GenericKey, GenericRow> records = consumer.poll(Duration.ofMillis(1000));
        // No assignment yet
        if (this.topicPartitions.get() == null) {
          continue;
        }
        if (firstPoll) {
          firstPoll = false;
          afterFirstPoll();
        }
        if (records.isEmpty()) {
          onEmptyRecords();
          continue;
        }

        if (newAssignment) {
          newAssignment = false;
          resetCurrentPosition(consumer, currentPositions);
          onNewAssignment();
        }

        String startToken = TokenUtils.getToken(currentPositions, topicName, partitions);

        for (ConsumerRecord<GenericKey, GenericRow> rec : records) {
          System.out.println("Got record " + rec);
          System.out.println("KEY " + rec.key());
          System.out.println("VALUE " + rec.value());
          handleRow(processingQueues, rec.key(), rec.value(), rec.timestamp(), TokenUtils.getToken(currentPositions, rec.topic(), partitions));
        }
        for (TopicPartition tp : topicPartitions.get()) {
          currentPositions.put(tp, consumer.position(tp));
        }

        String endToken = TokenUtils.getToken(currentPositions, ksqlTopic.getKafkaTopicName(), partitions);

        System.out.println("Sending tokens start " + startToken + " end " + endToken);
        handleProgressToken(processingQueues, startToken, endToken);
        consumer.commitSync();

        Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(new HashSet<>(topicPartitions.get()));
        latestOffsets.set(ImmutableMap.copyOf(offsets));

        afterCommit();
      }
    }

    public void close() {
      for (final ProcessingQueue processingQueue : processingQueues.values()) {
        processingQueue.close();
      }
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }

    public void register(final ProcessingQueue processingQueue) {
      processingQueues.put(processingQueue.getQueryId(), processingQueue);
    }

    public void unregister(final ProcessingQueue processingQueue) {
      processingQueues.remove(processingQueue.getQueryId());
    }

    public Set<TopicPartition> getAssignment() {
      return topicPartitions.get();
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
      return latestOffsets.get();
    }

    public int numRegistered() {
      return processingQueues.size();
    }

    public void onError() {
      for (ProcessingQueue queue : processingQueues.values()) {
        queue.onError();
      }
    }
  }

  private static class ConsumerMetadata implements AutoCloseable {

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
  }
}
