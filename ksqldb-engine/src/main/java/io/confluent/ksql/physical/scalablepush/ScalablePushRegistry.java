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
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.physical.scalablepush.consumer.CatchupCoordinator;
import io.confluent.ksql.physical.scalablepush.consumer.ConsumerMetadata;
import io.confluent.ksql.physical.scalablepush.consumer.LatestConsumer;
import io.confluent.ksql.physical.scalablepush.consumer.NoopCatchupCoordinator;
import io.confluent.ksql.physical.scalablepush.locator.AllHostsLocator;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This registry is kept with every persistent query, peeking at the stream which is the output
 * of the topology. These rows are then fed to any registered ProcessingQueues where they are
 * eventually passed on to scalable push queries.
 */
public class ScalablePushRegistry {

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
  private boolean closed = false;
  private boolean isLatestStarted = false;
  private AtomicReference<LatestConsumer> latestConsumer = new AtomicReference<>(null);
  private CatchupCoordinator catchupCoordinator = new NoopCatchupCoordinator();
  private final ConcurrentLinkedQueue<ProcessingQueue> latestPendingQueues
      = new ConcurrentLinkedQueue<>();
  private boolean stopLatestConsumerOnLastRequest = true;

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
  }

  public synchronized void close() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.close();
    }
    try {
      executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while shutting down executor");
    }
    closed = true;
  }

  public synchronized void register(
      final ProcessingQueue processingQueue,
      final boolean expectingStartOfRegistryData
  ) {
    if (closed) {
      throw new IllegalStateException("Shouldn't register after closing");
    }
    if (isLatestStarted) {
      final LatestConsumer latestConsumer = this.latestConsumer.get();
      if (latestConsumer != null && !latestConsumer.isClosed()) {
        if (latestConsumer.getNumRowsReceived() > 0
            && newNodeContinuityEnforced && expectingStartOfRegistryData) {
          throw new IllegalStateException("New node missed data");
        }
        latestConsumer.register(processingQueue);
      } else {
        latestPendingQueues.add(processingQueue);
      }
    } else {
      latestPendingQueues.add(processingQueue);
      startLatestIfNotRunning();
    }
  }

  public synchronized void unregister(final ProcessingQueue processingQueue) {
    if (closed) {
      throw new IllegalStateException("Shouldn't unregister after closing");
    }
    System.out.println("UNREGISTERING " + processingQueue);
    latestPendingQueues.remove(processingQueue);
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.unregister(processingQueue);
      if (latestConsumer.numRegistered() == 0 && stopLatestConsumerOnLastRequest) {
        latestConsumer.close();
      }
    }
  }

  private synchronized void startLatestIfNotRunning() {
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

  @VisibleForTesting
  public synchronized void setKeepLatestConsumerOnLastRequest() {
    stopLatestConsumerOnLastRequest = false;
  }

  public synchronized void onError() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.onError();
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
    config.putAll(
        ksqlConfig.originalsWithPrefix(KsqlConfig.KSQL_QUERY_PUSH_V2_CONSUMER_PREFIX, true));
    //UUID.randomUUID().toString()
    config.put(ConsumerConfig.GROUP_ID_CONFIG, latest ? "spq_latest1": "spq_catchup");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
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
        LOG.error("Interrupted while looking for topic", e);
        Thread.currentThread().interrupt();
      }
    }
    int numPartitions = partitionInfo.get(ksqlTopic.getKafkaTopicName()).size();
    return new ConsumerMetadata(numPartitions);
  }

  private void runLatest() {
    System.out.println("Starting Latest!");
    try (KafkaConsumer<GenericKey, GenericRow> consumer = createConsumer(true);
        ConsumerMetadata consumerMetadata = getMetadata(consumer);
        LatestConsumer latestConsumer = new  LatestConsumer(consumerMetadata.getNumPartitions(),
            ksqlTopic.getKafkaTopicName(), windowed, logicalSchema, consumer, catchupCoordinator,
            tp -> {}, ksqlConfig)) {
      this.latestConsumer.set(latestConsumer);
      while (!latestPendingQueues.isEmpty()) {
        ProcessingQueue pq = latestPendingQueues.poll();
        latestConsumer.register(pq);
      }

      latestConsumer.run();
    } finally {
      synchronized (this) {
        this.latestConsumer.set(null);
        this.isLatestStarted = false;
      }
    }
  }
}
