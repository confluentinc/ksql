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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.physical.scalablepush.consumer.KafkaConsumerFactory;
import io.confluent.ksql.physical.scalablepush.consumer.KafkaConsumerFactory.KafkaConsumerFactoryInterface;
import io.confluent.ksql.physical.scalablepush.consumer.CatchupCoordinator;
import io.confluent.ksql.physical.scalablepush.consumer.ConsumerMetadata;
import io.confluent.ksql.physical.scalablepush.consumer.ConsumerMetadata.ConsumerMetadataFactory;
import io.confluent.ksql.physical.scalablepush.consumer.LatestConsumer;
import io.confluent.ksql.physical.scalablepush.consumer.LatestConsumer.LatestConsumerFactory;
import io.confluent.ksql.physical.scalablepush.consumer.NoopCatchupCoordinator;
import io.confluent.ksql.physical.scalablepush.locator.AllHostsLocator;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
  private final KafkaConsumerFactoryInterface kafkaConsumerFactory;
  private final LatestConsumerFactory latestConsumerFactory;
  private final ConsumerMetadataFactory consumerMetadataFactory;
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
      final KsqlConfig ksqlConfig,
      final KafkaConsumerFactoryInterface kafkaConsumerFactory,
      final LatestConsumerFactory latestConsumerFactory,
      final ConsumerMetadataFactory consumerMetadataFactory
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
    this.kafkaConsumerFactory = kafkaConsumerFactory;
    this.latestConsumerFactory = latestConsumerFactory;
    this.consumerMetadataFactory = consumerMetadataFactory;
    this.executorService = Executors.newSingleThreadExecutor();
  }

  public synchronized void close() {
    System.out.println("Closing SQP registry");
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.close();
    }
    executorService.shutdownNow();
    closed = true;
    System.out.println("DONE Closing SQP registry");
  }

  public synchronized boolean isClosed() {
    return closed;
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
      executorService.submit(this::runLatest);
    }
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
  public synchronized boolean isLatestStarted() {
    return isLatestStarted;
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

  /**
   * Called when the underlying persistent query has an error.  Since we're now decoupled from the
   * query, we can safely ignore, just as conventional push query might.
   */
  public synchronized void onError() {
    // Ignore
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
    return Optional.of(new ScalablePushRegistry(
        pushLocator, logicalSchema, isTable, windowed, newNodeContinuityEnforced,
        consumerProperties, ksqlTopic, serviceContext, ksqlConfig,
        KafkaConsumerFactory::create, LatestConsumer::create, ConsumerMetadata::create));
  }

  private void runLatest() {
    System.out.println("Starting Latest2!");
    try (KafkaConsumer<Object, GenericRow> consumer = kafkaConsumerFactory.create(
        ksqlTopic, logicalSchema, serviceContext, consumerProperties, ksqlConfig, true);
        ConsumerMetadata consumerMetadata = consumerMetadataFactory.create(
            ksqlTopic.getKafkaTopicName(), consumer);
        LatestConsumer latestConsumer = latestConsumerFactory.create(
            consumerMetadata.getNumPartitions(),
            ksqlTopic.getKafkaTopicName(), windowed, logicalSchema, consumer, catchupCoordinator,
            tp -> {}, ksqlConfig, Clock.systemUTC())) {
      try {
        System.out.println("Main block");
        this.latestConsumer.set(latestConsumer);
        while (!latestPendingQueues.isEmpty()) {
          final ProcessingQueue pq = latestPendingQueues.peek();
          latestConsumer.register(pq);
          latestPendingQueues.poll();
        }
        // Now that everything is setup, make sure that if a close that happened during
        // startup that it gets registered.
        synchronized (this) {
          if (closed) {
            latestConsumer.close();
            return;
          }
        }

        System.out.println("ABOUT TO RUN");
        latestConsumer.run();
      } catch (final Throwable t) {
        LOG.error("Got error while running latest", t);
        // These errors aren't considered fatal, so we don't set the hasError flag since retrying
        // could cause recovery.
        latestConsumer.onError();
        if (!latestPendingQueues.isEmpty()) {
          for (final ProcessingQueue processingQueue : latestPendingQueues) {
            processingQueue.onError();
          }
          latestPendingQueues.clear();
        }
      }
    } catch (final Throwable t) {
      LOG.error("Got an error while handling another error", t);
    } finally {
      synchronized (this) {
        System.out.println("FINALLY BLOCK");
        this.latestConsumer.set(null);
        this.isLatestStarted = false;
      }
    }
  }
}
