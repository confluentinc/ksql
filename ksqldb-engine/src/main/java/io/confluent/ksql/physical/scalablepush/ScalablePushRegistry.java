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
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.physical.scalablepush.consumer.CatchupCoordinator;
import io.confluent.ksql.physical.scalablepush.consumer.ConsumerMetadata;
import io.confluent.ksql.physical.scalablepush.consumer.ConsumerMetadata.ConsumerMetadataFactory;
import io.confluent.ksql.physical.scalablepush.consumer.KafkaConsumerFactory;
import io.confluent.ksql.physical.scalablepush.consumer.KafkaConsumerFactory.KafkaConsumerFactoryInterface;
import io.confluent.ksql.physical.scalablepush.consumer.LatestConsumer;
import io.confluent.ksql.physical.scalablepush.consumer.LatestConsumer.LatestConsumerFactory;
import io.confluent.ksql.physical.scalablepush.consumer.NoopCatchupCoordinator;
import io.confluent.ksql.physical.scalablepush.locator.AllHostsLocator;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This registry is kept with every persistent query. The LatestConsumer started by this registry
 * reads from the persistent query's output topic, reading rows. These rows are then fed to any
 * registered ProcessingQueues where they are eventually passed on to scalable push queries.
 */
public class ScalablePushRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ScalablePushRegistry.class);
  private static final String LATEST_CONSUMER_GROUP_SUFFIX = "_scalable_push_query_latest";

  private final PushLocator pushLocator;
  private final LogicalSchema logicalSchema;
  private final boolean isTable;
  private final boolean newNodeContinuityEnforced;
  private final Map<String, Object> consumerProperties;
  private final KsqlTopic ksqlTopic;
  private final ServiceContext serviceContext;
  private final KsqlConfig ksqlConfig;
  private final String sourceApplicationId;
  private final KafkaConsumerFactoryInterface kafkaConsumerFactory;
  private final LatestConsumerFactory latestConsumerFactory;
  private final ConsumerMetadataFactory consumerMetadataFactory;
  private final ExecutorService executorService;

  /**
   * All of the following fields are synchronized by this.
   */

  // If the registry is closed.  Should only happen on server shutdown.
  private boolean closed = false;
  // Indicates that the latest consumer has been started and is running.
  private boolean isLatestStarted = false;
  // Once the latest consumer is created, this is a reference to it.
  private AtomicReference<LatestConsumer> latestConsumer = new AtomicReference<>(null);
  // The catchup coordinator used by the latest consumer.
  private CatchupCoordinator catchupCoordinator = new NoopCatchupCoordinator();
  // These are the ProcessingQueues which have been registered between the time that latest has
  // been kicked off but before the LatestConsumer has yet been created and run.
  private final LatestPendingQueues latestPendingQueues = new LatestPendingQueues();
  // If the latest consumer should be stopped when there are no more requests to serve. True by
  // default, this prevent reading unnecessary data from Kafka when not serving any requests.
  private boolean stopLatestConsumerOnLastRequest = true;

  @SuppressWarnings("ParameterNumber")
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public ScalablePushRegistry(
      final PushLocator pushLocator,
      final LogicalSchema logicalSchema,
      final boolean isTable,
      final boolean newNodeContinuityEnforced,
      final Map<String, Object> consumerProperties,
      final KsqlTopic ksqlTopic,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final String sourceApplicationId,
      final KafkaConsumerFactoryInterface kafkaConsumerFactory,
      final LatestConsumerFactory latestConsumerFactory,
      final ConsumerMetadataFactory consumerMetadataFactory,
      final ExecutorService executorService
  ) {
    this.pushLocator = pushLocator;
    this.logicalSchema = logicalSchema;
    this.isTable = isTable;
    this.newNodeContinuityEnforced = newNodeContinuityEnforced;
    this.consumerProperties = consumerProperties;
    this.ksqlTopic = ksqlTopic;
    this.serviceContext = serviceContext;
    this.ksqlConfig = ksqlConfig;
    this.sourceApplicationId = sourceApplicationId;
    this.kafkaConsumerFactory = kafkaConsumerFactory;
    this.latestConsumerFactory = latestConsumerFactory;
    this.consumerMetadataFactory = consumerMetadataFactory;
    this.executorService = executorService;
  }

  /**
   * Called when the server is shutting down.
   */
  public synchronized void close() {
    if (closed) {
      LOG.info("Already closed registry");
      return;
    }
    latestPendingQueues.close();
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.close();
    }

    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted during shutdown", e);
      executorService.shutdownNow();
    }

    closed = true;
  }

  /**
   * Called when a persistent query which this is associated with is dropped.
   */
  public synchronized void cleanup() {
    // Close if we haven't already
    close();
    try {
      serviceContext
          .getConsumerGroupClient()
          .deleteConsumerGroups(ImmutableSet.of(getLatestConsumerGroupId()));
    } catch (Throwable t) {
      LOG.error("Failed to delete consumer group " + getLatestConsumerGroupId(), t);
    }
  }

  public synchronized boolean isClosed() {
    return closed;
  }

  /**
   * Registers a ProcessingQueue with this scalable push registry so that it starts receiving
   * data as it streams in for that consumer.
   * @param processingQueue The queue to register
   * @param expectingStartOfRegistryData Whether the request is expecting a new server without
   *                                     rows received.
   */
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
          LOG.warn("Request missed data with new node added to the cluster");
          throw new KsqlException("New node missed data");
        }
        latestConsumer.register(processingQueue);
      } else {
        // Either latest is just starting up or just ending.  Either way, we'll be sure to pick it
        // up if we enqueue it.
        latestPendingQueues.add(processingQueue);
      }
    } else {
      latestPendingQueues.add(processingQueue);
      startLatestIfNotRunning();
    }
  }

  /**
   * Unregisters the given ProcessingQueue when its request is complete
   * @param processingQueue The queue to deregister
   */
  public synchronized void unregister(final ProcessingQueue processingQueue) {
    if (closed) {
      throw new IllegalStateException("Shouldn't unregister after closing");
    }
    latestPendingQueues.remove(processingQueue);
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.unregister(processingQueue);
      stopLatestConsumerOnLastRequest();
    }
  }

  public PushLocator getLocator() {
    return pushLocator;
  }

  public boolean isTable() {
    return isTable;
  }

  public boolean isWindowed() {
    return ksqlTopic.getKeyFormat().isWindowed();
  }

  @VisibleForTesting
  public synchronized boolean isLatestStarted() {
    return isLatestStarted;
  }

  @VisibleForTesting
  public int numRegistered() {
    return latestNumRegistered();
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
      final Map<String, Object> streamsProperties,
      final boolean newNodeContinuityEnforced,
      final Map<String, Object> consumerProperties,
      final String sourceApplicationId,
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
        pushLocator, logicalSchema, isTable, newNodeContinuityEnforced,
        consumerProperties, ksqlTopic, serviceContext, ksqlConfig, sourceApplicationId,
        KafkaConsumerFactory::create, LatestConsumer::create, ConsumerMetadata::create,
        Executors.newSingleThreadExecutor()));
  }

  /**
   * Starts the latest
   */
  private synchronized void startLatestIfNotRunning() {
    if (!isLatestStarted) {
      isLatestStarted = true;
      executorService.submit(this::runLatest);
    }
  }

  /**
   * If there are no more latest requests, closes the consumer.
   */
  private synchronized boolean stopLatestConsumerOnLastRequest() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      if (latestConsumer.numRegistered() == 0 && stopLatestConsumerOnLastRequest) {
        latestConsumer.close();
        return true;
      }
    }
    return false;
  }

  private void runLatest() {
    try (KafkaConsumer<Object, GenericRow> consumer = kafkaConsumerFactory.create(
        ksqlTopic, logicalSchema, serviceContext, consumerProperties, ksqlConfig,
        getLatestConsumerGroupId());
        ConsumerMetadata consumerMetadata = consumerMetadataFactory.create(
            ksqlTopic.getKafkaTopicName(), consumer);
        LatestConsumer latestConsumer = latestConsumerFactory.create(
            consumerMetadata.getNumPartitions(), ksqlTopic.getKafkaTopicName(), isWindowed(),
            logicalSchema, consumer, catchupCoordinator,
            tp -> { }, ksqlConfig, Clock.systemUTC())) {
      try {
        synchronized (this) {
          this.latestConsumer.set(latestConsumer);
          // This following block should never error since registering just adds to a map, so
          // the error handling code below assumes that it all succeeded.
          latestPendingQueues.foreachPoll(latestConsumer::register);
          // Now that everything is setup, make sure that if a close happened during
          // startup it gets registered.
          if (closed) {
            latestConsumer.close();
            return;
          }
          // It's also possible that requests were unregistered by the time we started and there's
          // no need to run anymore.
          if (stopLatestConsumerOnLastRequest()) {
            return;
          }
        }

        latestConsumer.run();
      } catch (final Throwable t) {
        LOG.error("Got error while running latest", t);
        // These errors aren't considered fatal, so we don't set the hasError flag since retrying
        // could cause recovery.
        latestConsumer.onError();
      }
    } catch (final Throwable t) {
      LOG.error("Got an error while handling another error", t);
      synchronized (this) {
        latestPendingQueues.sendErrorAndRemoveAll();
      }
    } finally {
      synchronized (this) {
        this.latestConsumer.set(null);
        this.isLatestStarted = false;

        // If more requests have been queued up since we started to close, then kick off a new
        // latest.
        if (latestPendingQueues.size() > 0) {
          LOG.info("Continuing with a new lastest just as we shut down the existing");
          startLatestIfNotRunning();
        }
      }
    }
  }

  private String getLatestConsumerGroupId() {
    return sourceApplicationId + LATEST_CONSUMER_GROUP_SUFFIX;
  }

  /**
   * Manages all of the pending ProcessingQueues that come in before the LatestConsumer is started.
   */
  private static class LatestPendingQueues {
    private final LinkedList<ProcessingQueue> latestPendingQueues
        = new LinkedList<>();

    public void sendErrorAndRemoveAll() {
      while (!latestPendingQueues.isEmpty()) {
        final ProcessingQueue processingQueue = latestPendingQueues.poll();
        processingQueue.onError();
      }
    }

    public void foreachPoll(final Consumer<ProcessingQueue> consumer) {
      while (!latestPendingQueues.isEmpty()) {
        consumer.accept(latestPendingQueues.poll());
      }
    }

    public void add(final ProcessingQueue pq) {
      latestPendingQueues.add(pq);
    }

    public void close() {
      while (!latestPendingQueues.isEmpty()) {
        latestPendingQueues.poll().close();
      }
    }

    public void remove(final ProcessingQueue pq) {
      latestPendingQueues.remove(pq);
    }

    public int size() {
      return latestPendingQueues.size();
    }
  }
}
