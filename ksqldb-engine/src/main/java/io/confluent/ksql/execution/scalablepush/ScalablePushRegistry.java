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

package io.confluent.ksql.execution.scalablepush;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.scalablepush.consumer.CatchupConsumer;
import io.confluent.ksql.execution.scalablepush.consumer.CatchupConsumer.CatchupConsumerFactory;
import io.confluent.ksql.execution.scalablepush.consumer.CatchupCoordinator;
import io.confluent.ksql.execution.scalablepush.consumer.CatchupCoordinatorImpl;
import io.confluent.ksql.execution.scalablepush.consumer.KafkaConsumerFactory;
import io.confluent.ksql.execution.scalablepush.consumer.KafkaConsumerFactory.KafkaConsumerFactoryInterface;
import io.confluent.ksql.execution.scalablepush.consumer.LatestConsumer;
import io.confluent.ksql.execution.scalablepush.consumer.LatestConsumer.LatestConsumerFactory;
import io.confluent.ksql.execution.scalablepush.locator.AllHostsLocator;
import io.confluent.ksql.execution.scalablepush.locator.PushLocator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PushOffsetRange;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
  private static final String CATCHUP_CONSUMER_GROUP_MIDDLE = "_scalable_push_query_catchup_";

  private final PushLocator pushLocator;
  private final LogicalSchema logicalSchema;
  private final boolean isTable;
  private final Map<String, Object> consumerProperties;
  private final KsqlTopic ksqlTopic;
  private final ServiceContext serviceContext;
  private final KsqlConfig ksqlConfig;
  private final String sourceApplicationId;
  private final KafkaConsumerFactoryInterface kafkaConsumerFactory;
  private final LatestConsumerFactory latestConsumerFactory;
  private final CatchupConsumerFactory catchupConsumerFactory;
  private final ExecutorService executorService;
  private final ScheduledExecutorService executorServiceCatchup;

  // If the registry is closed.  Should only happen on server shutdown.
  @GuardedBy("this")
  private boolean closed = false;
  // Once the latest consumer is created, this is a reference to it.
  private AtomicReference<LatestConsumer> latestConsumer = new AtomicReference<>(null);
  // The catchup coordinator used by the latest consumer.
  private CatchupCoordinator catchupCoordinator = new CatchupCoordinatorImpl();
  private Map<QueryId, CatchupConsumer> catchupConsumers = new ConcurrentHashMap<>();
  // If the latest consumer should be stopped when there are no more requests to serve. True by
  // default, this prevent reading unnecessary data from Kafka when not serving any requests.
  @GuardedBy("this")
  private boolean stopLatestConsumerOnLastRequest = true;

  @SuppressWarnings("ParameterNumber")
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public ScalablePushRegistry(
      final PushLocator pushLocator,
      final LogicalSchema logicalSchema,
      final boolean isTable,
      final Map<String, Object> consumerProperties,
      final KsqlTopic ksqlTopic,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final String sourceApplicationId,
      final KafkaConsumerFactoryInterface kafkaConsumerFactory,
      final LatestConsumerFactory latestConsumerFactory,
      final CatchupConsumerFactory catchupConsumerFactory,
      final ExecutorService executorService,
      final ScheduledExecutorService executorServiceCatchup
  ) {
    this.pushLocator = pushLocator;
    this.logicalSchema = logicalSchema;
    this.isTable = isTable;
    this.consumerProperties = consumerProperties;
    this.ksqlTopic = ksqlTopic;
    this.serviceContext = serviceContext;
    this.ksqlConfig = ksqlConfig;
    this.sourceApplicationId = sourceApplicationId;
    this.kafkaConsumerFactory = kafkaConsumerFactory;
    this.latestConsumerFactory = latestConsumerFactory;
    this.catchupConsumerFactory = catchupConsumerFactory;
    this.executorService = executorService;
    this.executorServiceCatchup = executorServiceCatchup;
  }

  /**
   * Called when the server is shutting down.
   */
  public synchronized void close() {
    if (closed) {
      LOG.warn("Already closed registry");
      return;
    }
    LOG.info("Closing scalable push registry for topic " + ksqlTopic.getKafkaTopicName());
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null) {
      latestConsumer.closeAsync();
    }
    for (CatchupConsumer catchupConsumer : catchupConsumers.values()) {
      catchupConsumer.closeAsync();
    }
    catchupConsumers.clear();

    MoreExecutors.shutdownAndAwaitTermination(executorService, 5000, TimeUnit.MILLISECONDS);
    MoreExecutors.shutdownAndAwaitTermination(executorServiceCatchup, 5000, TimeUnit.MILLISECONDS);

    closed = true;
  }

  /**
   * Called when a persistent query which this is associated with is dropped.
   */
  public synchronized void cleanup() {
    // Close if we haven't already
    close();
    deleteConsumerGroup(getLatestConsumerGroupId());
  }

  public synchronized boolean isClosed() {
    return closed;
  }

  /**
   * Registers a ProcessingQueue with this scalable push registry so that it starts receiving
   * data as it streams in for that consumer.
   * @param processingQueue The queue to register
   * @param catchupMetadata The catchup metadata including offsets to start at
   */
  public synchronized void register(
      final ProcessingQueue processingQueue,
      final Optional<CatchupMetadata> catchupMetadata
  ) {
    if (closed) {
      throw new IllegalStateException("Shouldn't register after closing");
    }
    try {
      if (catchupMetadata.isPresent()) {
        if (catchupConsumers.size()
            >= ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS)) {
          processingQueue.onError();
          throw new KsqlException("Too many catchups registered, "
              + KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS
              + ":" + ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS));
        }
        // Latest must be running for a catchup consumer to exist.
        startLatestIfNotRunning(Optional.empty());
        startCatchup(processingQueue, catchupMetadata.get());
      } else {
        final LatestConsumer latestConsumer = this.latestConsumer.get();
        if (latestConsumer != null && !latestConsumer.isClosed()) {
          latestConsumer.register(processingQueue);
        } else {
          // If latestConsumer is null, that means it's the first time.  If latestConsumer != null
          // but it's already closed, that means that it's stopping async, so we just let it
          // finish while creating a new one.
          startLatestIfNotRunning(Optional.of(processingQueue));
        }
      }
    } finally {
      // Make sure that if anything in the creation process throws an exception, that we stop the
      // latest if there are no existing queries.
      stopLatestConsumerOnLastRequest();
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
    try {
      final LatestConsumer latestConsumer = this.latestConsumer.get();
      if (latestConsumer != null && !latestConsumer.isClosed()) {
        latestConsumer.unregister(processingQueue);
      }
      unregisterCatchup(processingQueue);
    } finally {
      // Make sure we stop latest after any unregisters if necessary. We don't except the above to
      // throw any exceptions, but it's wrapped in a finally to be safe.
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
  public synchronized boolean isLatestRunning() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    return latestConsumer != null && !latestConsumer.isClosed();
  }

  @VisibleForTesting
  public synchronized boolean anyCatchupsRunning() {
    return catchupConsumers.size() > 0;
  }

  @VisibleForTesting
  public int numRegistered() {
    return latestNumRegistered() + catchupNumRegistered();
  }

  @VisibleForTesting
  public synchronized int latestNumRegistered() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null && !latestConsumer.isClosed()) {
      return latestConsumer.numRegistered();
    }
    return 0;
  }

  @VisibleForTesting
  public synchronized long latestNumRowsReceived() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null && !latestConsumer.isClosed()) {
      return latestConsumer.getNumRowsReceived();
    }
    return 0;
  }

  @VisibleForTesting
  public synchronized int catchupNumRegistered() {
    int total = 0;
    for (CatchupConsumer catchupConsumer : catchupConsumers.values()) {
      total += catchupConsumer.numRegistered();
    }
    return total;
  }

  @VisibleForTesting
  public synchronized boolean latestHasAssignment() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null && !latestConsumer.isClosed()) {
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
        pushLocator, logicalSchema, isTable,
        consumerProperties, ksqlTopic, serviceContext, ksqlConfig, sourceApplicationId,
        KafkaConsumerFactory::create, LatestConsumer::new, CatchupConsumer::new,
        Executors.newSingleThreadExecutor(),
        Executors.newScheduledThreadPool(
            ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_CATCHUP_CONSUMERS))));
  }

  /**
   * If there are no more latest requests, closes the consumer.
   */
  private synchronized boolean stopLatestConsumerOnLastRequest() {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null && !latestConsumer.isClosed()) {
      if (latestConsumer.numRegistered() == 0 && stopLatestConsumerOnLastRequest
          && catchupConsumers.size() == 0) {
        latestConsumer.closeAsync();
        return true;
      }
    }
    return false;
  }

  /**
   * Creates the latest consumer and its underlying kafka consumer.
   * @param processingQueue The queue on which to send an error if anything goes wrong
   * @return The new LatestConsumer
   */
  private LatestConsumer createLatestConsumer(final Optional<ProcessingQueue> processingQueue) {
    KafkaConsumer<Object, GenericRow> consumer = null;
    LatestConsumer latestConsumer = null;
    try {
      consumer = kafkaConsumerFactory.create(
          ksqlTopic, logicalSchema, serviceContext, consumerProperties, ksqlConfig,
          getLatestConsumerGroupId());
      latestConsumer = latestConsumerFactory.create(
          ksqlTopic.getKafkaTopicName(), isWindowed(),
          logicalSchema, consumer, catchupCoordinator,
          this::catchupAssignmentUpdater, ksqlConfig, Clock.systemUTC());
      return latestConsumer;
    } catch (Exception e) {
      LOG.error("Couldn't create latest consumer", e);
      processingQueue.ifPresent(ProcessingQueue::onError);
      // We're not supposed to block here, but if it fails here, hopefully it can immediately close.
      if (consumer != null) {
        consumer.close();
      }
      throw e;
    }
  }

  /**
   * Starts the Latest consumer if it's not started already.
   * @param processingQueue The queue to register with the latest consumer
   */
  private synchronized void startLatestIfNotRunning(
      final Optional<ProcessingQueue> processingQueue) {
    final LatestConsumer latestConsumer = this.latestConsumer.get();
    if (latestConsumer != null && !latestConsumer.isClosed()) {
      return;
    }
    final LatestConsumer newLatestConsumer = createLatestConsumer(processingQueue);
    processingQueue.ifPresent(newLatestConsumer::register);
    this.latestConsumer.set(newLatestConsumer);
    executorService.submit(() -> runLatest(newLatestConsumer));
  }

  /**
   * Runs the latest consumer passed in. Note that it's possible that by the time this runs, this
   * may have been closed and there may be a new latest, so we don't take from
   * this.latestConsumer.get(), but rather process the consumers in the order in which they were
   * run.
   * @param latestConsumerToRun The latest consumer to run
   */
  private void runLatest(final LatestConsumer latestConsumerToRun) {
    try (LatestConsumer latestConsumer = latestConsumerToRun) {
      latestConsumer.run();
    } catch (Throwable t) {
      LOG.error("Got error while running latest", t);
      latestConsumerToRun.onError();
    }
  }

  private String getLatestConsumerGroupId() {
    return sourceApplicationId + LATEST_CONSUMER_GROUP_SUFFIX;
  }

  private synchronized void catchupAssignmentUpdater(
      final Collection<TopicPartition> newAssignment
  ) {
    for (CatchupConsumer catchupConsumer : catchupConsumers.values()) {
      catchupConsumer.newAssignment(newAssignment);
    }
  }

  /**
   * Creates the latest consumer and its underlying kafka consumer.
   * @param processingQueue The queue on which to send an error if anything goes wrong
   * @return The new LatestConsumer
   */
  private CatchupConsumer createCatchupConsumer(
      final ProcessingQueue processingQueue,
      final PushOffsetRange offsetRange,
      final String consumerGroup
  ) {
    KafkaConsumer<Object, GenericRow> consumer = null;
    CatchupConsumer catchupConsumer = null;
    try {
      consumer = kafkaConsumerFactory.create(
          ksqlTopic, logicalSchema, serviceContext, consumerProperties, ksqlConfig,
          consumerGroup);
      catchupConsumer = catchupConsumerFactory.create(ksqlTopic.getKafkaTopicName(), isWindowed(),
          logicalSchema, consumer, latestConsumer::get, catchupCoordinator, offsetRange,
          Clock.systemUTC(),
          ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_PUSH_V2_CATCHUP_CONSUMER_MSG_WINDOW),
          this::unregisterCatchup);
      return catchupConsumer;
    } catch (Exception e) {
      LOG.error("Couldn't create catchup consumer", e);
      processingQueue.onError();
      // We're not supposed to block here, but if it fails here, hopefully it can immediately close.
      if (consumer != null) {
        consumer.close();
      }
      throw e;
    }
  }

  /**
   * Unregisters catchups either after they have run to completion and caught up, or even before
   * they've completed.
   * @param processingQueue The queue for the catchup query
   */
  private synchronized void unregisterCatchup(final ProcessingQueue processingQueue) {
    catchupConsumers
        .computeIfPresent(processingQueue.getQueryId(), (queryId, catchupConsumer) -> {
          catchupConsumer.unregister(processingQueue);
          catchupConsumer.closeAsync();
          return null;
        });
  }

  /**
   * Starts the catchup consumer
   * @param processingQueue The queue to register with the catchup consumer.
   * @param catchupMetadata The catchup metadata
   */
  private synchronized void startCatchup(
      final ProcessingQueue processingQueue,
      final CatchupMetadata catchupMetadata
  ) {
    final CatchupConsumer catchupConsumer = createCatchupConsumer(processingQueue,
        catchupMetadata.getPushOffsetRange(),
        catchupMetadata.getCatchupConsumerGroup());
    catchupConsumer.register(processingQueue);
    catchupConsumers.put(processingQueue.getQueryId(), catchupConsumer);
    executorServiceCatchup.submit(() -> runCatchup(catchupConsumer, processingQueue));
  }

  private void runCatchup(
      final CatchupConsumer catchupConsumerToRun,
      final ProcessingQueue processingQueue
  ) {
    try (CatchupConsumer catchupConsumer = catchupConsumerToRun) {
      catchupConsumer.run();
    } catch (Throwable t) {
      LOG.error("Got error while running catchup", t);
      catchupConsumerToRun.onError();
    } finally {
      catchupConsumerToRun.unregister(processingQueue);
      catchupConsumers.remove(processingQueue.getQueryId());
      // If things ended exceptionally, stop latest
      stopLatestConsumerOnLastRequest();
    }
  }

  public void cleanupCatchupConsumer(final String consumerGroup) {
    executorServiceCatchup.schedule(() -> {
      deleteConsumerGroup(consumerGroup);
    }, 5000L, TimeUnit.MILLISECONDS);
  }

  public String getCatchupConsumerId(final String suffix) {
    return sourceApplicationId + CATCHUP_CONSUMER_GROUP_MIDDLE + suffix;
  }

  private void deleteConsumerGroup(final String consumerGroupId) {
    LOG.info("Cleaning up consumer group " + consumerGroupId);
    try {
      final Map<TopicPartition, OffsetAndMetadata> metadata =
          serviceContext
              .getConsumerGroupClient()
              .listConsumerGroupOffsets(consumerGroupId);
      if (metadata == null || metadata.values().stream().allMatch(Objects::isNull)) {
        return;
      }
      serviceContext
          .getConsumerGroupClient()
          .deleteConsumerGroups(ImmutableSet.of(consumerGroupId));
    } catch (Throwable t) {
      LOG.error("Failed to delete consumer group " + consumerGroupId, t);
    }
  }

  public static class CatchupMetadata {

    private final PushOffsetRange pushOffsetRange;
    private final String catchupConsumerGroup;

    public CatchupMetadata(
        final PushOffsetRange pushOffsetRange,
        final String catchupConsumerGroup
    ) {
      this.pushOffsetRange = pushOffsetRange;
      this.catchupConsumerGroup = catchupConsumerGroup;
    }

    public PushOffsetRange getPushOffsetRange() {
      return pushOffsetRange;
    }

    public String getCatchupConsumerGroup() {
      return catchupConsumerGroup;
    }
  }
}
