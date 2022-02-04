/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.engine;

import static java.nio.file.Files.deleteIfExists;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.util.concurrent.AbstractScheduledService;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransientQueryCleanupService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(TransientQueryCleanupService.class);
  private static final Pattern TRANSIENT_PATTERN =
          Pattern.compile("(?i).*transient_.*_[0-9]\\d*_[0-9]\\d*");
  private static final int NUM_OF_RETRIES_TO_DELETE_RESOURCE = 5;

  private final BlockingQueue<Callable<Boolean>> cleanupTasks;
  private final Retryer<Boolean> retryer;
  private final String stateDir;
  private final ServiceContext serviceContext;
  private final Set<TransientQueryMetadata> allTransientQueriesEverCreated;
  private final Set<String> allTransientQueriesEverCreatedIds;
  private final int initialDelay;
  private final int intervalPeriod;


  public TransientQueryCleanupService(final ServiceContext serviceContext,
                                      final KsqlConfig ksqlConfig) {
    this.initialDelay = ksqlConfig.getInt(
            KsqlConfig.KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS);

    this.intervalPeriod = ksqlConfig.getInt(
            KsqlConfig.KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS);

    cleanupTasks = new LinkedBlockingDeque<>();

    retryer = RetryerBuilder.<Boolean>newBuilder()
            .retryIfResult(aBoolean -> Objects.equals(aBoolean, false))
            .retryIfExceptionOfType(IOException.class)
            .retryIfRuntimeException()
            .withStopStrategy(StopStrategies.stopAfterAttempt(NUM_OF_RETRIES_TO_DELETE_RESOURCE))
            .build();

    this.stateDir = ksqlConfig.getKsqlStreamConfigProps()
            .getOrDefault(
                    StreamsConfig.STATE_DIR_CONFIG,
                    StreamsConfig.configDef()
                            .defaultValues()
                            .get(StreamsConfig.STATE_DIR_CONFIG))
            .toString();
    this.serviceContext = serviceContext;
    this.allTransientQueriesEverCreated = new HashSet<>();
    this.allTransientQueriesEverCreatedIds = new HashSet<>();
  }

  @Override
  protected void runOneIteration() {
    LOG.info("Starting cleanup for leaked resources.");

    findCleanupTasks();

    final long numLeakedTopics = cleanupTasks.stream()
            .filter(p -> p.getClass().equals(TransientQueryTopicCleanupTask.class))
            .count();

    final long numLeakedStateDirs = cleanupTasks.stream()
            .filter(p -> p.getClass().equals(TransientQueryStateCleanupTask.class))
            .count();

    LOG.info("Cleaning up {} leaked topics.", numLeakedTopics);
    LOG.info("Cleaning up {} leaked state directories.", numLeakedStateDirs);
    try {
      while (!cleanupTasks.isEmpty()) {
        final Callable<Boolean> task = cleanupTasks.take();

        try {
          retryer.call(task);
        } catch (RetryException | ExecutionException e) {
          cleanupTasks.add(task);
        }
      }
    } catch (final InterruptedException e) {
      // gracefully exit if this method was interrupted and reset
      // the interrupt flag
      Thread.currentThread().interrupt();
    }
  }

  public Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(initialDelay, intervalPeriod, TimeUnit.SECONDS);
  }

  private void findCleanupTasks() {
    LOG.info("Searching for leaked resources.");
    findPossiblyLeakedStateDirs();
    findPossiblyLeakedTranientTopics();
  }

  public void addCleanupTask(final Callable<Boolean> task) {
    cleanupTasks.add(task);
  }

  private void findPossiblyLeakedStateDirs() {
    final String stateDir = this.stateDir;
    final File folder = new File(stateDir);
    final File[] listOfFiles = folder.listFiles();

    if (listOfFiles == null) {
      return;
    }

    for (File f: listOfFiles) {
      final String fileName = f.getName();
      final Matcher filenameMatcher = TRANSIENT_PATTERN.matcher(fileName);

      if (filenameMatcher.find()) {
        final String leakingQueryId = filenameMatcher.group(0);
        if (allTransientQueriesEverCreatedIds.contains(leakingQueryId)
                && isTerminatedQuery(leakingQueryId)) {
          LOG.info("{} is leaking state directories. Adding it to the cleanup queue.",
                  leakingQueryId);

          addCleanupTask(new TransientQueryStateCleanupTask(
                  leakingQueryId,
                  stateDir)
          );
        }
      }
    }
  }

  private void findPossiblyLeakedTranientTopics() {
    for (String topic : this.serviceContext
            .getTopicClient()
            .listTopicNames()) {
      final Matcher topicNameMatcher = TRANSIENT_PATTERN.matcher(topic);

      if (topicNameMatcher.find()) {
        final String leakingQueryId = topicNameMatcher.group(0);
        if (allTransientQueriesEverCreatedIds.contains(leakingQueryId)
                && isTerminatedQuery(leakingQueryId)) {
          LOG.info("{} is leaking topics. Adding it to the cleanup queue.", leakingQueryId);
          addCleanupTask(new TransientQueryTopicCleanupTask(
                  serviceContext,
                  leakingQueryId
          ));
        }
      }
    }
  }

  public void recordTransientQueryOnStart(final TransientQueryMetadata queryMetadata) {
    this.allTransientQueriesEverCreated.add(queryMetadata);
    this.allTransientQueriesEverCreatedIds.add(queryMetadata.getQueryApplicationId());
  }

  public static class TransientQueryTopicCleanupTask implements Callable<Boolean> {

    private final ServiceContext serviceContext;
    private final String appId;

    public TransientQueryTopicCleanupTask(
            final ServiceContext serviceContext,
            final String appId
    ) {
      this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
      this.appId = Objects.requireNonNull(appId, "appId");
    }

    @Override
    public Boolean call() {
      try {
        LOG.info("Deleting topics for transient query: {}", appId);
        serviceContext.getTopicClient().deleteInternalTopics(appId);
      } catch (final Exception e) {
        LOG.warn("Failed to cleanup topics for transient query: {}", appId, e);
        return false;
      }

      final Set<String> orphanedTopics = serviceContext
              .getTopicClient()
              .listTopicNames()
              .stream()
              .filter(topicName -> topicName.startsWith(appId))
              .collect(Collectors.toSet());

      if (orphanedTopics.isEmpty()) {
        return true;
      }

      LOG.warn("Failed to cleanup topics for transient query: {}\n"
                      + "The following topics seem to have leaked: {}",
              appId,
              orphanedTopics);

      return true;
    }
  }

  public static class TransientQueryStateCleanupTask implements Callable<Boolean> {
    private final String appId;
    private final String pathName;

    public TransientQueryStateCleanupTask(
            final String appId,
            final String stateDir
    ) {
      this.appId = Objects.requireNonNull(appId, "appId");
      this.pathName = stateDir + "/" + appId;
    }

    @Override
    public Boolean call() throws IOException {
      final Path pathName = Paths.get(this.pathName);
      final File directory = new File(String.valueOf(pathName.normalize()));
      deleteIfExists(pathName);
      if (directory.exists()) {
        LOG.warn("Failed to delete local state store for non-existing query: {}",
                appId);
        return false;
      } else {
        return true;
      }
    }
  }

  private boolean isTerminatedQuery(final String queryId) {
    return !allTransientQueriesEverCreated
            .stream()
            .filter(
                    queryMetadata ->
                            queryMetadata
                                    .getQueryApplicationId()
                                    .equals(queryId)
            )
            .collect(Collectors.toList())
            .get(0)
            .isRunning();
  }
}
