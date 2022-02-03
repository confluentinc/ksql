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

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.spun.util.io.FileUtils;
import io.confluent.ksql.query.QueryRegistry;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.File;
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
  private static final Pattern TRANSIENT_PATTERN = Pattern.compile("(?i).*transient_.*_[0-9]\\d*_[0-9]\\d*");

  private final BlockingQueue<Callable<Boolean>> cleanupTasks;
  private final Retryer<Boolean> retryer;
  private final String stateDir;
  private final ServiceContext serviceContext;
  private QueryRegistry queryRegistry;
  private Set<TransientQueryMetadata> allTransientQueriesEverCreated;
  private Set<String> allTransientQueriesEverCreatedIds;


  public TransientQueryCleanupService(final ServiceContext serviceContext,
                                      final KsqlConfig ksqlConfig) {
    cleanupTasks = new LinkedBlockingDeque<>();

    retryer = RetryerBuilder.<Boolean>newBuilder()
            .retryIfResult(aBoolean -> Objects.equals(aBoolean, false))
            .retryIfException()
            .withStopStrategy(StopStrategies.stopAfterAttempt(5))
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
    LOG.info("Starting cleanup for transient topics: {}; size: {}", cleanupTasks, cleanupTasks.size());
    findCleanupTasks();
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
    return Scheduler.newFixedRateSchedule(20, 10, TimeUnit.SECONDS);
  }

  private void findCleanupTasks() {
    LOG.info("lads we have created {} so far", allTransientQueriesEverCreatedIds);
    findPossiblyLeakedStateDirs();
    findPossiblyLeakedTranientTopics();
  }

  public void addCleanupTask(final Callable<Boolean> task) {
    LOG.info("Adding cleanup task");
    cleanupTasks.add(task);
    LOG.info("size after adding: {}", cleanupTasks.size());
  }

  private void findPossiblyLeakedStateDirs() {
    final String stateDir = this.stateDir;
    File folder = new File(stateDir);
    File[] listOfFiles = folder.listFiles();
    LOG.info("LIsting all the transient state files: ");
    for (File f: listOfFiles) {
      String fileName = f.getName();
      Matcher m = TRANSIENT_PATTERN.matcher(fileName);
      if (m.find()) {
        String leakingQueryId = m.group(0);
        if (allTransientQueriesEverCreatedIds.contains(leakingQueryId)) {
          boolean isRunning =  allTransientQueriesEverCreated.stream().filter(queryMetadata -> queryMetadata.getQueryApplicationId().equals(leakingQueryId)).collect(Collectors.toList()).get(0).isRunning();
          if (isRunning) {
            LOG.info("kk {} is still running, don't try to clean", leakingQueryId);
          } else {
            LOG.info("man, {} is leaking. Adding to cleanup", leakingQueryId);
            addCleanupTask(new TransientQueryStateCleanupTask(
                    serviceContext,
                    leakingQueryId,
                    stateDir));
          }
        } else {
          LOG.info("couldnt find {} in the registry", leakingQueryId);
        }
      }
    }
  }

  private void findPossiblyLeakedTranientTopics() {
    LOG.info("Listing out all the transient topic names: ");
    for (String topic : this.serviceContext
            .getTopicClient()
            .listTopicNames()) {
      Matcher m = TRANSIENT_PATTERN.matcher(topic);
      if (m.find()) {
        String leakingQueryId = m.group(0);
        if (allTransientQueriesEverCreatedIds.contains(leakingQueryId)) {
          boolean isRunning = allTransientQueriesEverCreated.stream().filter(queryMetadata -> queryMetadata.getQueryApplicationId().equals(leakingQueryId)).collect(Collectors.toList()).get(0).isRunning();
          if (isRunning) {
            LOG.info("kk {} is still running, don't try to clean", leakingQueryId);
          } else {
            LOG.info("man, {} is leaking. Adding to cleanup", leakingQueryId);
            addCleanupTask(new TransientQueryTopicCleanupTask(
                    serviceContext,
                    leakingQueryId
            ));
          }
        } else {
          LOG.info("couldnt find {} in the registry", leakingQueryId);
        }
      }
    }
  }

  public void recordTransientQueryOnStart(TransientQueryMetadata tqmd) {
    this.allTransientQueriesEverCreated.add(tqmd);
    this.allTransientQueriesEverCreatedIds.add(tqmd.getQueryApplicationId());
  }

  public void setQueryRegistry(QueryRegistry queryRegistry) {
    this.queryRegistry = queryRegistry;
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
    private final ServiceContext serviceContext;
    private final String appId;
    private final String pathName;

    public TransientQueryStateCleanupTask(
            final ServiceContext serviceContext,
            final String appId,
            final String stateDir
    ) {
      this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
      this.appId = Objects.requireNonNull(appId, "appId");
      this.pathName = stateDir + "/" + appId;
    }

    @Override
    public Boolean call() {
      final Path pathName = Paths.get(this.pathName);
      final File directory = new File(String.valueOf(pathName.normalize()));
      try {
        if (directory.exists()) {
          LOG.warn("Deleting local state store for non-existing query {}. "
                          + "This is not expected and was likely due to a "
                          + "race condition when the query was dropped before.",
                  appId);
          FileUtils.deleteDirectory(directory);
        } else {
          return true;
        }
      } catch (Exception e) {
        LOG.error("Error cleaning up state directory {}\n. {}", pathName, e);
        return false;
      }
      if (directory.exists()) {
        LOG.warn("Failed to delete local state store for non-existing query: {}",
                appId);
        return false;
      } else {
        return true;
      }
    }
  }
}
