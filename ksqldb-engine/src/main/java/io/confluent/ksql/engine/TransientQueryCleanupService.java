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

import com.google.common.util.concurrent.AbstractScheduledService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.query.QueryRegistry;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
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
  private final Pattern transientPattern;
  private static final Pattern LEAKED_TOPIC_PREFIX_PATTERN =
          Pattern.compile("(?i).*transient_.*_[0-9]\\d*_[0-9]\\d*");

  private final BlockingQueue<Callable<Boolean>> stateDirsCleanupTasks;
  private final BlockingQueue<Callable<Boolean>> topicsCleanupTasks;
  private Set<String> localCommandsQueryAppIds;
  private boolean isLocalCommandsInitialized;
  private boolean isLocalCommandsProcessed;
  private QueryRegistry queryRegistry;
  private final String stateDir;
  private final ServiceContext serviceContext;
  private final int initialDelay;
  private final int intervalPeriod;


  public TransientQueryCleanupService(final ServiceContext serviceContext,
                                      final KsqlConfig ksqlConfig) {
    this.transientPattern = Pattern.compile(
            ksqlConfig.getString(
                    KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG));

    this.initialDelay = ksqlConfig.getInt(
            KsqlConfig.KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS);

    this.intervalPeriod = ksqlConfig.getInt(
            KsqlConfig.KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS);

    topicsCleanupTasks = new LinkedBlockingDeque<>();
    stateDirsCleanupTasks = new LinkedBlockingDeque<>();

    this.stateDir = ksqlConfig.getKsqlStreamConfigProps()
            .getOrDefault(
                    StreamsConfig.STATE_DIR_CONFIG,
                    StreamsConfig.configDef()
                            .defaultValues()
                            .get(StreamsConfig.STATE_DIR_CONFIG))
            .toString();

    this.serviceContext = serviceContext;
    this.isLocalCommandsInitialized = false;
    this.isLocalCommandsProcessed = false;
  }

  @Override
  protected void runOneIteration() {
    if (isLocalCommandsInitialized && !isLocalCommandsProcessed) {
      LOG.info("Adding LocalCommands to TransientQueryCleanupService.");
      localCommandsQueryAppIds.forEach(id -> {
        addTopicCleanupTask(new TransientQueryTopicCleanupTask(serviceContext, id));
        addStateCleanupTask(new TransientQueryStateCleanupTask(id, stateDir));
      });
      isLocalCommandsProcessed = true;
    }

    LOG.info("Starting cleanup for leaked resources.");

    topicsCleanupTasks.addAll(findPossiblyLeakedTransientTopics());
    stateDirsCleanupTasks.addAll(findPossiblyLeakedStateDirs());

    final int numLeakedTopics = topicsCleanupTasks.size();
    final int numLeakedStateDirs = stateDirsCleanupTasks.size();

    LOG.info("Cleaning up {} leaked topics.", numLeakedTopics);
    cleanupLeakedTopics();

    LOG.info("Cleaning up {} leaked state directories.", numLeakedStateDirs);
    cleanupLeakedStateDirs();
  }

  @Override
  public Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(initialDelay, intervalPeriod, TimeUnit.SECONDS);
  }

  public void cleanupLeakedTopics() {
    try {
      while (!topicsCleanupTasks.isEmpty()) {
        final Callable<Boolean> task = topicsCleanupTasks.take();

        try {
          final boolean cleanedUp = task.call();
          if (!cleanedUp) {
            topicsCleanupTasks.add(task);
          }
        } catch (Exception e) {
          topicsCleanupTasks.add(task);
        }
      }
    } catch (final InterruptedException e) {
      // gracefully exit if this method was interrupted and reset
      // the interrupt flag
      Thread.currentThread().interrupt();
    }
  }

  public void cleanupLeakedStateDirs() {
    try {
      while (!stateDirsCleanupTasks.isEmpty()) {
        final Callable<Boolean> task = stateDirsCleanupTasks.take();

        try {
          final boolean cleanedUp = task.call();
          if (!cleanedUp) {
            stateDirsCleanupTasks.add(task);
          }
        } catch (Exception e) {
          stateDirsCleanupTasks.add(task);
        }
      }
    } catch (final InterruptedException e) {
      // gracefully exit if this method was interrupted and reset
      // the interrupt flag
      Thread.currentThread().interrupt();
    }
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public void setLocalCommandsQueryAppIds(final Set<String> ids) {
    this.localCommandsQueryAppIds = ids;
    this.isLocalCommandsInitialized = true;
  }

  public void setQueryRegistry(final QueryRegistry queryRegistry) {
    this.queryRegistry = queryRegistry;
  }

  public void addTopicCleanupTask(final TransientQueryTopicCleanupTask task) {
    topicsCleanupTasks.add(task);
  }

  public void addStateCleanupTask(final TransientQueryStateCleanupTask task) {
    stateDirsCleanupTasks.add(task);
  }

  private List<TransientQueryStateCleanupTask> findPossiblyLeakedStateDirs() {
    final String stateDir = this.stateDir;
    final File folder = new File(stateDir);
    final File[] listOfFiles = folder.listFiles();

    if (listOfFiles == null) {
      return Collections.emptyList();
    }

    final List<TransientQueryStateCleanupTask> leakedStates
            = new LinkedList<>();

    for (File f: listOfFiles) {
      final String fileName = f.getName();
      final Matcher filenameMatcher = transientPattern.matcher(fileName);
      if (filenameMatcher.find() && !isCorrespondingQueryRunning(f.getName())) {
        LOG.info("{} seems to be a leaked state directory. Adding it to the cleanup queue.",
                f.getName());

        leakedStates.add(new TransientQueryStateCleanupTask(
                f.getName(),
                stateDir)
        );
      }
    }

    return leakedStates;
  }

  private List<TransientQueryTopicCleanupTask> findPossiblyLeakedTransientTopics() {
    final List<TransientQueryTopicCleanupTask> leakedQueries
            = new LinkedList<>();

    for (String topic : this.serviceContext
            .getTopicClient()
            .listTopicNames()) {
      final Matcher topicNameMatcher = transientPattern.matcher(topic);
      if (topicNameMatcher.find() && !isCorrespondingQueryRunning(topic)) {
        LOG.info("{} topic seems to have leaked. Adding it to the cleanup queue.", topic);
        final Matcher topicPrefixMatcher = LEAKED_TOPIC_PREFIX_PATTERN.matcher(topic);
        if (topicPrefixMatcher.find()) {
          final String leakedTopicPrefix = topicPrefixMatcher.group();
          leakedQueries.add(new TransientQueryTopicCleanupTask(
                  serviceContext,
                  leakedTopicPrefix
          ));
        }
      }
    }

    return leakedQueries;
  }

  private boolean isCorrespondingQueryRunning(final String topic) {
    return this.queryRegistry
            .getAllLiveQueries()
            .stream()
            .map(qm -> qm.getQueryId().toString())
            .anyMatch(topic::contains);
  }

  public static class TransientQueryTopicCleanupTask implements Callable<Boolean> {

    private final ServiceContext serviceContext;
    private final String leakedTopicPrefix;

    public TransientQueryTopicCleanupTask(
            final ServiceContext serviceContext,
            final String leakedTopicPrefix
    ) {
      this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
      this.leakedTopicPrefix = Objects.requireNonNull(leakedTopicPrefix, "queryId");
    }

    @Override
    public Boolean call() {
      try {
        LOG.info("Deleting topics for transient query: {}", leakedTopicPrefix);
        serviceContext.getTopicClient().deleteInternalTopics(leakedTopicPrefix);
      } catch (final Exception e) {
        LOG.warn("Failed to cleanup topics for transient query: {}", leakedTopicPrefix, e);
        return false;
      }

      final Set<String> orphanedTopics = serviceContext
              .getTopicClient()
              .listTopicNames()
              .stream()
              .filter(topicName -> topicName.startsWith(leakedTopicPrefix))
              .collect(Collectors.toSet());

      if (orphanedTopics.isEmpty()) {
        return true;
      }

      LOG.warn("Failed to cleanup topics for transient query: {}\n"
                      + "The following topics seem to have leaked: {}",
              leakedTopicPrefix,
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
}
