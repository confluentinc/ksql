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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransientQueryCleanupService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(TransientQueryCleanupService.class);
  private static final Pattern LEAKED_TOPIC_PREFIX_PATTERN =
          Pattern.compile("(?i).*transient_.*_[0-9]\\d*_[0-9]\\d*");

  private final Pattern transientPattern;
  private final   Set<String> queriesGuaranteedToBeRunning;
  private Set<String> localCommandsQueryAppIds;
  private boolean isLocalCommandsInitialized;
  private boolean isLocalCommandsProcessed;
  private final Set<String> localCommandsTopics;
  private final Set<String> localCommandsStates;
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

    this.queriesGuaranteedToBeRunning = new HashSet<>();
    this.localCommandsTopics = new HashSet<>();
    this.localCommandsStates = new HashSet<>();

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
        localCommandsTopics.add(id);
        localCommandsStates.add(stateDir + id);
      });
      isLocalCommandsProcessed = true;
    }

    deleteLocalCommandsStates();
    deleteLocalCommandsTopics();

    LOG.info("Starting cleanup for leaked resources.");

    final List<String> leakedTopics = findPossiblyLeakedTransientTopics();
    final List<String> leakedStateDirs = findPossiblyLeakedStateDirs();

    LOG.info("Cleaning up {} leaked topics: {}", leakedTopics.size(), leakedTopics);
    leakedTopics.forEach(this::deleteLeakedTopic);

    LOG.info("Cleaning up {} leaked state directories: {}",
            leakedStateDirs.size(),
            leakedStateDirs);
    leakedStateDirs.forEach(this::deleteLeakedStateDir);
  }

  @Override
  public Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(initialDelay, intervalPeriod, TimeUnit.SECONDS);
  }

  public void setQueryRegistry(final QueryRegistry queryRegistry) {
    this.queryRegistry = queryRegistry;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public void setLocalCommandsQueryAppIds(final Set<String> ids) {
    this.localCommandsQueryAppIds = ids;
    this.isLocalCommandsInitialized = true;
  }

  private void deleteLocalCommandsTopics() {
    if (localCommandsTopics.isEmpty()) {
      return;
    }
    final Set<String> allKafkaTopics = serviceContext.getTopicClient().listTopicNames();
    final Set<String> cleanedUp = new HashSet<>();

    for (String topicPrefix: localCommandsTopics) {
      if (allKafkaTopics.stream().anyMatch(a -> a.startsWith(topicPrefix))) {
        deleteLeakedTopic(topicPrefix);
      } else {
        cleanedUp.add(topicPrefix);
      }
    }

    localCommandsTopics.removeAll(cleanedUp);
  }

  private void deleteLocalCommandsStates() {
    if (localCommandsStates.isEmpty()) {
      return;
    }
    final Set<String> cleanedUp = new HashSet<>();
    for (String localCommandState: localCommandsStates) {
      final Path pathName = Paths.get(localCommandState);
      final File directory = new File(String.valueOf(pathName.normalize()));
      if (directory.exists()) {
        deleteLeakedStateDir(localCommandState);
      } else {
        cleanedUp.add(localCommandState);
      }
    }
    localCommandsStates.removeAll(cleanedUp);
  }

  private List<String> findPossiblyLeakedStateDirs() {
    final String stateDir = this.stateDir;
    final File folder = new File(stateDir);
    final File[] listOfFiles = folder.listFiles();

    if (listOfFiles == null) {
      return Collections.emptyList();
    }

    final List<String> leakedStates
            = new LinkedList<>();

    for (File f: listOfFiles) {
      final String fileName = f.getName();
      final Matcher filenameMatcher = transientPattern.matcher(fileName);
      if (filenameMatcher.find()
              && isCorrespondingQueryTerminated(fileName)
              && queriesGuaranteedToBeRunning.contains(fileName)) {
        final String leakedState = stateDir + "/" + fileName;
        LOG.info("{} seems to be a leaked state directory. Adding it to the cleanup queue.",
                leakedState);

        leakedStates.add(leakedState);
      }
    }

    return leakedStates;
  }

  private List<String> findPossiblyLeakedTransientTopics() {
    final List<String> leakedQueries = new LinkedList<>();

    for (String topic : this.serviceContext
            .getTopicClient()
            .listTopicNames()) {
      final Matcher topicNameMatcher = transientPattern.matcher(topic);
      if (topicNameMatcher.find() && isCorrespondingQueryTerminated(topic)) {
        final Matcher topicPrefixMatcher = LEAKED_TOPIC_PREFIX_PATTERN.matcher(topic);
        if (topicPrefixMatcher.find()
                && queriesGuaranteedToBeRunning.contains(topicPrefixMatcher.group())) {
          final String leakedTopicPrefix = topicPrefixMatcher.group();

          LOG.info("{} topic seems to have leaked. Adding it to the cleanup queue.", topic);
          leakedQueries.add(leakedTopicPrefix);
        }
      }
    }

    return leakedQueries;
  }

  private boolean isCorrespondingQueryTerminated(final String topic) {
    return this.queryRegistry
            .getAllLiveQueries()
            .stream()
            .map(qm -> qm.getQueryId().toString())
            .noneMatch(topic::contains);
  }

  public void queryIsRunning(final String appId) {
    queriesGuaranteedToBeRunning.add(appId);
  }

  private void deleteLeakedTopic(final String topicPrefix) {
    serviceContext.getTopicClient().deleteInternalTopics(topicPrefix);
  }

  private void deleteLeakedStateDir(final String filename) {
    final Path pathName = Paths.get(filename);
    try {
      deleteIfExists(pathName);
    } catch (IOException e) {
      LOG.info("Transient Query Cleanup Service failed "
               + "to delete leaked state directory: " + filename, e);
    }
  }
}
