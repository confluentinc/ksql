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

import static io.confluent.ksql.util.QueryApplicationId.buildInternalTopicPrefix;
import static java.nio.file.Files.deleteIfExists;

import com.google.common.util.concurrent.AbstractScheduledService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.query.QueryRegistry;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransientQueryCleanupService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(TransientQueryCleanupService.class);
  private final Pattern internalTopicPrefixPattern;
  private final Pattern transientAppIdPattern;
  private final Set<String> queriesGuaranteedToBeRunningAtSomePoint;
  private final String stateDir;
  private final KafkaTopicClient topicClient;
  private final int initialDelay;
  private final int intervalPeriod;
  private Optional<Set<String>> localCommandsQueryAppIds;
  private QueryRegistry queryRegistry;
  private int numLeakedTopics;
  private int numLeakedStateDirs;
  private int numLeakedTopicsFailedToCleanUp;
  private int numLeakedStateDirsFailedToCleanUp;

  public TransientQueryCleanupService(final ServiceContext serviceContext,
                                      final KsqlConfig ksqlConfig) {
    final String internalTopicPrefix = buildInternalTopicPrefix(ksqlConfig, false);
    this.internalTopicPrefixPattern = Pattern.compile(internalTopicPrefix);
    this.transientAppIdPattern = Pattern.compile(internalTopicPrefix + ".*_[0-9]\\d*_[0-9]\\d*");

    this.initialDelay = ksqlConfig.getInt(
            KsqlConfig.KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS);

    this.intervalPeriod = ksqlConfig.getInt(
            KsqlConfig.KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS);

    this.stateDir = ksqlConfig.getKsqlStreamConfigProps()
            .getOrDefault(
                    StreamsConfig.STATE_DIR_CONFIG,
                    StreamsConfig.configDef()
                            .defaultValues()
                            .get(StreamsConfig.STATE_DIR_CONFIG))
            .toString();

    this.topicClient = serviceContext.getTopicClient();
    this.localCommandsQueryAppIds = Optional.empty();
    this.queriesGuaranteedToBeRunningAtSomePoint = new HashSet<>();
    this.numLeakedTopics = 0;
    this.numLeakedStateDirs = 0;
  }

  @Override
  protected void runOneIteration() {
    findAndDeleteLeakedTopics();
    findAndDeleteLeakedStateDirs();
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
    this.localCommandsQueryAppIds = Optional.of(ids);
  }

  private void findAndDeleteLeakedTopics() {
    try {
      final List<String> leakedTopics = findLeakedTopics();
      this.numLeakedTopics = leakedTopics.size();
      LOG.info("Cleaning up {} leaked topics: {}", numLeakedTopics, leakedTopics);
      getTopicClient().deleteTopics(leakedTopics);
      this.numLeakedTopicsFailedToCleanUp = findLeakedTopics().size();
    } catch (Throwable t) {
      LOG.error(
              "Failed to clean up topics with exception: " + t.getMessage(), t);
    }
  }

  private void findAndDeleteLeakedStateDirs() {
    try {
      final List<String> leakedStateDirs = findLeakedStateDirs();
      this.numLeakedStateDirs = leakedStateDirs.size();
      LOG.info("Cleaning up {} leaked state directories: {}",
              numLeakedStateDirs,
              leakedStateDirs.stream().map(file -> stateDir + "/" + file)
                      .collect(Collectors.toList()));
      leakedStateDirs.forEach(this::deleteLeakedStateDir);
      this.numLeakedStateDirsFailedToCleanUp = findLeakedStateDirs().size();
    } catch (Throwable t) {
      LOG.error(
              "Failed to clean up state directories with exception: " + t.getMessage(), t);
    }
  }

  private void deleteLeakedStateDir(final String filename) {
    final String path = stateDir + "/" + filename;
    final Path pathName = Paths.get(path);
    try {
      deleteIfExists(pathName);
    } catch (IOException e) {
      LOG.info("Transient Query Cleanup Service failed "
              + "to delete leaked state directory: " + path, e);
    }
  }

  List<String> findLeakedTopics() {
    return getTopicClient()
            .listTopicNames()
            .stream()
            .filter(this::isLeaked)
            .collect(Collectors.toList());
  }

  List<String> findLeakedStateDirs() {
    return listAllStateFiles()
            .stream()
            .filter(this::isLeaked)
            .collect(Collectors.toList());
  }

  List<String> listAllStateFiles() {
    final File folder = new File(stateDir);
    final File[] listOfFiles = folder.listFiles();

    if (listOfFiles == null) {
      return Collections.emptyList();
    }
    return  Arrays.stream(listOfFiles)
            .map(File::getName)
            .collect(Collectors.toList());
  }

  boolean isLeaked(final String resource) {
    if (foundInLocalCommands(resource)) {
      return true;
    }
    if (!internalTopicPrefixPattern.matcher(resource).find()) {
      return false;
    }
    if (!isCorrespondingQueryTerminated(resource)) {
      return false;
    }
    final Matcher appIdMatcher = transientAppIdPattern.matcher(resource);
    if (appIdMatcher.find()) {
      return wasQueryGuaranteedToBeRunningAtSomePoint(appIdMatcher.group());
    }

    return false;
  }

  boolean isCorrespondingQueryTerminated(final String appId) {
    return this.queryRegistry
            .getAllLiveQueries()
            .stream()
            .map(qm -> qm.getQueryId().toString())
            .noneMatch(appId::contains);
  }

  public void registerRunningQuery(final String appId) {
    queriesGuaranteedToBeRunningAtSomePoint.add(appId);
  }

  boolean wasQueryGuaranteedToBeRunningAtSomePoint(final String appId) {
    return queriesGuaranteedToBeRunningAtSomePoint.contains(appId);
  }

  boolean foundInLocalCommands(final String resourceName) {
    return localCommandsQueryAppIds
            .map(strings -> strings.stream().anyMatch(resourceName::contains))
            .orElse(false);
  }

  KafkaTopicClient getTopicClient() {
    return topicClient;
  }

  String getStateDir() {
    return stateDir;
  }

  public int getNumLeakedTopics() {
    return numLeakedTopics;
  }

  public int getNumLeakedStateDirs() {
    return numLeakedStateDirs;
  }

  public int getNumLeakedTopicsFailedToCleanUp() {
    return  numLeakedTopicsFailedToCleanUp;
  }

  public int getNumLeakedStateDirsFailedToCleanUp() {
    return numLeakedStateDirsFailedToCleanUp;
  }
}
