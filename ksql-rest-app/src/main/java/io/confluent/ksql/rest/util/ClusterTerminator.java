/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.util;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterTerminator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTerminator.class);

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;

  public ClusterTerminator(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext
  ) {
    Objects.requireNonNull(ksqlConfig, "ksqlConfig is null.");
    Objects.requireNonNull(ksqlEngine, "ksqlEngine is null.");
    this.ksqlConfig = ksqlConfig;
    this.ksqlEngine = ksqlEngine;
    this.serviceContext = Objects.requireNonNull(serviceContext);
  }

  public void terminateCluster(final List<String> deleteTopicPatterns) {
    terminatePersistentQueries();
    deleteSinkTopics(deleteTopicPatterns);
    deleteCommandTopic();
    ksqlEngine.close();
  }

  private void terminatePersistentQueries() {
    ksqlEngine.getPersistentQueries().forEach(QueryMetadata::close);
  }

  private void deleteSinkTopics(final List<String> deleteTopicPatterns) {
    if (deleteTopicPatterns.isEmpty()) {
      return;
    }

    final List<Pattern> patterns = deleteTopicPatterns.stream()
        .map(Pattern::compile)
        .collect(Collectors.toList());

    final MetaStore metaStore = ksqlEngine.getMetaStore();
    final List<String> toDelete = metaStore.getAllKsqlTopics().values().stream()
        .filter(KsqlTopic::isKsqlSink)
        .map(KsqlTopic::getKafkaTopicName)
        .filter(topicName -> topicShouldBeDeleted(topicName, patterns))
        .collect(Collectors.toList());
    deleteTopics(toDelete);
    cleanUpSinkAvroSchemas(toDelete);
  }

  private List<String> filterNonExistingTopics(final List<String> topicList) {
    final Set<String> existingTopicNames = serviceContext.getTopicClient().listTopicNames();
    return topicList.stream().filter(existingTopicNames::contains).collect(Collectors.toList());
  }

  private static boolean topicShouldBeDeleted(
      final String topicName, final List<Pattern> patterns
  ) {
    return patterns.stream()
        .anyMatch(pattern -> pattern.matcher(topicName).matches());
  }

  private void deleteCommandTopic() {
    final String ksqlServiceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final String commandTopic = KsqlRestConfig.getCommandTopic(ksqlServiceId);
    deleteTopics(Collections.singletonList(commandTopic));
  }

  private void deleteTopics(final List<String> topicsToBeDeleted) {
    try {
      ExecutorUtil.executeWithRetries(
          () -> serviceContext.getTopicClient().deleteTopics(
              filterNonExistingTopics(topicsToBeDeleted)),
          ExecutorUtil.RetryBehaviour.ALWAYS);
    } catch (final Exception e) {
      throw new KsqlException(
          "Exception while deleting topics: " + StringUtils.join(topicsToBeDeleted, ","));
    }
  }

  private void cleanUpSinkAvroSchemas(final List<String> topicsToBeDeleted) {
    final MetaStore metaStore = ksqlEngine.getMetaStore();
    final Stream<StructuredDataSource> avroSourcesToCleanUp = topicsToBeDeleted.stream()
        .map(metaStore::getSourceForTopic)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(StructuredDataSource::isAvroSerialized);
    final Stream<String> subjectsToDelete = avroSourcesToCleanUp
        .map(source -> source.getName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
    filterNonExistingSubjects(subjectsToDelete).forEach(this::deleteSubject);
  }

  private Stream<String> filterNonExistingSubjects(final Stream<String> subjects) {
    final Set<String> existingSubjects =
        SchemaRegistryUtil.getSubjectNames(serviceContext.getSchemaRegistryClient())
            .collect(Collectors.toSet());
    return subjects.filter(existingSubjects::contains);
  }

  private void deleteSubject(final String subject) {
    try {
      SchemaRegistryUtil.deleteSubjectWithRetries(
          serviceContext.getSchemaRegistryClient(), subject);
    } catch (final Exception e) {
      LOGGER.warn("Failed to clean up Avro schema for subject: " + subject, e);
    }
  }
}
