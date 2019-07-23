/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterTerminator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTerminator.class);

  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;
  private final List<String> managedTopics;

  public ClusterTerminator(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final List<String> managedTopics
  ) {
    Objects.requireNonNull(ksqlConfig, "ksqlConfig is null.");
    Objects.requireNonNull(ksqlEngine, "ksqlEngine is null.");
    this.ksqlEngine = ksqlEngine;
    this.serviceContext = Objects.requireNonNull(serviceContext);
    this.managedTopics = ImmutableList.copyOf(
        Objects.requireNonNull(managedTopics, "managedTopics"));
  }

  public void terminateCluster(final List<String> deleteTopicPatterns) {
    terminatePersistentQueries();
    deleteSinkTopics(deleteTopicPatterns);
    deleteTopics(managedTopics);
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

    final List<DataSource<?>> toDelete = getSourcesToDelete(patterns, ksqlEngine.getMetaStore());

    deleteTopics(topicNames(toDelete));
    cleanUpSinkAvroSchemas(subjectNames(toDelete));
  }

  private List<String> filterNonExistingTopics(final Collection<String> topicList) {
    final Set<String> existingTopicNames = serviceContext.getTopicClient().listTopicNames();
    return topicList.stream().filter(existingTopicNames::contains).collect(Collectors.toList());
  }

  private void deleteTopics(final Collection<String> topicsToBeDeleted) {
    try {
      ExecutorUtil.executeWithRetries(
          () -> serviceContext.getTopicClient().deleteTopics(
              filterNonExistingTopics(topicsToBeDeleted)),
          ExecutorUtil.RetryBehaviour.ALWAYS);
    } catch (final TopicDeletionDisabledException e) {
      LOGGER.info("Did not delete any topics: ", e.getMessage());
    } catch (final Exception e) {
      throw new KsqlException(
          "Exception while deleting topics: " + StringUtils.join(topicsToBeDeleted, ", "));
    }
  }

  private void cleanUpSinkAvroSchemas(final Collection<String> subjectsToDelete) {
    final Set<String> knownSubject = SchemaRegistryUtil
        .getSubjectNames(serviceContext.getSchemaRegistryClient())
        .collect(Collectors.toSet());

    subjectsToDelete.retainAll(knownSubject);

    subjectsToDelete.forEach(this::deleteSubject);
  }

  private void deleteSubject(final String subject) {
    try {
      SchemaRegistryUtil.deleteSubjectWithRetries(
          serviceContext.getSchemaRegistryClient(), subject);
    } catch (final Exception e) {
      LOGGER.warn("Failed to clean up Avro schema for subject: " + subject, e);
    }
  }

  private static List<DataSource<?>> getSourcesToDelete(
      final List<Pattern> patterns,
      final MetaStore metaStore
  ) {
    final Predicate<String> predicate = topicName -> patterns.stream()
        .anyMatch(pattern -> pattern.matcher(topicName).matches());

    return metaStore.getAllDataSources().values().stream()
        .filter(s -> s.getKsqlTopic().isKsqlSink())
        .filter(s -> predicate.test(s.getKsqlTopic().getKafkaTopicName()))
        .collect(Collectors.toList());
  }

  private static Set<String> topicNames(final List<DataSource<?>> sources) {
    return sources.stream()
        .map(DataSource::getKsqlTopic)
        .map(KsqlTopic::getKafkaTopicName)
        .collect(Collectors.toSet());
  }

  private static Set<String> subjectNames(final List<DataSource<?>> sources) {
    return sources.stream()
        .filter(s -> s.getKsqlTopic().getValueSerdeFactory().getFormat() == Format.AVRO)
        .map(DataSource::getKsqlTopic)
        .map(KsqlTopic::getKafkaTopicName)
        .map(topicName -> topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX)
        .collect(Collectors.toSet());
  }
}
