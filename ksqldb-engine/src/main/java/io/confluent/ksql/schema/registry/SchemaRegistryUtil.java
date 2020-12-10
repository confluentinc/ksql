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

package io.confluent.ksql.schema.registry;

import static io.confluent.ksql.util.ExecutorUtil.RetryBehaviour.ALWAYS;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchemaRegistryUtil {

  @VisibleForTesting
  public static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryUtil.class);

  private static final String CHANGE_LOG_SUFFIX = KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX
      + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

  private static final String REPARTITION_SUFFIX = KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX
      + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

  private SchemaRegistryUtil() {
  }

  public static void cleanupInternalTopicSchemas(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    getInternalSubjectNames(applicationId, schemaRegistryClient)
        .forEach(subject -> tryDeleteInternalSubject(applicationId, schemaRegistryClient, subject));
  }

  public static Stream<String> getSubjectNames(final SchemaRegistryClient schemaRegistryClient) {
    return getSubjectNames(
        schemaRegistryClient,
        "Could not get subject names from schema registry.");
  }

  private static Stream<String> getSubjectNames(
      final SchemaRegistryClient schemaRegistryClient, final String errorMsg) {
    try {
      return schemaRegistryClient.getAllSubjects().stream();
    } catch (final Exception e) {
      LOG.warn(errorMsg, e);
      return Stream.empty();
    }
  }

  public static void deleteSubjectWithRetries(
      final SchemaRegistryClient schemaRegistryClient,
      final String subject) throws Exception {
    ExecutorUtil.executeWithRetries(() -> schemaRegistryClient.deleteSubject(subject), ALWAYS);
  }

  private static Stream<String> getInternalSubjectNames(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final Stream<String> allSubjectNames = getSubjectNames(
        schemaRegistryClient,
        "Could not clean up the schema registry for query: " + applicationId);
    return allSubjectNames
        .filter(subjectName -> subjectName.startsWith(applicationId))
        .filter(subjectName ->
            subjectName.endsWith(CHANGE_LOG_SUFFIX) || subjectName.endsWith(REPARTITION_SUFFIX));
  }

  private static void tryDeleteInternalSubject(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient,
      final String subjectName
  ) {
    try {
      deleteSubjectWithRetries(schemaRegistryClient, subjectName);
    } catch (final Exception e) {
      LOG.warn("Could not clean up the schema registry for"
          + " query: " + applicationId
          + ", subject: " + subjectName, e);
    }
  }

  public static boolean subjectExists(
      final SchemaRegistryClient srClient,
      final String subject
  ) {
    return getLatestSchema(srClient, subject).isPresent();
  }

  private static Optional<SchemaMetadata> getLatestSchema(
      final SchemaRegistryClient srClient,
      final String subject
  ) {
    try {
      final SchemaMetadata schemaMetadata = srClient.getLatestSchemaMetadata(subject);
      return Optional.ofNullable(schemaMetadata);
    } catch (final Exception e) {
      if (isSubjectNotFoundErrorCode(e)) {
        return Optional.empty();
      }
      throw new KsqlException("Could not get latest schema for subject " + subject, e);
    }
  }

  public static boolean isSubjectNotFoundErrorCode(final Throwable error) {
    return (error instanceof RestClientException
        && ((RestClientException) error).getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE);
  }
}