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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.AbstractKafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.kafka.common.acl.AclOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchemaRegistryUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryUtil.class);

  private static final Pattern DENIED_OPERATION_STRING_PATTERN =
      Pattern.compile("User is denied operation (.*) on .*");

  @VisibleForTesting
  public static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

  private SchemaRegistryUtil() {
  }

  public static void cleanupInternalTopicSchemas(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient) {
    getInternalSubjectNames(applicationId, schemaRegistryClient)
        .forEach(subject -> tryDeleteInternalSubject(
            applicationId,
            schemaRegistryClient,
            subject));
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
    try {
      ExecutorUtil.executeWithRetries(
          () -> schemaRegistryClient.deleteSubject(subject),
          error -> isRetriableError(error)
      );
    } catch (final RestClientException e) {
      if (isAuthErrorCode(e)) {
        throw new KsqlSchemaAuthorizationException(
            AclOperation.DELETE,
            subject
        );
      }

      throw e;
    }
  }

  public static boolean subjectExists(
      final SchemaRegistryClient srClient,
      final String subject
  ) {
    return getLatestSchema(srClient, subject).isPresent();
  }

  public static Optional<Integer> getLatestSchemaId(
      final SchemaRegistryClient srClient,
      final String topic,
      final boolean isKey
  ) {
    final String subject = KsqlConstants.getSRSubject(topic, isKey);
    return getLatestSchema(srClient, subject).map(SchemaMetadata::getId);
  }

  public static Optional<SchemaAndId> getLatestSchemaAndId(
      final SchemaRegistryClient srClient,
      final String topic,
      final boolean isKey
  ) {
    final String subject = KsqlConstants.getSRSubject(topic, isKey);

    return getLatestSchemaId(srClient, topic, isKey)
        .map(id -> {
          try {
            return new SchemaAndId(srClient.getSchemaById(id), id);
          } catch (final Exception e) {
            throwOnAuthError(e, subject);
            throw new KsqlException(
                "Could not get schema for subject " + subject + " and id " + id, e);
          }
        });
  }

  private static void throwOnAuthError(final Exception e, final String subject) {
    if (isAuthErrorCode(e)) {
      final AclOperation deniedOperation = SchemaRegistryUtil.getDeniedOperation(e.getMessage());

      if (deniedOperation != AclOperation.UNKNOWN) {
        throw new KsqlSchemaAuthorizationException(
            deniedOperation,
            subject
        );
      }
    }
  }

  public static Optional<SchemaMetadata> getLatestSchema(
      final SchemaRegistryClient srClient,
      final String topic,
      final boolean getKeySchema
  ) {
    final String subject = KsqlConstants.getSRSubject(topic, getKeySchema);
    return getLatestSchema(srClient, subject);
  }

  public static Optional<SchemaMetadata> getLatestSchema(
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

      throwOnAuthError(e, subject);

      throw new KsqlException("Could not get latest schema for subject " + subject, e);
    }
  }

  public static AclOperation getDeniedOperation(final String errorMessage) {
    final Matcher matcher = DENIED_OPERATION_STRING_PATTERN.matcher(errorMessage);
    if (matcher.matches()) {
      return AclOperation.fromString(matcher.group(1));
    } else {
      return AclOperation.UNKNOWN;
    }
  }

  public static boolean isSubjectNotFoundErrorCode(final Throwable error) {
    return (error instanceof RestClientException
        && ((RestClientException) error).getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE);
  }

  public static boolean isAuthErrorCode(final Throwable error) {
    return (error instanceof RestClientException
        && ((((RestClientException) error).getStatus() == HttpStatus.SC_UNAUTHORIZED)
              || ((RestClientException) error).getStatus() == HttpStatus.SC_FORBIDDEN));
  }

  private static boolean isRetriableError(final Throwable error) {
    return !isSubjectNotFoundErrorCode(error) && !isAuthErrorCode(error);
  }

  private static void hardDeleteSubjectWithRetries(
      final SchemaRegistryClient schemaRegistryClient,
      final String subject) throws Exception {
    try {
      ExecutorUtil.executeWithRetries(
          () -> schemaRegistryClient.deleteSubject(subject, true),
          error -> isRetriableError(error)
      );
    } catch (final RestClientException e) {
      if (isAuthErrorCode(e)) {
        final AclOperation deniedOperation = SchemaRegistryUtil.getDeniedOperation(e.getMessage());

        if (deniedOperation != AclOperation.UNKNOWN) {
          throw new KsqlSchemaAuthorizationException(
              deniedOperation,
              subject
          );
        }
      }

      throw e;
    }
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
        .filter(SchemaRegistryUtil::isInternalSubject);
  }

  private static boolean isInternalSubject(final String subjectName) {
    for (boolean isKey : new boolean[]{true, false}) {
      final String changelog =
          KsqlConstants.getSRSubject(KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX, isKey);
      final String repartition =
          KsqlConstants.getSRSubject(KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX, isKey);
      if (subjectName.endsWith(changelog) || subjectName.endsWith(repartition)) {
        return true;
      }
    }
    return false;
  }

  private static void tryDeleteInternalSubject(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient,
      final String subjectName) {
    try {
      deleteSubjectWithRetries(schemaRegistryClient, subjectName);
      hardDeleteSubjectWithRetries(schemaRegistryClient, subjectName);
    } catch (final Exception e) {
      LOG.warn("Could not clean up the schema registry for"
          + " query: " + applicationId
          + ", subject: " + subjectName, e);
    }
  }

  public static int registerSchema(
      final SchemaRegistryClient srClient,
      final ParsedSchema parsedSchema,
      final String topic,
      final String subject,
      final boolean isKey
  ) throws KsqlSchemaAuthorizationException, KsqlException {
    try {
      if (parsedSchema instanceof ProtobufSchema) {
        final ProtobufSchema resolved = AbstractKafkaProtobufSerializer.resolveDependencies(
            srClient,
            true,
            false,
            true,
            null,
            new DefaultReferenceSubjectNameStrategy(),
            topic,
            isKey,
            (ProtobufSchema) parsedSchema
        );
        return srClient.register(subject, resolved);
      } else {
        return srClient.register(subject, parsedSchema);
      }
    } catch (IOException | RestClientException e) {
      if (SchemaRegistryUtil.isAuthErrorCode(e)) {
        final AclOperation deniedOperation = SchemaRegistryUtil.getDeniedOperation(e.getMessage());

        if (deniedOperation != AclOperation.UNKNOWN) {
          throw new KsqlSchemaAuthorizationException(
              deniedOperation,
              subject);
        }
      }

      throw new KsqlException("Could not register schema for topic: " + e.getMessage(), e);
    }
  }
}