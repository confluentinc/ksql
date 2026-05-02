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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.util.Optional;

import org.apache.kafka.common.acl.AclOperation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaRegistryUtilTest {

  private static final String APP_ID = "_my_app_id";
  private static final AvroSchema AVRO_SCHEMA = new AvroSchema(
      "{\"type\":\"record\",\"name\":\"KsqlDataSourceSchema\","
          + "\"namespace\":\"io.confluent.ksql.avro_schemas\",\"fields\":"
          + "[{\"name\":\"F1\",\"type\":[\"null\",\"string\"],\"default\":null}],"
          + "\"connect.name\":\"io.confluent.ksql.avro_schemas.KsqlDataSourceSchema\"}");
  private static final ProtobufSchema PROTOBUF_SCHEMA = new ProtobufSchema(
      "syntax = \"proto3\"; import \"google/protobuf/timestamp.proto\";"
          + "message ConnectDefault1 {google.protobuf.Timestamp F1 = 1;}");
  @Mock
  private SchemaRegistryClient schemaRegistryClient;
  @Mock
  private SchemaMetadata schemaMetadata;

  @Test
  public void shouldReturnParsedSchemaFromSubjectValue() throws Exception {
    // Given:
    when(schemaMetadata.getId()).thenReturn(123);
    when(schemaRegistryClient.getLatestSchemaMetadata("bar-value"))
        .thenReturn(schemaMetadata);
    when(schemaRegistryClient.getSchemaById(123))
        .thenReturn(AVRO_SCHEMA);

    // When:
    final Optional<SchemaAndId> schemaAndId =
        SchemaRegistryUtil.getLatestSchemaAndId(schemaRegistryClient, "bar", false);

    // Then:
    assertThat(schemaAndId.get().getSchema(), equalTo(AVRO_SCHEMA));
  }

  @Test
  public void shouldReturnSchemaIdFromSubjectKey() throws Exception {
    // Given:
    when(schemaMetadata.getId()).thenReturn(123);
    when(schemaRegistryClient.getLatestSchemaMetadata("bar-key"))
        .thenReturn(schemaMetadata);

    // When:
    final Optional<Integer> schemaId =
        SchemaRegistryUtil.getLatestSchemaId(schemaRegistryClient, "bar", true);

    // Then:
    assertThat(schemaId.get(), equalTo(123));
  }

  @Test
  public void shouldReturnSchemaIdFromSubjectValue() throws Exception {
    // Given:
    when(schemaMetadata.getId()).thenReturn(123);
    when(schemaRegistryClient.getLatestSchemaMetadata("bar-value"))
        .thenReturn(schemaMetadata);

    // When:
    final Optional<Integer> schemaId =
        SchemaRegistryUtil.getLatestSchemaId(schemaRegistryClient, "bar", false);

    // Then:
    assertThat(schemaId.get(), equalTo(123));
  }


  @Test
  public void shouldReturnParsedSchemaFromSubjectKey() throws Exception {
    // Given:
    when(schemaMetadata.getId()).thenReturn(123);
    when(schemaRegistryClient.getLatestSchemaMetadata("bar-key"))
        .thenReturn(schemaMetadata);
    when(schemaRegistryClient.getSchemaById(123))
        .thenReturn(AVRO_SCHEMA);

    // When:
    final Optional<SchemaAndId> schemaAndId =
        SchemaRegistryUtil.getLatestSchemaAndId(schemaRegistryClient, "bar", true);

    // Then:
    assertThat(schemaAndId.get().getSchema(), equalTo(AVRO_SCHEMA));
  }

  // The fixtures below all follow the canonical internal-subject shape
  // "<applicationId>-<rawTopic>-(changelog|repartition)-(key|value)".
  // Rationale (see also SchemaRegistryUtil#getInternalSubjectNames):
  //  * Kafka Streams' `InternalTopologyBuilder#decorateTopic` unconditionally
  //    builds internal topic names as "<applicationId>-<rawTopic>" — the "-"
  //    is hardcoded, so every internal topic (and therefore every internal SR
  //    subject) begins with "<applicationId>-".
  //  * `KsqlConstants#getSRSubject` then appends "-key" or "-value", so a full
  //    internal subject is "<applicationId>-<...>-(changelog|repartition)-(key|value)".
  //  * The companion Kafka-topic cleanup at
  //    `KafkaTopicClientImpl#isInternalTopic` already uses
  //    `topicName.startsWith(applicationId + "-")` — these tests use the same
  //    shape so the SR-side cleanup is exercised against realistic input.
  // The pre-fix fixtures used "APP_ID + \"SOME-changelog-key\"" (no separator
  // after the applicationId), which does not match anything Kafka Streams
  // would actually emit and is what masked KSQL-14907.
  @Test
  public void shouldDeleteChangeLogTopicSchema() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "-SOME-changelog-key",
        APP_ID + "-SOME-changelog-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient);

    // Then not exception:
    verify(schemaRegistryClient).deleteSubject(APP_ID + "-SOME-changelog-key");
    verify(schemaRegistryClient).deleteSubject(APP_ID + "-SOME-changelog-value");
  }

  @Test
  public void shouldDeleteRepartitionTopicSchema() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "-SOME-repartition-key",
        APP_ID + "-SOME-repartition-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient);

    // Then not exception:
    verify(schemaRegistryClient).deleteSubject(APP_ID + "-SOME-repartition-key");
    verify(schemaRegistryClient).deleteSubject(APP_ID + "-SOME-repartition-value");
  }

  @Test
  public void shouldHardDeleteIfFlagSet() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "-SOME-repartition-key",
        APP_ID + "-SOME-repartition-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient);

    // Then not exception:
    final InOrder inOrder = inOrder(schemaRegistryClient);
    inOrder.verify(schemaRegistryClient).deleteSubject(APP_ID + "-SOME-repartition-key");
    inOrder.verify(schemaRegistryClient).deleteSubject(APP_ID + "-SOME-repartition-key", true);
    inOrder.verify(schemaRegistryClient).deleteSubject(APP_ID + "-SOME-repartition-value");
    inOrder.verify(schemaRegistryClient).deleteSubject(APP_ID + "-SOME-repartition-value", true);
  }

  @Test
  public void shouldNotDeleteOtherSchemasForThisApplicationId() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "-SOME-other-key",
        APP_ID + "-SOME-other-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient);

    // Then not exception:
    verify(schemaRegistryClient, never()).deleteSubject(any());
  }

  @Test
  public void shouldNotDeleteSchemasOfLargerNumberedQueryWithSamePrefix() throws Exception {
    // Regression for KSQL-14907.
    //
    // Production scenario this models:
    //   * A user issued >=10 CREATE OR REPLACE TABLE FOO AS SELECT ..., so KSQL had
    //     advanced its query name from CTAS_FOO_1 to CTAS_FOO_17.
    //   * Stale local state from CTAS_FOO_1 lingered on a pod's PV (KSQL logs the
    //     "race condition when the query was dropped before" WARN in this case).
    //   * On a subsequent pod restart, QueryCleanupService scheduled cleanup for
    //     the orphaned single-digit applicationId.
    //
    // Pre-fix bug: `getInternalSubjectNames` used `startsWith(applicationId)` with
    // no terminator, so cleanup for "..._CTAS_FOO_1" matched the four active
    // subjects of "..._CTAS_FOO_17-..." and proceeded to soft- then hard-delete
    // them — wiping the running materialized table's schemas.
    //
    // Post-fix: `getInternalSubjectNames` uses `startsWith(applicationId + "-")`.
    // Since "..._CTAS_FOO_17-..." does not start with "..._CTAS_FOO_1-" (the
    // character after "_FOO_1" is "7", not "-"), nothing is matched and nothing
    // is deleted. This is the only behavioural change vs. the old filter.
    //
    // Given:
    final String singleDigitAppId = "_confluent-ksql-default_query_CTAS_FOO_1";
    final String activeAppId = "_confluent-ksql-default_query_CTAS_FOO_17";
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        // Active (running) query's internal subjects — must NOT be deleted.
        // Shape "<activeAppId>-<storeName>-(changelog|repartition)-(key|value)"
        // matches what Kafka Streams' `InternalTopologyBuilder#decorateTopic`
        // actually emits (the "-" between applicationId and storeName is
        // hardcoded there).
        activeAppId + "-Aggregate-Aggregate-Materialize-changelog-key",
        activeAppId + "-Aggregate-Aggregate-Materialize-changelog-value",
        activeAppId + "-Join-repartition-key",
        activeAppId + "-Join-repartition-value"
    ));

    // When: cleanup for the older single-digit query.
    SchemaRegistryUtil.cleanupInternalTopicSchemas(singleDigitAppId, schemaRegistryClient);

    // Then: nothing deleted — none of the active query's subjects share the
    // "<singleDigitAppId>-" prefix (because "<singleDigitAppId>" is followed by
    // "7", not "-", in the active query's subject names).
    verify(schemaRegistryClient, never()).deleteSubject(any());
    verify(schemaRegistryClient, never()).deleteSubject(any(), any(Boolean.class));
  }

  @Test
  public void shouldNotDeleteOtherSchemas() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        "SOME-other-key",
        "SOME-other-value"
    ));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient);

    // Then not exception:
    verify(schemaRegistryClient, never()).deleteSubject(any());
  }

  @Test
  public void shouldNotThrowIfAllSubjectsThrows() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenThrow(new RuntimeException("Boom!"));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient);

    // Then not exception:
    verify(schemaRegistryClient).getAllSubjects();
  }

  @Test
  public void shouldNotThrowIfDeleteSubjectThrows() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "-SOME-changelog-key",
        APP_ID + "-SOME-changelog-value"
    ));

    when(schemaRegistryClient.deleteSubject(any())).thenThrow(new RuntimeException("Boom!"));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient);

    // Then not exception:
    verify(schemaRegistryClient, times(5)).deleteSubject(APP_ID + "-SOME-changelog-key");
    verify(schemaRegistryClient, times(5)).deleteSubject(APP_ID + "-SOME-changelog-value");
  }

  @Test
  public void shouldNotRetryIf40401() throws Exception {
    // Given:
    when(schemaRegistryClient.getAllSubjects()).thenReturn(ImmutableList.of(
        APP_ID + "-SOME-changelog-key",
        APP_ID + "-SOME-changelog-value"
    ));

    when(schemaRegistryClient.deleteSubject(any())).thenThrow(new RestClientException("foo", 404, 40401));

    // When:
    SchemaRegistryUtil.cleanupInternalTopicSchemas(APP_ID, schemaRegistryClient);

    // Then not exception (only tried once):
    verify(schemaRegistryClient, times(1)).deleteSubject(APP_ID + "-SOME-changelog-key");
    verify(schemaRegistryClient, times(1)).deleteSubject(APP_ID + "-SOME-changelog-value");
  }

  @Test
  public void shouldReturnTrueOnIsSubjectExists() throws Exception {
    // Given:
    when(schemaRegistryClient.getLatestSchemaMetadata("foo-value")).thenReturn(schemaMetadata);

    // When:
    final boolean subjectExists = SchemaRegistryUtil.subjectExists(schemaRegistryClient, "foo-value");

    // Then:
    assertTrue("Expected subject to exist", subjectExists);
  }

  @Test
  public void shouldReturnFalseOnSubjectMissing() throws Exception {
    // Given:
    when(schemaRegistryClient.getLatestSchemaMetadata("bar-value")).thenThrow(
        new RestClientException("foo", 404, SchemaRegistryUtil.SUBJECT_NOT_FOUND_ERROR_CODE)
    );

    // When:
    final boolean subjectExists = SchemaRegistryUtil.subjectExists(schemaRegistryClient, "bar-value");

    // Then:
    assertFalse("Expected subject to not exist", subjectExists);
  }

  @Test
  public void shouldThrowAuthorizationExceptionOnUnauthorizedSubjectAccess() throws Exception {
    // Given:
    when(schemaRegistryClient.getLatestSchemaMetadata("bar-value")).thenThrow(
        new RestClientException(
            "User is denied operation Write on Subject: bar-value; error code: 40301", 403, 40301)
    );

    // When:
    final Exception e = assertThrows(KsqlSchemaAuthorizationException.class,
        () -> SchemaRegistryUtil.subjectExists(schemaRegistryClient, "bar-value"));

    // Then:
    assertThat(e.getMessage(), equalTo(
        "Authorization denied to Write on Schema Registry subject: [bar-value]"));
  }

  @Test
  public void shouldReturnKnownDeniedOperationFromValidAuthorizationMessage() {
    // When:
    final AclOperation operation = SchemaRegistryUtil.getDeniedOperation(
        "User is denied operation Write on Subject: t2-value; error code: 40301");

    // Then:
    assertThat(operation, is(AclOperation.WRITE));
  }

  @Test
  public void shouldReturnUnknownDeniedOperationFromNoValidAuthorizationMessage() {
    // When:
    final AclOperation operation = SchemaRegistryUtil.getDeniedOperation(
        "INVALID is denied operation Write on Subject: t2-value; error code: 40301");

    // Then:
    assertThat(operation, is(AclOperation.UNKNOWN));
  }

  @Test
  public void shouldRegisterAvroSchema() throws Exception {
    // Given:
    when(schemaRegistryClient.register(any(), any(AvroSchema.class))).thenReturn(1);

    // When:
    SchemaRegistryUtil.registerSchema(schemaRegistryClient, AVRO_SCHEMA, "topic", "subject", false);

    // Then:
    verify(schemaRegistryClient, times(1)).register("subject", AVRO_SCHEMA);
  }

  @Test
  public void shouldRegisterProtobufSchema() throws Exception {
    // Given:
    when(schemaRegistryClient.register(any(), any(ProtobufSchema.class))).thenReturn(1);

    // When:
    SchemaRegistryUtil.registerSchema(schemaRegistryClient, PROTOBUF_SCHEMA, "topic", "subject", false);

    // Then:
    verify(schemaRegistryClient, times(1)).register("subject", PROTOBUF_SCHEMA);
  }

  @Test
  public void shouldThrowKsqlSchemaAuthorizationException() throws Exception {
    // Given:
    when(schemaRegistryClient.register(anyString(), any(ParsedSchema.class)))
        .thenThrow(new RestClientException("User is denied operation Write on Subject", 403, 40301));

    // When:
    final Exception e = assertThrows(
        KsqlSchemaAuthorizationException.class,
        () -> SchemaRegistryUtil.registerSchema(schemaRegistryClient, AVRO_SCHEMA, "topic", "subject", false)
    );

    // Then:
    assertThat(e.getMessage(), equalTo(
        "Authorization denied to Write on Schema Registry subject: [subject]"));
  }

  @Test
  public void shouldThrowKsqlException() throws Exception {
    // Given:
    when(schemaRegistryClient.register(anyString(), any(ParsedSchema.class)))
        .thenThrow(new IOException("FUBAR"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> SchemaRegistryUtil.registerSchema(schemaRegistryClient, AVRO_SCHEMA, "topic", "subject", false)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Could not register schema for topic: "));
  }
}