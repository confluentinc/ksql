/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql.inference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SchemaTranslator;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class SchemaRegistryTopicSchemaSupplierTest {

  private static final String TOPIC_NAME = "some-topic";
  private static final int SCHEMA_ID = 12;
  private static final String AVRO_SCHEMA = "{use your imagination}";

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private ParsedSchema parsedSchema;
  @Mock
  private SimpleColumn column1;
  @Mock
  private SimpleColumn column2;
  @Mock
  private Format format;
  @Mock
  private FormatInfo expectedFormat;
  @Mock
  private Map<String, String> formatProperties;
  @Mock
  private SchemaTranslator schemaTranslator;

  private SchemaRegistryTopicSchemaSupplier supplier;

  @Before
  public void setUp() throws Exception {
    supplier = new SchemaRegistryTopicSchemaSupplier(srClient, f -> format);

    when(srClient.getLatestSchemaMetadata(any()))
        .thenReturn(new SchemaMetadata(SCHEMA_ID, -1, AVRO_SCHEMA));

    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenReturn(parsedSchema);

    when(parsedSchema.schemaType()).thenReturn(AvroSchema.TYPE);

    when(parsedSchema.canonicalString()).thenReturn(AVRO_SCHEMA);

    when(format.getSchemaTranslator(any())).thenReturn(schemaTranslator);
    when(format.getSchemaTranslator(any(), any())).thenReturn(schemaTranslator);
    when(schemaTranslator.toColumns(eq(parsedSchema), any(), anyBoolean()))
        .thenReturn(ImmutableList.of(column1));
    when(schemaTranslator.name()).thenReturn("AVRO");

    when(expectedFormat.getProperties()).thenReturn(formatProperties);
  }

  @Test
  public void shouldReturnErrorFromGetValueSchemaIfSchemaIsNotInExpectedFormat() {
    // Given:
    when(parsedSchema.schemaType()).thenReturn(ProtobufSchema.TYPE);

    // When:
    final SchemaResult result = supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of());

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    assertThat(result.failureReason.get().getMessage(), is(
        "Value schema is not in the expected format. "
            + "You may want to set VALUE_FORMAT to 'PROTOBUF'."
            + System.lineSeparator()
            + "topic: " + TOPIC_NAME
            + System.lineSeparator()
            + "expected format: AVRO"
            + System.lineSeparator()
            + "actual format from Schema Registry: PROTOBUF"
    ));
  }

  @Test
  public void shouldReturnErrorFromGetKeySchemaIfSchemaIsNotInExpectedFormat() {
    // Given:
    when(parsedSchema.schemaType()).thenReturn(ProtobufSchema.TYPE);

    // When:
    final SchemaResult result = supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    assertThat(result.failureReason.get().getMessage(), is(
        "Key schema is not in the expected format. "
            + "You may want to set KEY_FORMAT to 'PROTOBUF'."
            + System.lineSeparator()
            + "topic: " + TOPIC_NAME
            + System.lineSeparator()
            + "expected format: AVRO"
            + System.lineSeparator()
            + "actual format from Schema Registry: PROTOBUF"
    ));
  }

  @Test
  public void shouldReturnErrorFromGetValueSchemaIfNotFound() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(notFoundException());

    // When:
    final SchemaResult result = supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of());

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    verifyFailureMessageForValue(result, Optional.empty());
  }

  @Test
  public void shouldReturnErrorFromGetKeySchemaIfNotFound() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(notFoundException());

    // When:
    final SchemaResult result = supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    verifyFailureMessageForKey(result, Optional.empty());
  }

  @Test
  public void shouldReturnErrorFromGetValueWithIdSchemaIfNotFound() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(notFoundException());

    // When:
    final SchemaResult result = supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.of(42), expectedFormat, SerdeFeatures.of());

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    verifyFailureMessageForValue(result, Optional.of(42));
  }

  @Test
  public void shouldReturnErrorFromGetKeyWithIdSchemaIfNotFound() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(notFoundException());

    // When:
    final SchemaResult result = supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.of(42), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    verifyFailureMessageForKey(result, Optional.of(42));
  }

  @Test
  public void shouldReturnErrorFromGetValueIfUnauthorized() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(unauthorizedException());

    // When:
    final SchemaResult result = supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.of(42), expectedFormat, SerdeFeatures.of());

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    verifyFailureMessageForValue(result, Optional.of(42));
  }

  @Test
  public void shouldReturnErrorFromGetKeyIfUnauthorized() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(unauthorizedException());

    // When:
    final SchemaResult result = supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.of(42), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    verifyFailureMessageForKey(result, Optional.of(42));
  }

  @Test
  public void shouldReturnErrorFromGetValueIfForbidden() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(forbiddenException());

    // When:
    final SchemaResult result = supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.of(42), expectedFormat, SerdeFeatures.of());

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    verifyFailureMessageForValue(result, Optional.of(42));
  }

  @Test
  public void shouldReturnErrorFromGetKeyIfForbidden() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(forbiddenException());

    // When:
    final SchemaResult result = supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.of(42), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    verifyFailureMessageForKey(result, Optional.of(42));
  }

  private void verifyFailureMessageForKey(final SchemaResult result,
      final Optional<Integer> id) {
    verifyFailureMessage(result, true, id);
  }

  private void verifyFailureMessageForValue(final SchemaResult result, final Optional<Integer> id) {
    verifyFailureMessage(result, false, id);
  }

  private void verifyFailureMessage(final SchemaResult result,
      final boolean isKey, Optional<Integer> id) {
    final String keyOrValue = isKey ? "keys" : "values";
    final String keyOrValueSuffix = isKey ? "key" : "value";
    final String schemaIdMsg = id.map(integer -> "Schema Id: " + integer + System.lineSeparator())
        .orElse("");
    assertThat(result.failureReason.get().getMessage(), is(
        "Schema for message " + keyOrValue + " on topic '" + TOPIC_NAME + "' does not exist in the Schema Registry." + System.lineSeparator()
            + "Subject: " + TOPIC_NAME + "-" + keyOrValueSuffix
            + System.lineSeparator()
            + schemaIdMsg
            + "Possible causes include:" + System.lineSeparator()
            + "- The topic itself does not exist" + System.lineSeparator()
            + "\t-> Use SHOW TOPICS; to check" + System.lineSeparator()
            + "- Messages on the topic are not serialized using a format Schema Registry supports" + System.lineSeparator()
            + "\t-> Use PRINT '" + TOPIC_NAME + "' FROM BEGINNING; to verify" + System.lineSeparator()
            + "- Messages on the topic have not been serialized using a Confluent Schema Registry supported serializer" + System.lineSeparator()
            + "\t-> See https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html" + System.lineSeparator()
            + "- The schema is registered on a different instance of the Schema Registry" + System.lineSeparator()
            + "\t-> Use the REST API to list available subjects\thttps://docs.confluent.io/current/schema-registry/docs/api.html#get--subjects" + System.lineSeparator()
            + "- You do not have permissions to access the Schema Registry." + System.lineSeparator()
            + "\t-> See https://docs.confluent.io/current/schema-registry/docs/security.html"));
  }

  @Test
  public void shouldThrowFromGetValueSchemaOnOtherRestExceptions() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(new RestClientException("failure", 1, 1));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> supplier.getValueSchema(Optional.of(TOPIC_NAME),
            Optional.empty(), expectedFormat, SerdeFeatures.of())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Schema registry fetch for topic "
        + "value request failed for topic: " + TOPIC_NAME));
  }

  @Test
  public void shouldThrowFromGetKeySchemaOnOtherRestExceptions() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(new RestClientException("failure", 1, 1));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> supplier.getKeySchema(Optional.of(TOPIC_NAME),
            Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Schema registry fetch for topic "
        + "key request failed for topic: " + TOPIC_NAME));
  }

  @Test
  public void shouldThrowFromGetValueWithIdSchemaOnOtherRestExceptions() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(new RestClientException("failure", 1, 1));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> supplier.getValueSchema(Optional.of(TOPIC_NAME),
            Optional.of(42), expectedFormat, SerdeFeatures.of())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Schema registry fetch for topic "
        + "value request failed for topic: " + TOPIC_NAME));
  }

  @Test
  public void shouldThrowFromGetKeyWithIdSchemaOnOtherRestExceptions() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(new RestClientException("failure", 1, 1));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> supplier.getKeySchema(Optional.of(TOPIC_NAME),
            Optional.of(42), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Schema registry fetch for topic "
        + "key request failed for topic: " + TOPIC_NAME));
  }

  @Test
  public void shouldThrowFromGetValueSchemaOnOtherException() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(new IOException("boom"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> supplier.getValueSchema(Optional.of(TOPIC_NAME),
            Optional.empty(), expectedFormat, SerdeFeatures.of())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Schema registry fetch for topic "
        + "value request failed for topic: " + TOPIC_NAME));
  }

  @Test
  public void shouldThrowFromGetKeySchemaOnOtherException() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(new IOException("boom"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> supplier.getKeySchema(Optional.of(TOPIC_NAME),
            Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Schema registry fetch for topic "
        + "key request failed for topic: " + TOPIC_NAME));
  }

  @Test
  public void shouldThrowFromGetValueWithIdSchemaOnOtherException() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(new IOException("boom"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> supplier.getValueSchema(Optional.of(TOPIC_NAME),
            Optional.of(42), expectedFormat, SerdeFeatures.of())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Schema registry fetch for topic "
        + "value request failed for topic: " + TOPIC_NAME));
  }

  @Test
  public void shouldThrowFromGetKeyWithIdSchemaOnOtherException() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(new IOException("boom"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> supplier.getKeySchema(Optional.of(TOPIC_NAME),
            Optional.of(42), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Schema registry fetch for topic "
        + "key request failed for topic: " + TOPIC_NAME));
  }

  @Test
  public void shouldReturnErrorFromGetValueSchemaIfCanNotConvertToConnectSchema() {
    // Given:
    when(schemaTranslator.toColumns(any(), any(), anyBoolean()))
        .thenThrow(new RuntimeException("it went boom"));

    // When:
    final SchemaResult result = supplier
        .getValueSchema(Optional.of(TOPIC_NAME), Optional.empty(), expectedFormat, SerdeFeatures.of());

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "Unable to verify if the value schema for topic: some-topic is compatible with ksqlDB."));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "it went boom"));
    assertThat(result.failureReason.get().getMessage(), containsString(AVRO_SCHEMA));
  }

  @Test
  public void shouldReturnErrorFromGetKeySchemaIfCanNotConvertToConnectSchema() {
    // Given:
    when(schemaTranslator.toColumns(any(), any(), anyBoolean()))
        .thenThrow(new RuntimeException("it went boom"));

    // When:
    final SchemaResult result = supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "Unable to verify if the key schema for topic: some-topic is compatible with ksqlDB."));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "it went boom"));
    assertThat(result.failureReason.get().getMessage(), containsString(AVRO_SCHEMA));
  }

  @Test
  public void shouldReturnErrorFromGetKeySchemaOnMultipleColumns() {
    // Given:
    when(schemaTranslator.toColumns(parsedSchema, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES), true))
        .thenReturn(ImmutableList.of(column1, column2));

    // When:
    final SchemaResult result = supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "The key schema for topic: some-topic contains multiple columns, "
            + "which is not supported by ksqlDB at this time."));
    assertThat(result.failureReason.get().getMessage(), containsString(AVRO_SCHEMA));
  }

  @Test
  public void shouldRequestCorrectSchemaOnGetValueSchema() throws Exception {
    // When:
    supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of());

    // Then:
    verify(srClient).getLatestSchemaMetadata(TOPIC_NAME + "-value");
  }

  @Test
  public void shouldRequestCorrectSchemaOnGetKeySchema() throws Exception {
    // When:
    supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    verify(srClient).getLatestSchemaMetadata(TOPIC_NAME + "-key");
  }

  @Test
  public void shouldRequestCorrectSchemaOnGetValueSchemaWithId() throws Exception {
    // When:
    supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.of(42), expectedFormat, SerdeFeatures.of());

    // Then:
    verify(srClient).getSchemaBySubjectAndId(TOPIC_NAME + "-value", 42);
  }

  @Test
  public void shouldRequestCorrectSchemaOnGetKeySchemaWithId() throws Exception {
    // When:
    supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.of(42), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    verify(srClient).getSchemaBySubjectAndId(TOPIC_NAME + "-key", 42);
  }

  @Test
  public void shouldPassRightSchemaToFormat() {
    // When:
    supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of());

    // Then:
    verify(format).getSchemaTranslator(formatProperties);
    verify(schemaTranslator).toColumns(parsedSchema, SerdeFeatures.of(), false);
  }

  @Test
  public void shouldPassCorrectValueWrapping() {
    // When:
    supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    verify(schemaTranslator).toColumns(parsedSchema, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES), false);
  }

  @Test
  public void shouldPassCorrectKeyWrapping() {
    // When:
    supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    verify(schemaTranslator).toColumns(parsedSchema, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES), true);
  }

  @Test
  public void shouldReturnSchemaFromGetValueSchemaIfFound() {
    // When:
    final SchemaResult result = supplier.getValueSchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of());

    // Then:
    assertThat(result.schemaAndId, is(not(Optional.empty())));
    assertThat(result.schemaAndId.get().id, is(SCHEMA_ID));
    assertThat(result.schemaAndId.get().columns, is(ImmutableList.of(column1)));
  }

  @Test
  public void shouldReturnSchemaFromGetKeySchemaIfFound() {
    // When:
    final SchemaResult result = supplier.getKeySchema(Optional.of(TOPIC_NAME),
        Optional.empty(), expectedFormat, SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // Then:
    assertThat(result.schemaAndId, is(not(Optional.empty())));
    assertThat(result.schemaAndId.get().id, is(SCHEMA_ID));
    assertThat(result.schemaAndId.get().columns, is(ImmutableList.of(column1)));
  }

  private static Throwable notFoundException() {
    return new RestClientException("no found", HttpStatus.SC_NOT_FOUND, -1);
  }

  private static Throwable unauthorizedException() {
    return new RestClientException("unauthorized", HttpStatus.SC_UNAUTHORIZED, -1);
  }

  private static Throwable forbiddenException() {
    return new RestClientException("forbidden", HttpStatus.SC_FORBIDDEN, -1);
  }
}