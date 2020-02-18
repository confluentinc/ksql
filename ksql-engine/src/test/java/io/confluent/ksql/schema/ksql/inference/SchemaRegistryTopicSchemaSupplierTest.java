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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import org.apache.http.HttpStatus;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaRegistryTopicSchemaSupplierTest {

  private static final String TOPIC_NAME = "some-topic";
  private static final int SCHEMA_ID = 12;
  private static final String AVRO_SCHEMA = "{use your imagination}";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private Function<Schema, Schema> toKsqlTranslator;
  @Mock
  private ParsedSchema parsedSchema;
  @Mock
  private Schema connectSchema;
  @Mock
  private Schema ksqlSchema;
  @Mock
  private Format format;

  private SchemaRegistryTopicSchemaSupplier supplier;

  @Before
  public void setUp() throws Exception {
    supplier = new SchemaRegistryTopicSchemaSupplier(
        srClient, toKsqlTranslator, f -> format);

    when(srClient.getLatestSchemaMetadata(any()))
        .thenReturn(new SchemaMetadata(SCHEMA_ID, -1, AVRO_SCHEMA));

    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenReturn(parsedSchema);

    when(parsedSchema.schemaType()).thenReturn(AvroSchema.TYPE);

    when(parsedSchema.canonicalString()).thenReturn(AVRO_SCHEMA);

    when(format.toConnectSchema(parsedSchema)).thenReturn(connectSchema);

    when(toKsqlTranslator.apply(any()))
        .thenReturn(ksqlSchema);
  }

  @Test
  public void shouldReturnErrorFromGetValueSchemaIfNotFound() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(notFoundException());

    // When:
    final SchemaResult result = supplier.getValueSchema(TOPIC_NAME, Optional.empty());

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "Schema for message values on topic " + TOPIC_NAME
            + " does not exist in the Schema Registry."));
  }

  @Test
  public void shouldReturnErrorFromGetValueWithIdSchemaIfNotFound() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(notFoundException());

    // When:
    final SchemaResult result = supplier.getValueSchema(TOPIC_NAME, Optional.of(42));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "Schema for message values on topic " + TOPIC_NAME
            + " does not exist in the Schema Registry."));
  }

  @Test
  public void shouldReturnErrorFromGetValueIfUnauthorized() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(unauthorizedException());

    // When:
    final SchemaResult result = supplier.getValueSchema(TOPIC_NAME, Optional.of(42));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "Schema for message values on topic " + TOPIC_NAME
            + " does not exist in the Schema Registry."));
  }

  @Test
  public void shouldReturnErrorFromGetValueIfForbidden() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(forbiddenException());

    // When:
    final SchemaResult result = supplier.getValueSchema(TOPIC_NAME, Optional.of(42));

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason, is(not(Optional.empty())));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "Schema for message values on topic " + TOPIC_NAME
            + " does not exist in the Schema Registry."));
  }

  @Test
  public void shouldThrowFromGetValueSchemaOnOtherRestExceptions() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(new RestClientException("failure", 1, 1));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Schema registry fetch for topic "
        + TOPIC_NAME + " request failed.");

    // When:
    supplier.getValueSchema(TOPIC_NAME, Optional.empty());
  }

  @Test
  public void shouldThrowFromGetValueWithIdSchemaOnOtherRestExceptions() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(new RestClientException("failure", 1, 1));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Schema registry fetch for topic "
        + TOPIC_NAME + " request failed.");

    // When:
    supplier.getValueSchema(TOPIC_NAME, Optional.of(42));
  }

  @Test
  public void shouldThrowFromGetValueSchemaOnOtherException() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(new IOException("boom"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Schema registry fetch for topic "
        + TOPIC_NAME + " request failed.");

    // When:
    supplier.getValueSchema(TOPIC_NAME, Optional.empty());
  }

  @Test
  public void shouldThrowFromGetValueWithIdSchemaOnOtherException() throws Exception {
    // Given:
    when(srClient.getSchemaBySubjectAndId(any(), anyInt()))
        .thenThrow(new IOException("boom"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Schema registry fetch for topic "
        + TOPIC_NAME + " request failed.");

    // When:
    supplier.getValueSchema(TOPIC_NAME, Optional.of(42));
  }

  @Test
  public void shouldReturnErrorFromGetValueSchemaIfCanNotConvertToConnectSchema() {
    // Given:
    when(format.toConnectSchema(any()))
        .thenThrow(new RuntimeException("it went boom"));

    // When:
    final SchemaResult result = supplier.getValueSchema(TOPIC_NAME, Optional.empty());

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "Unable to verify if the schema for topic some-topic is compatible with KSQL."));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "it went boom"));
    assertThat(result.failureReason.get().getMessage(), containsString(AVRO_SCHEMA));
  }

  @Test
  public void shouldReturnErrorFromGetValueSchemaIfCanNotConvertToKsqlSchema() {
    // Given:
    when(toKsqlTranslator.apply(any()))
        .thenThrow(new RuntimeException("big badda boom"));

    // When:
    final SchemaResult result = supplier.getValueSchema(TOPIC_NAME, Optional.empty());

    // Then:
    assertThat(result.schemaAndId, is(Optional.empty()));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "Unable to verify if the schema for topic some-topic is compatible with KSQL."));
    assertThat(result.failureReason.get().getMessage(), containsString(
        "big badda boom"));
    assertThat(result.failureReason.get().getMessage(), containsString(AVRO_SCHEMA));
  }

  @Test
  public void shouldRequestCorrectSchemaOnGetValueSchema() throws Exception {
    // When:
    supplier.getValueSchema(TOPIC_NAME, Optional.empty());

    // Then:
    verify(srClient).getLatestSchemaMetadata(TOPIC_NAME + "-value");
  }

  @Test
  public void shouldRequestCorrectSchemaOnGetValueSchemaWithId() throws Exception {
    // When:
    supplier.getValueSchema(TOPIC_NAME, Optional.of(42));

    // Then:
    verify(srClient).getSchemaBySubjectAndId(TOPIC_NAME + "-value", 42);
  }

  @Test
  public void shouldPassWriteSchemaToFormat() {
    // When:
    supplier.getValueSchema(TOPIC_NAME, Optional.empty());

    // Then:
    verify(format).toConnectSchema(parsedSchema);
  }

  @Test
  public void shouldPassWriteSchemaToKsqlTranslator() {
    // When:
    supplier.getValueSchema(TOPIC_NAME, Optional.empty());

    // Then:
    verify(toKsqlTranslator).apply(connectSchema);
  }

  @Test
  public void shouldReturnSchemaFromGetValueSchemaIfFound() {
    // When:
    final SchemaResult result = supplier.getValueSchema(TOPIC_NAME, Optional.empty());

    // Then:
    assertThat(result.schemaAndId, is(not(Optional.empty())));
    assertThat(result.schemaAndId.get().id, is(SCHEMA_ID));
    assertThat(result.schemaAndId.get().schema, is(ksqlSchema));
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