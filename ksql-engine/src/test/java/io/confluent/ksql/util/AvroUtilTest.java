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


package io.confluent.ksql.util;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import java.io.IOException;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroUtilTest {

  private static final String STREAM_NAME = "some-stream";

  private static final String AVRO_SCHEMA_STRING = "{"
      + "\"namespace\": \"some.namespace\","
      + " \"name\": \"orders\","
      + " \"type\": \"record\","
      + " \"fields\": ["
      + "     {\"name\": \"ordertime\", \"type\": \"long\"},"
      + "     {\"name\": \"orderid\",  \"type\": \"long\"},"
      + "     {\"name\": \"itemid\", \"type\": \"string\"},"
      + "     {\"name\": \"orderunits\", \"type\": \"double\"},"
      + "     {\"name\": \"arraycol\", \"type\": {\"type\": \"array\", \"items\": \"double\"}},"
      + "     {\"name\": \"mapcol\", \"type\": {\"type\": \"map\", \"values\": \"double\"}}"
      + " ]"
      + "}";

  private static final String SINGLE_FIELD_AVRO_SCHEMA_STRING = "{"
      + "\"namespace\": \"some.namespace\","
      + " \"name\": \"orders\","
      + " \"type\": \"record\","
      + " \"fields\": ["
      + "     {\"name\": \"ordertime\", \"type\": \"long\"}"
      + " ]"
      + "}";

  private static final LogicalSchema MUTLI_FIELD_SCHEMA =
      toKsqlSchema(AVRO_SCHEMA_STRING);

  private static final LogicalSchema SINGLE_FIELD_SCHEMA =
      toKsqlSchema(SINGLE_FIELD_AVRO_SCHEMA_STRING);

  private static final KsqlTopic RESULT_TOPIC = new KsqlTopic(
      "actual-name",
      KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
      ValueFormat.of(FormatInfo.of(Format.AVRO)),
      false);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private PersistentQueryMetadata persistentQuery;

  @Before
  public void setUp() {
    when(persistentQuery.getPhysicalSchema())
        .thenReturn(PhysicalSchema.from(MUTLI_FIELD_SCHEMA, SerdeOption.none()));

    when(persistentQuery.getResultTopic()).thenReturn(RESULT_TOPIC);
    when(persistentQuery.getResultTopicFormat())
        .thenReturn(RESULT_TOPIC.getValueFormat().getFormat());
    when(persistentQuery.getSinkName()).thenReturn(STREAM_NAME);
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSubject() throws Exception {
    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    verify(srClient).testCompatibility(eq(RESULT_TOPIC.getKafkaTopicName() + "-value"), any());
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSchema() throws Exception {
    // Given:
    final PhysicalSchema schema = PhysicalSchema.from(MUTLI_FIELD_SCHEMA, SerdeOption.none());

    final org.apache.avro.Schema expectedAvroSchema = SchemaUtil
        .buildAvroSchema(schema.valueSchema(), STREAM_NAME);

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldValidateWrappedSingleFieldSchemaEvolution() throws Exception {
    // Given:
    final PhysicalSchema schema = PhysicalSchema
        .from(SINGLE_FIELD_SCHEMA, SerdeOption.none());

    when(persistentQuery.getPhysicalSchema()).thenReturn(schema);

    final org.apache.avro.Schema expectedAvroSchema = SchemaUtil
        .buildAvroSchema(schema.valueSchema(), STREAM_NAME);

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldValidateUnwrappedSingleFieldSchemaEvolution() throws Exception {
    // Given:
    final PhysicalSchema schema = PhysicalSchema
        .from(SINGLE_FIELD_SCHEMA, SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES));

    when(persistentQuery.getPhysicalSchema()).thenReturn(schema);

    final org.apache.avro.Schema expectedAvroSchema = SchemaUtil
        .buildAvroSchema(schema.valueSchema(), STREAM_NAME);

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldReturnValidEvolution() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any())).thenReturn(true);

    // When:
    final boolean result = AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    assertThat(result, is(true));
  }

  @Test
  public void shouldReturnInvalidEvolution() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any())).thenReturn(false);

    // When:
    final boolean result = AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    assertThat(result, is(false));
  }

  @Test
  public void shouldReturnValidEvolutionIfSubjectNotRegistered() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any()))
        .thenThrow(new RestClientException("Unknown subject", 404, 40401));

    // When:
    final boolean result = AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    assertThat(result, is(true));
  }

  @Test
  public void shouldThrowOnSrAuthorizationErrors() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any()))
        .thenThrow(new RestClientException("Unknown subject", 403, 40401));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not connect to Schema Registry service");
    expectedException.expectMessage(containsString(String.format(
        "Not authorized to access Schema Registry subject: [%s]",
        persistentQuery.getResultTopic().getKafkaTopicName()
            + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
    )));

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);
  }

  @Test
  public void shouldThrowOnAnyOtherEvolutionSrException() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any()))
        .thenThrow(new RestClientException("Unknown subject", 500, 40401));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not connect to Schema Registry service");

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);
  }

  @Test
  public void shouldThrowOnAnyOtherEvolutionIOException() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any()))
        .thenThrow(new IOException("something"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not check Schema compatibility");

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);
  }

  private static LogicalSchema toKsqlSchema(final String avroSchemaString) {
    final org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(avroSchemaString);
    final AvroData avroData = new AvroData(new AvroDataConfig(Collections.emptyMap()));
    return LogicalSchema.of(new ConnectSchemaTranslator()
        .toKsqlSchema(avroData.toConnectSchema(avroSchema)));
  }
}
