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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.ConnectToSqlTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.avro.AvroSchemas;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import java.io.IOException;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroUtilTest {

  private static final SourceName STREAM_NAME = SourceName.of("some_stream");

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

  private static final String STATEMENT_TEXT = "STATEMENT";

  private static final LogicalSchema MUTLI_FIELD_SCHEMA =
      toKsqlSchema(AVRO_SCHEMA_STRING);

  private static final LogicalSchema SINGLE_FIELD_SCHEMA =
      toKsqlSchema(SINGLE_FIELD_AVRO_SCHEMA_STRING);

  private static final LogicalSchema SCHEMA_WITH_MAPS = LogicalSchema.builder()
      .valueColumn(ColumnName.of("notmap"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("mapcol"), SqlTypes.map(SqlTypes.INTEGER))
      .build();

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
  private CreateSourceCommand ddlCommand;

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

  @Before
  public void setUp() {
    when(ddlCommand.getSerdeOptions()).thenReturn(SerdeOption.none());
    when(ddlCommand.getSchema()).thenReturn(MUTLI_FIELD_SCHEMA);
    when(ddlCommand.getSourceName()).thenReturn(STREAM_NAME);
    when(ddlCommand.getTopic()).thenReturn(RESULT_TOPIC);
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSubject() throws Exception {
    // Given:
    when(srClient.testCompatibility(anyString(), any())).thenReturn(true);

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);

    // Then:
    verify(srClient).testCompatibility(eq(RESULT_TOPIC.getKafkaTopicName() + "-value"), any());
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSchema() throws Exception {
    // Given:
    final PhysicalSchema schema = PhysicalSchema.from(MUTLI_FIELD_SCHEMA, SerdeOption.none());

    final org.apache.avro.Schema expectedAvroSchema = AvroSchemas
        .getAvroSchema(schema.valueSchema(), STREAM_NAME.name(), ksqlConfig);
    when(srClient.testCompatibility(anyString(), any())).thenReturn(true);

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldValidateSchemaWithMaps() throws Exception {
    // Given:
    when(ddlCommand.getSchema()).thenReturn(SCHEMA_WITH_MAPS);
    final PhysicalSchema schema = PhysicalSchema
        .from(SCHEMA_WITH_MAPS, SerdeOption.none());

    when(srClient.testCompatibility(anyString(), any())).thenReturn(true);

    final org.apache.avro.Schema expectedAvroSchema = AvroSchemas
        .getAvroSchema(schema.valueSchema(), STREAM_NAME.name(), ksqlConfig);

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldValidateWrappedSingleFieldSchemaEvolution() throws Exception {
    // Given:
    when(ddlCommand.getSchema()).thenReturn(SINGLE_FIELD_SCHEMA);
    final PhysicalSchema schema = PhysicalSchema
        .from(SINGLE_FIELD_SCHEMA, SerdeOption.none());

    when(srClient.testCompatibility(anyString(), any())).thenReturn(true);

    final org.apache.avro.Schema expectedAvroSchema = AvroSchemas
        .getAvroSchema(schema.valueSchema(), STREAM_NAME.name(), ksqlConfig);

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldValidateUnwrappedSingleFieldSchemaEvolution() throws Exception {
    // Given:
    when(ddlCommand.getSchema()).thenReturn(SINGLE_FIELD_SCHEMA);
    when(ddlCommand.getSerdeOptions())
        .thenReturn(ImmutableSet.of(SerdeOption.UNWRAP_SINGLE_VALUES));
    final PhysicalSchema schema = PhysicalSchema
        .from(SINGLE_FIELD_SCHEMA, SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES));

    when(srClient.testCompatibility(anyString(), any())).thenReturn(true);

    final org.apache.avro.Schema expectedAvroSchema = AvroSchemas
        .getAvroSchema(schema.valueSchema(), STREAM_NAME.name(), ksqlConfig);

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldNotThrowInvalidEvolution() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any())).thenReturn(true);

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);
  }

  @Test
  public void shouldReturnInvalidEvolution() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any())).thenReturn(false);

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Cannot register avro schema for actual-name as the schema is incompatible with the current schema version registered for the topic");

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);
  }

  @Test
  public void shouldNotThrowInvalidEvolutionIfSubjectNotRegistered() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any()))
        .thenThrow(new RestClientException("Unknown subject", 404, 40401));

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);
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
        ddlCommand.getTopic().getKafkaTopicName()
            + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
    )));

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);
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
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);
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
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient, ksqlConfig);
  }

  private static LogicalSchema toKsqlSchema(final String avroSchemaString) {
    final org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(avroSchemaString);
    final AvroData avroData = new AvroData(new AvroDataConfig(Collections.emptyMap()));
    final Schema connectSchema = new ConnectSchemaTranslator()
        .toKsqlSchema(avroData.toConnectSchema(avroSchema));

    final ConnectToSqlTypeConverter converter = SchemaConverters
        .connectToSqlConverter();

    final Builder builder = LogicalSchema.builder();
    connectSchema.fields()
        .forEach(f -> builder.valueColumn(ColumnName.of(f.name()), converter.toSqlType(f.schema())));

    return builder.build();
  }
}
