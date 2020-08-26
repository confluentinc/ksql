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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.ConnectToSqlTypeConverter;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.avro.AvroSchemas;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import java.io.IOException;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
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
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("notmap"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("mapcol"), SqlTypes.map(SqlTypes.INTEGER))
      .build();

  private static final String SCHEMA_NAME = "schema_name";

  private static final String RESULT_TOPIC_NAME = "actual-name";

  private static final Formats FORMATS = Formats
      .of(
          KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
          ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name(), ImmutableMap
              .of(AvroFormat.FULL_SCHEMA_NAME, SCHEMA_NAME))),
          SerdeOptions.of()
      );

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private CreateSourceCommand ddlCommand;

  @Before
  public void setUp() {
    when(ddlCommand.getFormats()).thenReturn(FORMATS);
    when(ddlCommand.getSchema()).thenReturn(MUTLI_FIELD_SCHEMA);
    when(ddlCommand.getTopicName()).thenReturn(RESULT_TOPIC_NAME);
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSubject() throws Exception {
    // Given:
    when(srClient.testCompatibility(anyString(), any(AvroSchema.class))).thenReturn(true);

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient);

    // Then:
    verify(srClient).testCompatibility(eq(RESULT_TOPIC_NAME + "-value"), any(AvroSchema.class));
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSchema() throws Exception {
    // Given:
    final PhysicalSchema schema = PhysicalSchema.from(MUTLI_FIELD_SCHEMA, SerdeOptions.of());

    final AvroSchema expectedAvroSchema = new AvroSchema(AvroSchemas
        .getAvroSchema(schema.valueSchema(), SCHEMA_NAME));
    when(srClient.testCompatibility(anyString(), any(AvroSchema.class))).thenReturn(true);

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldValidateSchemaWithMaps() throws Exception {
    // Given:
    when(ddlCommand.getSchema()).thenReturn(SCHEMA_WITH_MAPS);
    final PhysicalSchema schema = PhysicalSchema
        .from(SCHEMA_WITH_MAPS, SerdeOptions.of());

    when(srClient.testCompatibility(anyString(), any(AvroSchema.class))).thenReturn(true);

    final AvroSchema expectedAvroSchema = new AvroSchema(AvroSchemas
        .getAvroSchema(schema.valueSchema(), SCHEMA_NAME));

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldValidateWrappedSingleFieldSchemaEvolution() throws Exception {
    // Given:
    when(ddlCommand.getSchema()).thenReturn(SINGLE_FIELD_SCHEMA);
    final PhysicalSchema schema = PhysicalSchema
        .from(SINGLE_FIELD_SCHEMA, SerdeOptions.of());

    when(srClient.testCompatibility(anyString(), any(AvroSchema.class))).thenReturn(true);

    final AvroSchema expectedAvroSchema = new AvroSchema(AvroSchemas
        .getAvroSchema(schema.valueSchema(), SCHEMA_NAME));

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldValidateUnwrappedSingleFieldSchemaEvolution() throws Exception {
    // Given:
    when(ddlCommand.getSchema()).thenReturn(SINGLE_FIELD_SCHEMA);
    when(ddlCommand.getFormats())
        .thenReturn(Formats.of(
            KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
            ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name())),
            SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES)
        ));
    final PhysicalSchema schema = PhysicalSchema
        .from(SINGLE_FIELD_SCHEMA, SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES));

    when(srClient.testCompatibility(anyString(), any(AvroSchema.class))).thenReturn(true);

    final AvroSchema expectedAvroSchema = new AvroSchema(AvroSchemas
        .getAvroSchema(schema.valueSchema(), SCHEMA_NAME));

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldNotThrowInvalidEvolution() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any(AvroSchema.class))).thenReturn(true);

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient);
  }

  @Test
  public void shouldReturnInvalidEvolution() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any(AvroSchema.class))).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot register avro schema for actual-name as the schema is incompatible "
            + "with the current schema version registered for the topic"
    ));
  }

  @Test
  public void shouldNotThrowInvalidEvolutionIfSubjectNotRegistered() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any(AvroSchema.class)))
        .thenThrow(new RestClientException("Unknown subject", 404, 40401));

    // When:
    AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient);
  }

  @Test
  public void shouldThrowOnSrAuthorizationErrors() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any(AvroSchema.class)))
        .thenThrow(new RestClientException("Unknown subject", 403, 40401));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not connect to Schema Registry service"));
    assertThat(e.getMessage(), containsString(
        String.format(
            "Not authorized to access Schema Registry subject: [%s]",
            ddlCommand.getTopicName()
                + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
        )));
  }

  @Test
  public void shouldThrowOnAnyOtherEvolutionSrException() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any(AvroSchema.class)))
        .thenThrow(new RestClientException("Unknown subject", 500, 40401));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not connect to Schema Registry service"));
  }

  @Test
  public void shouldThrowOnAnyOtherEvolutionIOException() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any(AvroSchema.class)))
        .thenThrow(new IOException("something"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> AvroUtil.throwOnInvalidSchemaEvolution(STATEMENT_TEXT, ddlCommand, srClient)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not check Schema compatibility"));
  }

  private static LogicalSchema toKsqlSchema(final String avroSchemaString) {
    final org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(avroSchemaString);
    final AvroData avroData = new AvroData(new AvroDataConfig(Collections.emptyMap()));
    final org.apache.kafka.connect.data.Schema connectSchema = new ConnectSchemaTranslator()
        .toKsqlSchema(avroData.toConnectSchema(avroSchema));

    final ConnectToSqlTypeConverter converter = SchemaConverters
        .connectToSqlConverter();

    final Builder builder = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING);

    connectSchema.fields()
        .forEach(f -> builder.valueColumn(ColumnName.of(f.name()), converter.toSqlType(f.schema())));

    return builder.build();
  }
}
