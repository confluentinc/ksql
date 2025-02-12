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

import static io.confluent.ksql.serde.connect.ConnectKsqlSchemaTranslator.OPTIONAL_DATE_SCHEMA;
import static io.confluent.ksql.serde.connect.ConnectKsqlSchemaTranslator.OPTIONAL_TIMESTAMP_SCHEMA;
import static io.confluent.ksql.serde.connect.ConnectKsqlSchemaTranslator.OPTIONAL_TIME_SCHEMA;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SchemaTranslationPolicies;
import io.confluent.ksql.serde.SchemaTranslationPolicy;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.connect.ConnectKsqlSchemaTranslator;
import io.confluent.ksql.serde.kafka.KafkaFormat;
import io.confluent.ksql.services.KafkaConsumerGroupClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.IdentifierUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlParserTestUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
public class DefaultSchemaInjectorFunctionalTest {

  private enum TestType {
    CS_WITH_SCHEMA_ID,
    WITHOUT_SCHEMA_ID,
    CSAS_WITH_SCHEMA_ID
  }

  private static final Object[][] TEST_DATA = {
      {
          "with_schema_id",
          TestType.CS_WITH_SCHEMA_ID,
          SchemaTranslationPolicy.ORIGINAL_FIELD_NAME,
          "CREATE STREAM TEST WITH (KAFKA_TOPIC='test', KEY_FORMAT='kafka', VALUE_FORMAT='avro', VALUE_SCHEMA_ID=1);"
      },
      {
          "without_schema_id",
          TestType.WITHOUT_SCHEMA_ID,
          SchemaTranslationPolicy.UPPERCASE_FIELD_NAME,
          "CREATE STREAM TEST WITH (KAFKA_TOPIC='test', KEY_FORMAT='kafka', VALUE_FORMAT='avro');"
      },
      {
          "csas_with_schema_ia",
          TestType.CSAS_WITH_SCHEMA_ID,
          SchemaTranslationPolicy.ORIGINAL_FIELD_NAME,
          "CREATE STREAM TEST WITH (VALUE_FORMAT='avro', VALUE_SCHEMA_ID=1) AS SELECT * FROM TEST_SOURCE EMIT CHANGES;"
      }
  };

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(TEST_DATA);
  }

  private static final SqlSchemaFormatter FORMATTER =
      new SqlSchemaFormatter(IdentifierUtil::needsQuotes);

  private static final org.apache.avro.Schema DECIMAL_SCHEMA =
      parseAvroSchema(
          "{"
              + "\"type\": \"bytes\","
              + "\"logicalType\": \"decimal\","
              + "\"precision\": 4,"
              + "\"scale\": 2"
              + "}");

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private AvroSchema avroSchema;
  @Mock
  private MetaStore metaStore;
  @Mock
  KsqlStream ksqlStream;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlExecutionContext executionContext;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private KafkaConsumerGroupClient consumerGroupClient;
  @Mock
  private KsqlExecutionContext executionSandbox;
  @Mock
  private KsqlPlan ksqlPlan;
  @Mock
  private CreateSourceCommand ddlCommand;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private DefaultSchemaInjector schemaInjector;
  private final TestType testType;
  private final SchemaTranslationPolicy translationPolicy;
  private final String statement;

  @SuppressWarnings("unused")
  public DefaultSchemaInjectorFunctionalTest(
      final String testName,
      final TestType testType,
      final SchemaTranslationPolicy policy,
      final String statement
  ) {
    this.testType = testType;
    this.translationPolicy = policy;
    this.statement = statement;
  }

  @Before
  public void setUp() {
    schemaInjector = new DefaultSchemaInjector(new SchemaRegistryTopicSchemaSupplier(srClient),
        executionContext, serviceContext);
  }

  @Test
  public void shouldInferIntAsInteger() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().intType(),
        Schema.OPTIONAL_INT32_SCHEMA
    );
  }

  @Test
  public void shouldInferLongAsLong() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().longType(),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldInferBooleanAsBoolean() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().booleanType(),
        Schema.OPTIONAL_BOOLEAN_SCHEMA
    );
  }

  @Test
  public void shouldInferStringAsString() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().stringType(),
        Schema.OPTIONAL_STRING_SCHEMA
    );
  }

  @Test
  public void shouldInferFloatAsDouble() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().floatType(),
        Schema.OPTIONAL_FLOAT64_SCHEMA
    );
  }

  @Test
  public void shouldInferDoubleAsDouble() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().doubleType(),
        Schema.OPTIONAL_FLOAT64_SCHEMA
    );
  }

  @Test
  public void shouldInferDecimalAsDecimal() {
    shouldInferType(
        DECIMAL_SCHEMA,
        DecimalUtil.builder(4, 2).build()
    );
  }

  @Test
  public void shouldInferArrayAsArray() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().array().items(
            org.apache.avro.SchemaBuilder.builder().longType()
        ),
        SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build()
    );
  }

  @Test
  public void shouldInferMapAsMap() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.builder().map().values(
            org.apache.avro.SchemaBuilder.builder().intType()
        ),
        SchemaBuilder.map(
            Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA
        ).optional().build()
    );
  }

  @Test
  public void shouldInferRecordAsAStruct() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.record("inner_record").fields()
            .name("inner1").type().intType().noDefault()
            .name("inner2").type().stringType().noDefault()
            .endRecord(),
        SchemaBuilder.struct()
            .field("inner1", Schema.OPTIONAL_INT32_SCHEMA)
            .field("inner2", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferRecordWithOptionalField() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.record("inner_record").fields()
            .name("inner1").type().optional().intType()
            .name("inner2").type().optional().stringType()
            .endRecord(),
        SchemaBuilder.struct()
            .field("inner1", Schema.OPTIONAL_INT32_SCHEMA)
            .field("inner2", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferOptionalField() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.unionOf().nullType().and().intType().endUnion(),
        Schema.OPTIONAL_INT32_SCHEMA
    );
  }

  @Test
  public void shouldInferEnumAsString() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.enumeration("foo").symbols("A", "B", "C"),
        Schema.OPTIONAL_STRING_SCHEMA
    );
  }

  @Test
  public void shouldInferDateAsDate() {
    shouldInferType(
        org.apache.avro.LogicalTypes.date().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()
        ),
        OPTIONAL_DATE_SCHEMA
    );
  }

  @Test
  public void shouldInferTimeMillisAsTime() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timeMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()
        ),
        OPTIONAL_TIME_SCHEMA
    );
  }

  @Test
  public void shouldInferTimeMicrosAsBigint() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timeMicros().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()
        ),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldInferTimestampMillisAsTimestamp() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timestampMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()
        ),
        OPTIONAL_TIMESTAMP_SCHEMA
    );

  }

  @Test
  public void shouldInferTimestampMicrosAsBigint() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timestampMicros().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()
        ),
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  @Test
  public void shouldInferUnionAsStruct() {
    shouldInferType(
        org.apache.avro.SchemaBuilder
            .unionOf()
            .intType()
            .and()
            .stringType()
            .endUnion(),
        SchemaBuilder
            .struct()
            .field("int", Schema.OPTIONAL_INT32_SCHEMA)
            .field("string", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldIgnoreFixed() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.record("foo").fields()
            .name("fixed_field").type(org.apache.avro.SchemaBuilder.fixed("fixed").size(32))
            .noDefault()
            .nullableString("STRING", "bar")
            .endRecord(),
        SchemaBuilder.struct()
            .field("fixed_field", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("STRING", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferBytes() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.record("foo").fields()
            .nullableBytes("bytes", new byte[]{})
            .nullableString("STRING", "bar")
            .endRecord(),
        SchemaBuilder.struct()
            .field("bytes", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("STRING", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferInt8AsInteger() {
    shouldInferConnectType(Schema.INT8_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldInferInt16AsInteger() {
    shouldInferConnectType(Schema.INT16_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldInferOptional() {
    shouldInferConnectType(Schema.OPTIONAL_INT32_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA);
  }

  @Test
  public void shouldInferConnectMapWithOptionalStringKey() {
    shouldInferConnectType(
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build(),
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferConnectMapWithInt8Key() {
    shouldInferConnectType(
        SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build(),
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferConnectMapWithInt16Key() {
    shouldInferConnectType(
        SchemaBuilder.map(Schema.INT16_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build(),
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferConnectMapWithInt32Key() {
    shouldInferConnectType(
        SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build(),
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferConnectMapWithInt64Key() {
    shouldInferConnectType(
        SchemaBuilder.map(Schema.INT64_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build(),
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldInferConnectMapWithBooleanKey() {
    shouldInferConnectType(
        SchemaBuilder.map(Schema.BOOLEAN_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build(),
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldIgnoreConnectMapWithUnsupportedKey() {
    final Schema map = SchemaBuilder.map(Schema.BYTES_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA).build();
    final Schema schema =
        SchemaBuilder.struct()
            .field("map", map)
            .field("foo", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    shouldInferConnectType(
        schema,
        SchemaBuilder.struct().field("foo", Schema.OPTIONAL_STRING_SCHEMA).optional().build()
    );
  }

  @Test
  public void shouldInferEmptyStruct() {
    final Schema emptyStruct = SchemaBuilder.struct().optional().build();
    shouldInferConnectType(emptyStruct, emptyStruct);
  }

  @Test
  public void shouldInferComplexConnectSchema() {
    final Schema arrayInner = SchemaBuilder.struct()
        .field("arrayInner1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("arrayInner2", Schema.INT64_SCHEMA)
        .name("arrayInner")
        .build();
    final Schema mapInner = SchemaBuilder.struct()
        .field("mapInner1", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("mapInner2", Schema.STRING_SCHEMA)
        .name("mapInner")
        .build();
    final Schema structInner2 = SchemaBuilder.struct()
        .field("structInner2_1", Schema.BOOLEAN_SCHEMA)
        .field("structInner2_2", Schema.OPTIONAL_INT32_SCHEMA)
        .name("structInner2")
        .optional()
        .build();
    final Schema structInner1 = SchemaBuilder.struct()
        .field("structInner1_1", Schema.STRING_SCHEMA)
        .field("structInner1_2", structInner2)
        .name("structInner1")
        .build();
    final Schema connectSchema = SchemaBuilder.struct()
        .field("primitive", Schema.INT32_SCHEMA)
        .field("array", arrayInner)
        .field("map", mapInner)
        .field("struct", structInner1)
        .build();

    final Schema ksqlArrayInner = SchemaBuilder.struct()
        .field("arrayInner1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("arrayInner2", Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();
    final Schema ksqlMapInner = SchemaBuilder.struct()
        .field("mapInner1", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("mapInner2", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();
    final Schema ksqlStructInner2 = SchemaBuilder.struct()
        .field("structInner2_1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("structInner2_2", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();
    final Schema ksqlStructInner1 = SchemaBuilder.struct()
        .field("structInner1_1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("structInner1_2", ksqlStructInner2)
        .optional()
        .build();
    final Schema ksqlSchema = SchemaBuilder.struct()
        .field("primitive", Schema.OPTIONAL_INT32_SCHEMA)
        .field("array", ksqlArrayInner)
        .field("map", ksqlMapInner)
        .field("struct", ksqlStructInner1)
        .build();

    shouldInferSchema(
        new AvroData(1).fromConnectSchema(connectSchema),
        ksqlSchema);
  }

  private void shouldInferType(
      final org.apache.avro.Schema avroSchema,
      final Schema expectedKsqlSchema
  ) {
    final FieldAssembler<org.apache.avro.Schema> fieldAssembler
        = org.apache.avro.SchemaBuilder.record("stream")
        .fields()
        .name("field0").type(avroSchema).noDefault();

    // Add extra field in SR returned schema
    if (this.testType == TestType.CSAS_WITH_SCHEMA_ID) {
      fieldAssembler.name("field1")
          .type(org.apache.avro.SchemaBuilder.builder().intType()).noDefault();
    }

    org.apache.avro.Schema avroStreamSchema = fieldAssembler.endRecord();

    final SchemaBuilder ksqlStreamSchemaBuilder = SchemaBuilder.struct();
    if (expectedKsqlSchema != null) {
      ksqlStreamSchemaBuilder.field("field0", expectedKsqlSchema);
      if (this.testType == TestType.CSAS_WITH_SCHEMA_ID) {
        ksqlStreamSchemaBuilder.field("field1", Schema.OPTIONAL_INT32_SCHEMA);
      }
    }

    shouldInferSchema(avroStreamSchema, ksqlStreamSchemaBuilder.build());
  }

  private void shouldInferConnectType(
      final Schema connectSchema,
      final Schema expectedKsqlSchema
  ) {
    shouldInferType(
        new AvroData(1).fromConnectSchema(connectSchema),
        expectedKsqlSchema
    );
  }

  private void shouldInferSchema(
      final org.apache.avro.Schema avroSchema,
      final Schema expectedKqlSchema
  ) {
    // Given:
    final Schema expectedSchema = new ConnectKsqlSchemaTranslator(
        SchemaTranslationPolicies.of(this.translationPolicy))
        .toKsqlSchema(expectedKqlSchema);

    try {
      if (testType == TestType.WITHOUT_SCHEMA_ID) {
        when(srClient.getLatestSchemaMetadata(any()))
            .thenReturn(new SchemaMetadata(1, 1, avroSchema.toString()));
      }
      when(srClient.getSchemaBySubjectAndId(any(), anyInt())).thenReturn(this.avroSchema);
      when(this.avroSchema.schemaType()).thenReturn("AVRO");
      when(this.avroSchema.rawSchema()).thenReturn(avroSchema);
      constructCSASSource(expectedSchema);
    } catch (final Exception e) {
      throw new AssertionError(e);
    }

    final PreparedStatement<Statement> prepared = KsqlParserTestUtil
        .buildSingleAst(this.statement, metaStore);

    // When:
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
    final ConfiguredStatement<?> inferred = schemaInjector.inject(
        ConfiguredStatement.of(prepared, SessionConfig.of(ksqlConfig, ImmutableMap.of())));

    // Then:
    if (testType == TestType.CSAS_WITH_SCHEMA_ID) {
      validateCsasInference((ConfiguredStatement<CreateStreamAsSelect>) inferred, this.avroSchema);
    } else {
      final Statement withSchema = KsqlParserTestUtil
          .buildSingleAst(inferred.getMaskedStatementText(), metaStore)
          .getStatement();

      final Schema actual = getSchemaForDdlStatement((CreateSource) withSchema);

      assertThat(FORMATTER.format(actual),
          equalTo(FORMATTER.format(expectedSchema)));
      assertThat(actual, equalTo(expectedSchema));
    }
  }

  private static void validateCsasInference(final ConfiguredStatement<CreateStreamAsSelect> statement,
      final AvroSchema expectedSchema) {
    assertThat(statement.getSessionConfig().getOverrides(),
        hasKey(CommonCreateConfigs.VALUE_SCHEMA_ID));
    final SchemaAndId schemaAndId = (SchemaAndId) statement.getSessionConfig().getOverrides().
        get(CommonCreateConfigs.VALUE_SCHEMA_ID);
    assertThat(schemaAndId.rawSchema, is(expectedSchema));
  }

  private void constructCSASSource(Schema valueSchema) {
    if (testType != TestType.CSAS_WITH_SCHEMA_ID) {
      return;
    }
    LogicalSchema.Builder builder = LogicalSchema.builder();
    valueSchema.fields().forEach(
        field -> {
          if (!field.name().equals("field1")) {
            builder.valueColumn(ColumnName.of(field.name()),
                SchemaConverters.connectToSqlConverter().toSqlType(field.schema()));
          }
        });
    builder.keyColumn(ColumnName.of("key"), SqlTypes.INTEGER);
    LogicalSchema schema = builder.build();

    when(metaStore.getSource(any())).thenReturn(ksqlStream);
    when(ksqlStream.getSchema()).thenReturn(schema);
    when(serviceContext.getSchemaRegistryClient()).thenReturn(srClient);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(serviceContext.getConsumerGroupClient()).thenReturn(consumerGroupClient);

    when(executionContext.createSandbox(any())).thenReturn(executionSandbox);

    when(executionSandbox.plan(any(), any())).thenReturn(ksqlPlan);
    when(ksqlPlan.getDdlCommand()).thenReturn(Optional.of(ddlCommand));
    when(ddlCommand.getSchema()).thenReturn(schema);
    when(ddlCommand.getFormats()).thenReturn(
        Formats.of(
            FormatInfo.of(KafkaFormat.NAME),
            FormatInfo.of(AvroFormat.NAME),
            SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES),
            SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)
        )
    );
  }

  private static Schema getSchemaForDdlStatement(final CreateSource statement) {
    final SchemaBuilder builder = SchemaBuilder.struct();
    for (final TableElement tableElement : statement.getElements()) {
      builder.field(
          tableElement.getName().text(),
          SchemaConverters.sqlToConnectConverter()
              .toConnectSchema(tableElement.getType().getSqlType())
      );
    }
    return builder.optional().build();
  }

  private static org.apache.avro.Schema parseAvroSchema(final String avroSchema) {
    return new org.apache.avro.Schema.Parser().parse(avroSchema);
  }
}