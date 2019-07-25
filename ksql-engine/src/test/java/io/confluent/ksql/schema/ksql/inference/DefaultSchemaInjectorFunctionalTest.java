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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.ParserUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSchemaInjectorFunctionalTest {

  private static final SqlSchemaFormatter FORMATTER =
      new SqlSchemaFormatter(ParserUtil::isReservedIdentifier);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
  private MetaStore metaStore;
  private DefaultSchemaInjector schemaInjector;

  @Before
  public void setUp() {
    schemaInjector = new DefaultSchemaInjector(new SchemaRegistryTopicSchemaSupplier(srClient));
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
            .field("INNER1", Schema.OPTIONAL_INT32_SCHEMA)
            .field("INNER2", Schema.OPTIONAL_STRING_SCHEMA)
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
            .field("INNER1", Schema.OPTIONAL_INT32_SCHEMA)
            .field("INNER2", Schema.OPTIONAL_STRING_SCHEMA)
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
  public void shouldInferDateAsIntegeer() {
    shouldInferType(
        org.apache.avro.LogicalTypes.date().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()
        ),
        Schema.OPTIONAL_INT32_SCHEMA
    );
  }

  @Test
  public void shouldInferTimeMillisAsInteger() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timeMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().intType()
        ),
        Schema.OPTIONAL_INT32_SCHEMA
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
  public void shouldInferTimestampMillisAsBigint() {
    shouldInferType(
        org.apache.avro.LogicalTypes.timestampMillis().addToSchema(
            org.apache.avro.SchemaBuilder.builder().longType()
        ),
        Schema.OPTIONAL_INT64_SCHEMA
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
            .field("INT", Schema.OPTIONAL_INT32_SCHEMA)
            .field("STRING", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldThrowIfNoFields() {
    // expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("STRUCT type must define fields");

    // when:
    shouldInferConnectType(SchemaBuilder.struct(), null);
  }

  @Test
  public void shouldIgnoreFixed() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.record("foo").fields()
            .name("fixed_field").type(org.apache.avro.SchemaBuilder.fixed("fixed").size(32)).noDefault()
            .nullableString("STRING", "bar")
            .endRecord(),
        SchemaBuilder.struct()
            .field("STRING", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
    );
  }

  @Test
  public void shouldIgnoreBytes() {
    shouldInferType(
        org.apache.avro.SchemaBuilder.record("foo").fields()
            .nullableBytes("bytes", new byte[]{})
            .nullableString("STRING", "bar")
            .endRecord(),
        SchemaBuilder.struct()
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
        SchemaBuilder.struct().field("FOO", Schema.OPTIONAL_STRING_SCHEMA).optional().build()
    );
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
        .field("ARRAYINNER1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ARRAYINNER2", Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();
    final Schema ksqlMapInner = SchemaBuilder.struct()
        .field("MAPINNER1", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("MAPINNER2", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();
    final Schema ksqlStructInner2 = SchemaBuilder.struct()
        .field("STRUCTINNER2_1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("STRUCTINNER2_2", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();
    final Schema ksqlStructInner1 = SchemaBuilder.struct()
        .field("STRUCTINNER1_1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("STRUCTINNER1_2", ksqlStructInner2)
        .optional()
        .build();
    final Schema ksqlSchema = SchemaBuilder.struct()
        .field("PRIMITIVE", Schema.OPTIONAL_INT32_SCHEMA)
        .field("ARRAY", ksqlArrayInner)
        .field("MAP", ksqlMapInner)
        .field("STRUCT", ksqlStructInner1)
        .build();

    shouldInferSchema(
        new AvroData(1).fromConnectSchema(connectSchema),
        ksqlSchema);
  }

  private void shouldInferType(
      final org.apache.avro.Schema avroSchema,
      final Schema expectedKsqlSchema
  ) {
    final org.apache.avro.Schema avroStreamSchema
        = org.apache.avro.SchemaBuilder.record("stream")
        .fields()
        .name("field0").type(avroSchema).noDefault()
        .endRecord();

    final SchemaBuilder ksqlStreamSchemaBuilder = SchemaBuilder.struct();
    if (expectedKsqlSchema != null) {
      ksqlStreamSchemaBuilder.field("FIELD0", expectedKsqlSchema);
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
    try {
      when(srClient.getLatestSchemaMetadata(any()))
          .thenReturn(new SchemaMetadata(1, 1, avroSchema.toString()));
    } catch (final Exception e) {
      throw new AssertionError(e);
    }

    final String stmtNoSchema =
        "CREATE STREAM TEST WITH (KAFKA_TOPIC='test', VALUE_FORMAT='avro');";

    final PreparedStatement<Statement> prepared = KsqlParserTestUtil
        .buildSingleAst(stmtNoSchema, metaStore);

    // When:
    final ConfiguredStatement<?> inferred = schemaInjector.inject(
        ConfiguredStatement.of(prepared, ImmutableMap.of(), new KsqlConfig(ImmutableMap.of())));

    // Then:
    final Statement withSchema = KsqlParserTestUtil
        .buildSingleAst(inferred.getStatementText(), metaStore)
        .getStatement();

    final Schema actual = getSchemaForDdlStatement((CreateSource) withSchema);

    Assert.assertThat(FORMATTER.format(actual),
        equalTo(FORMATTER.format(expectedKqlSchema)));
    Assert.assertThat(actual, equalTo(expectedKqlSchema));
  }

  private static Schema getSchemaForDdlStatement(final CreateSource statement) {
    final SchemaBuilder builder = SchemaBuilder.struct();
    for (final TableElement tableElement : statement.getElements()) {
      builder.field(
          tableElement.getName(),
          SchemaConverters.sqlToConnectConverter().toConnectSchema(tableElement.getType().getSqlType())
      );
    }
    return builder.build();
  }

  private static org.apache.avro.Schema parseAvroSchema(final String avroSchema) {
    return new org.apache.avro.Schema.Parser().parse(avroSchema);
  }
}