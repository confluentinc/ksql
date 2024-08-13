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

package io.confluent.ksql.serde.avro;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class AvroDataTranslatorTest {
  @Test
  public void shoudRenameSourceDereference() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("STREAM_NAME.COLUMN_NAME", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final AvroDataTranslator dataTranslator = new AvroDataTranslator(
        schema,
        AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME
    );

    final Struct ksqlRow = new Struct(schema)
        .put("STREAM_NAME.COLUMN_NAME", 123);

    // When:
    final Struct struct = (Struct)dataTranslator.toConnectRow(ksqlRow);

    // Then:
    assertThat(struct.schema(),
        equalTo(SchemaBuilder
            .struct()
            .name(struct.schema().name())
            .field("STREAM_NAME_COLUMN_NAME", Schema.OPTIONAL_INT32_SCHEMA)
            .build()
        )
    );
    assertThat(struct.get("STREAM_NAME_COLUMN_NAME"), equalTo(123));

    final Struct translatedRow = (Struct) dataTranslator.toKsqlRow(struct.schema(), struct);
    assertThat(translatedRow, equalTo(ksqlRow));
  }

  @Test
  public void shouldAddNamesToSchema() {
    // Given:
    final Schema arrayInner = SchemaBuilder.struct()
        .field("ARRAY_INNER", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();
    final Schema mapInner = SchemaBuilder.struct()
        .field("MAP_INNER", Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();
    final Schema structInner = SchemaBuilder.struct()
        .field("STRUCT_INNER", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();

    final Schema schema = SchemaBuilder.struct()
        .field("ARRAY", SchemaBuilder.array(arrayInner).optional().build())
        .field(
            "MAP",
            SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, mapInner).optional().build())
        .field("STRUCT", structInner)
        .optional()
        .build();

    final Struct arrayInnerStruct = new Struct(arrayInner)
        .put("ARRAY_INNER", 123);
    final Struct mapInnerStruct = new Struct(mapInner)
        .put("MAP_INNER", 456L);
    final Struct structInnerStruct = new Struct(structInner)
        .put("STRUCT_INNER", "foo");

    final AvroDataTranslator dataTranslator = new AvroDataTranslator(
        schema,
        AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME
    );

    final Struct ksqlRow = new Struct(schema)
        .put("ARRAY", ImmutableList.of(arrayInnerStruct))
        .put("MAP", ImmutableMap.of("bar", mapInnerStruct))
        .put("STRUCT", structInnerStruct);

    // When:
    final Struct struct = (Struct)dataTranslator.toConnectRow(ksqlRow);
    final Schema namedSchema = struct.schema();

    assertThat(namedSchema.type(), equalTo(Schema.Type.STRUCT));
    assertThat(namedSchema.name(), notNullValue());
    final String baseName = namedSchema.name();
    assertThat(namedSchema.field("ARRAY").schema().type(), equalTo(Schema.Type.ARRAY));
    assertThat(namedSchema.field("MAP").schema().type(), equalTo(Schema.Type.MAP));
    assertThat(namedSchema.field("STRUCT").schema().type(), equalTo(Schema.Type.STRUCT));

    final Schema namedArrayInner = namedSchema.field("ARRAY").schema().valueSchema();
    assertThat(namedArrayInner.type(), equalTo(Schema.Type.STRUCT));
    assertThat(namedArrayInner.fields().size(), equalTo(1));
    assertThat(
        namedArrayInner.field("ARRAY_INNER").schema(),
        equalTo(Schema.OPTIONAL_INT32_SCHEMA));
    assertThat(namedArrayInner.name(), equalTo(baseName + "_ARRAY"));

    final Schema namedMapInner = namedSchema.field("MAP").schema().valueSchema();
    assertThat(namedMapInner.type(), equalTo(Schema.Type.STRUCT));
    assertThat(namedMapInner.fields().size(), equalTo(1));
    assertThat(
        namedMapInner.field("MAP_INNER").schema(),
        equalTo(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(namedMapInner.name(), equalTo(baseName + "_MAP_MapValue"));

    final Schema namedStructInner = namedSchema.field("STRUCT").schema();
    assertThat(namedStructInner.type(), equalTo(Schema.Type.STRUCT));
    assertThat(namedStructInner.fields().size(), equalTo(1));
    assertThat(
        namedStructInner.field("STRUCT_INNER").schema(),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(namedStructInner.name(), equalTo(baseName + "_STRUCT"));

    assertThat(struct.schema(), equalTo(namedSchema));
    assertThat(
        ((Struct)struct.getArray("ARRAY").get(0)).getInt32("ARRAY_INNER"),
        equalTo(123));
    assertThat(
        ((Struct)struct.getMap("MAP").get("bar")).getInt64("MAP_INNER"),
        equalTo(456L));
    assertThat(
        struct.getStruct("STRUCT").getString("STRUCT_INNER"),
        equalTo("foo"));

    final Struct translatedRow = (Struct) dataTranslator.toKsqlRow(struct.schema(), struct);
    assertThat(translatedRow, equalTo(ksqlRow));
  }

  @Test
  public void shouldReplaceNullWithNull() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field(
            "COLUMN_NAME",
            SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build())
        .optional()
        .build();

    final AvroDataTranslator dataTranslator = new AvroDataTranslator(
        schema,
        AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME
    );

    final Struct ksqlRow = new Struct(schema);

    // When:
    final Struct struct = (Struct)dataTranslator.toConnectRow(ksqlRow);

    assertThat(struct.get("COLUMN_NAME"), nullValue());

    final Struct translatedRow = (Struct) dataTranslator.toKsqlRow(struct.schema(), struct);
    assertThat(translatedRow, equalTo(ksqlRow));
  }

  @Test
  public void shoudlReplacePrimitivesCorrectly() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("COLUMN_NAME", Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();

    final AvroDataTranslator dataTranslator = new AvroDataTranslator(
        schema,
        AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME
    );

    final Struct ksqlRow = new Struct(schema)
        .put("COLUMN_NAME", 123L);

    // When:
    final Struct struct = (Struct)dataTranslator.toConnectRow(ksqlRow);

    assertThat(struct.get("COLUMN_NAME"), equalTo(123L));

    final Struct translatedRow = (Struct) dataTranslator.toKsqlRow(struct.schema(), struct);
    assertThat(translatedRow, equalTo(ksqlRow));
  }

  @Test
  public void shouldUseExplicitSchemaName() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("COLUMN_NAME", Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();

    final String schemaFullName = "com.custom.schema";

    final AvroDataTranslator dataTranslator = new AvroDataTranslator(schema, schemaFullName);
    final Struct ksqlRow = new Struct(schema)
        .put("COLUMN_NAME", 123L);

    // When:
    final Struct struct = (Struct)dataTranslator.toConnectRow(ksqlRow);

    // Then:
    assertThat(struct.schema().name(), equalTo(schemaFullName));
  }

  @Test
  public void shouldDropOptionalFromRootPrimitiveSchema() {
    // Given:
    final Schema schema = Schema.OPTIONAL_INT64_SCHEMA;

    // When:
    final AvroDataTranslator translator =
        new AvroDataTranslator(schema, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    // Then:
    assertThat("Root required", translator.getAvroCompatibleSchema().isOptional(), is(false));
  }

  @Test
  public void shouldDropOptionalFromRootArraySchema() {
    // Given:
    final Schema schema = SchemaBuilder
        .array(Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();

    // When:
    final AvroDataTranslator translator =
        new AvroDataTranslator(schema, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    // Then:
    assertThat("Root required", translator.getAvroCompatibleSchema().isOptional(), is(false));
  }

  @Test
  public void shouldDropOptionalFromRootMapSchema() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();

    // When:
    final AvroDataTranslator translator =
        new AvroDataTranslator(schema, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    // Then:
    assertThat("Root required", translator.getAvroCompatibleSchema().isOptional(), is(false));
  }

  @Test
  public void shouldDropOptionalFromRootStructSchema() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("COLUMN_NAME", Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();

    // When:
    final AvroDataTranslator translator =
        new AvroDataTranslator(schema, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    // Then:
    assertThat("Root required", translator.getAvroCompatibleSchema().isOptional(), is(false));
  }

  @Test
  public void shouldRejectUnmatchingDecimalSchema() {
    // Given:
    final Schema ksqlSchema = DecimalUtil.builder(4, 2).build();
    final Schema topicSchema = DecimalUtil.builder(6, 3).build();

    // When:
    final AvroDataTranslator translator =
        new AvroDataTranslator(ksqlSchema, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    // Then:
    final ArithmeticException e = assertThrows(
        ArithmeticException.class,
        () -> translator.toKsqlRow(topicSchema, new BigDecimal("123.456")));
    assertThat(
        e.getMessage(),
        containsString("Numeric field overflow: A field with precision 4 and scale 2 must round to an absolute value less than 10^2"));
  }

  @Test
  public void shouldForceUnmatchingDecimalSchemaIfPossible() {
    // Given:
    final Schema ksqlSchema = DecimalUtil.builder(4, 2).build();
    final Schema topicSchema = DecimalUtil.builder(2, 1).build();

    // When:
    final AvroDataTranslator translator =
        new AvroDataTranslator(ksqlSchema, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    // Then:
    assertThat(
        translator.toKsqlRow(topicSchema, new BigDecimal("12.1")),
        is(new BigDecimal("12.10")));
  }

  @Test
  public void shouldRejectConversionsRequiringRounding() {
    // Given:
    final Schema ksqlSchema = DecimalUtil.builder(3, 0).build();
    final Schema topicSchema = DecimalUtil.builder(4, 1).build();

    // When:
    final AvroDataTranslator translator =
        new AvroDataTranslator(ksqlSchema, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    // Then:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> translator.toKsqlRow(topicSchema, new BigDecimal("123.4")));
    assertThat(
        e.getMessage(),
        containsString("Cannot fit decimal '123.4' into DECIMAL(3, 0) without rounding. (Requires 4,1)"));
  }

  @Test
  public void shouldReturnBytes() {
    // When:
    final AvroDataTranslator translator =
        new AvroDataTranslator(Schema.BYTES_SCHEMA, AvroProperties.DEFAULT_AVRO_SCHEMA_FULL_NAME);

    // Then:
    assertThat(
        translator.toKsqlRow(Schema.BYTES_SCHEMA, new byte[] {123}),
        is(ByteBuffer.wrap(new byte[] {123})));
  }
}
