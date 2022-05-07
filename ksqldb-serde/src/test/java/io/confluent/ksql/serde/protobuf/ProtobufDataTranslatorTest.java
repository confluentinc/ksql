/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class ProtobufDataTranslatorTest {
  @Test
  public void shouldNameParentSchemaOnly() {
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

    final ProtobufDataTranslator dataTranslator = new ProtobufDataTranslator(
        schema,
        ""
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
    assertThat(namedSchema.field("ARRAY").schema().type(), equalTo(Schema.Type.ARRAY));
    assertThat(namedSchema.field("MAP").schema().type(), equalTo(Schema.Type.MAP));
    assertThat(namedSchema.field("STRUCT").schema().type(), equalTo(Schema.Type.STRUCT));

    final Schema namedArrayInner = namedSchema.field("ARRAY").schema().valueSchema();
    assertThat(namedArrayInner.type(), equalTo(Schema.Type.STRUCT));
    assertThat(namedArrayInner.fields().size(), equalTo(1));
    assertThat(
        namedArrayInner.field("ARRAY_INNER").schema(),
        equalTo(Schema.OPTIONAL_INT32_SCHEMA));
    assertThat(namedArrayInner.name(), nullValue());

    final Schema namedMapInner = namedSchema.field("MAP").schema().valueSchema();
    assertThat(namedMapInner.type(), equalTo(Schema.Type.STRUCT));
    assertThat(namedMapInner.fields().size(), equalTo(1));
    assertThat(
        namedMapInner.field("MAP_INNER").schema(),
        equalTo(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(namedMapInner.name(), nullValue());

    final Schema namedStructInner = namedSchema.field("STRUCT").schema();
    assertThat(namedStructInner.type(), equalTo(Schema.Type.STRUCT));
    assertThat(namedStructInner.fields().size(), equalTo(1));
    assertThat(
        namedStructInner.field("STRUCT_INNER").schema(),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(namedStructInner.name(), nullValue());

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

    final ProtobufDataTranslator dataTranslator = new ProtobufDataTranslator(
        schema,
        ""
    );

    final Struct ksqlRow = new Struct(schema);

    // When:
    final Struct struct = (Struct)dataTranslator.toConnectRow(ksqlRow);

    assertThat(struct.get("COLUMN_NAME"), nullValue());

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

    final ProtobufDataTranslator dataTranslator = new ProtobufDataTranslator(schema, schemaFullName);
    final Struct ksqlRow = new Struct(schema)
        .put("COLUMN_NAME", 123L);

    // When:
    final Struct struct = (Struct)dataTranslator.toConnectRow(ksqlRow);

    // Then:
    assertThat(struct.schema().name(), equalTo(schemaFullName));
  }
}