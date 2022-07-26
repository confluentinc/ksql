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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class ProtobufSchemasTest {
  private static final String CUSTOM_FULL_SCHEMA_NAME = "io.CustomName";

  @Test
  public void shouldNameSchemaWithoutName() {
    // Given
    final Schema unnamedSchema = SchemaBuilder.struct()
        .field("field1", Schema.INT32_SCHEMA)
        .field("field2",
            SchemaBuilder.struct()
                .field("product_id", Schema.INT32_SCHEMA)
                .build())
        .build();

    // When
    final Schema schemaWithName = ProtobufSchemas.schemaWithName(unnamedSchema, CUSTOM_FULL_SCHEMA_NAME);

    // Then
    assertThat(schemaWithName, is(SchemaBuilder.struct()
        .field("field1", Schema.INT32_SCHEMA)
        .field("field2",
            SchemaBuilder.struct()
                .field("product_id", Schema.INT32_SCHEMA)
                .build())
        .name(CUSTOM_FULL_SCHEMA_NAME)
        .build()));
  }

  @Test
  public void shouldReplaceSchemaName() {
    // Given
    final Schema namedSchema = SchemaBuilder.struct()
        .field("field1", Schema.INT32_SCHEMA)
        .field("field2",
            SchemaBuilder.struct()
                .field("product_id", Schema.INT32_SCHEMA)
                .build())
        .name("Ole")
        .build();

    // When
    final Schema schemaWithNewName = ProtobufSchemas.schemaWithName(namedSchema, CUSTOM_FULL_SCHEMA_NAME);

    // Then
    assertThat(schemaWithNewName, is(SchemaBuilder.struct()
        .field("field1", Schema.INT32_SCHEMA)
        .field("field2",
            SchemaBuilder.struct()
                .field("product_id", Schema.INT32_SCHEMA)
                .build())
        .name(CUSTOM_FULL_SCHEMA_NAME)
        .build()));
  }

  @Test
  public void shouldReturnSameSchemaOnNullNewName() {
    // Given
    final Schema namedSchema = SchemaBuilder.struct()
        .field("field1", Schema.INT32_SCHEMA)
        .field("field2",
            SchemaBuilder.struct()
                .field("product_id", Schema.INT32_SCHEMA)
                .build())
        .build();

    // When
    final Schema schemaWithNewName = ProtobufSchemas.schemaWithName(namedSchema, null);

    // Then
    assertThat(schemaWithNewName, is(SchemaBuilder.struct()
        .field("field1", Schema.INT32_SCHEMA)
        .field("field2",
            SchemaBuilder.struct()
                .field("product_id", Schema.INT32_SCHEMA)
                .build())
        .build()));
  }
}
