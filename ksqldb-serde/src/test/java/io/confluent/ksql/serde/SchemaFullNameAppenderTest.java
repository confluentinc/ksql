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

package io.confluent.ksql.serde;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaFullNameAppenderTest {
  private static final String TEST_NAME = "test";

  @Test
  public void testAppenderReturnsSingleStructSchemaWithFullName() {
    // Given
    final Schema unNamedSchema = SchemaBuilder.struct().build();

    // When
    final Schema schema = SchemaFullNameAppender.appendSchemaFullName(unNamedSchema, TEST_NAME);

    // Then
    assertThat(schema, is(SchemaBuilder.struct().name(TEST_NAME).build()));
  }

  @Test
  public void testAppenderReturnsStructSchemaInArrayWithFullName() {
    // Given
    final Schema unNamedSchema = SchemaBuilder.array(
        SchemaBuilder.struct().build()
    ).build();

    // When
    final Schema schema = SchemaFullNameAppender.appendSchemaFullName(unNamedSchema, TEST_NAME);

    // Then
    assertThat(schema, is(SchemaBuilder.array(
        SchemaBuilder.struct().name(TEST_NAME).build()
    ).build()));
  }

  @Test
  public void testAppenderReturnsMapWithFullName() {
    // Given
    final Schema unNamedSchema = SchemaBuilder.map(
        SchemaBuilder.struct().build(),
        SchemaBuilder.struct().build()
    );

    // When
    final Schema schema = SchemaFullNameAppender.appendSchemaFullName(unNamedSchema, TEST_NAME);

    // Then
    assertThat(schema, is(SchemaBuilder.map(
        SchemaBuilder.struct().name(TEST_NAME + "_MapKey").build(),
        SchemaBuilder.struct().name(TEST_NAME + "_MapValue").build()
    ).name(TEST_NAME).build()));
  }

  @Test
  public void testReplacesInvalidDottedFieldNamesToValidFieldNames() {
    // Given
    final Schema unNamedSchema = SchemaBuilder.struct()
        .field("internal.struct",
            SchemaBuilder.struct()
                .field("product.id", Schema.INT32_SCHEMA)
                .build())
        .build();

    // When
    final Schema schema = SchemaFullNameAppender.appendSchemaFullName(unNamedSchema, TEST_NAME);

    // Then
    assertThat(schema, is(SchemaBuilder.struct()
        .field("internal_struct",
            SchemaBuilder.struct()
                .field("product_id", Schema.INT32_SCHEMA)
                .name(TEST_NAME + "_internal.struct")
                .build())
        .name(TEST_NAME)
        .build()));

  }
}
