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

package io.confluent.ksql.schema.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.util.DecimalUtil;
import java.util.function.Predicate;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class SqlSchemaFormatterTest {

  private static final Predicate<String> DO_NOT_ESCAPE_COLUMN_NAMES = word -> false;

  private static final SqlSchemaFormatter DEFAULT =
      new SqlSchemaFormatter(DO_NOT_ESCAPE_COLUMN_NAMES);

  private static final SqlSchemaFormatter STRICT =
      new SqlSchemaFormatter(DO_NOT_ESCAPE_COLUMN_NAMES, Option.APPEND_NOT_NULL);


  @Test
  public void shouldFormatBoolean() {
    assertThat(DEFAULT.format(Schema.BOOLEAN_SCHEMA), is("BOOLEAN"));
    assertThat(STRICT.format(Schema.BOOLEAN_SCHEMA), is("BOOLEAN NOT NULL"));
  }

  @Test
  public void shouldFormatOptionalBoolean() {
    assertThat(DEFAULT.format(Schema.OPTIONAL_BOOLEAN_SCHEMA), is("BOOLEAN"));
    assertThat(STRICT.format(Schema.OPTIONAL_BOOLEAN_SCHEMA), is("BOOLEAN"));
  }

  @Test
  public void shouldFormatInt() {
    assertThat(DEFAULT.format(Schema.INT32_SCHEMA), is("INT"));
    assertThat(STRICT.format(Schema.INT32_SCHEMA), is("INT NOT NULL"));
  }

  @Test
  public void shouldFormatOptionalInt() {
    assertThat(DEFAULT.format(Schema.OPTIONAL_INT32_SCHEMA), is("INT"));
    assertThat(STRICT.format(Schema.OPTIONAL_INT32_SCHEMA), is("INT"));
  }

  @Test
  public void shouldFormatBigint() {
    assertThat(DEFAULT.format(Schema.INT64_SCHEMA), is("BIGINT"));
    assertThat(STRICT.format(Schema.INT64_SCHEMA), is("BIGINT NOT NULL"));
  }

  @Test
  public void shouldFormatOptionalBigint() {
    assertThat(DEFAULT.format(Schema.OPTIONAL_INT64_SCHEMA), is("BIGINT"));
    assertThat(STRICT.format(Schema.OPTIONAL_INT64_SCHEMA), is("BIGINT"));
  }

  @Test
  public void shouldFormatDouble() {
    assertThat(DEFAULT.format(Schema.FLOAT64_SCHEMA), is("DOUBLE"));
    assertThat(STRICT.format(Schema.FLOAT64_SCHEMA), is("DOUBLE NOT NULL"));
  }

  @Test
  public void shouldFormatOptionalDouble() {
    assertThat(DEFAULT.format(Schema.OPTIONAL_FLOAT64_SCHEMA), is("DOUBLE"));
    assertThat(STRICT.format(Schema.OPTIONAL_FLOAT64_SCHEMA), is("DOUBLE"));
  }

  @Test
  public void shouldFormatString() {
    assertThat(DEFAULT.format(Schema.STRING_SCHEMA), is("VARCHAR"));
    assertThat(STRICT.format(Schema.STRING_SCHEMA), is("VARCHAR NOT NULL"));
  }

  @Test
  public void shouldFormatOptionalString() {
    assertThat(DEFAULT.format(Schema.OPTIONAL_STRING_SCHEMA), is("VARCHAR"));
    assertThat(STRICT.format(Schema.OPTIONAL_STRING_SCHEMA), is("VARCHAR"));
  }

  @Test
  public void shouldFormatDecimal() {
    assertThat(DEFAULT.format(DecimalUtil.builder(2, 1).build()), is("DECIMAL(2, 1)"));
    assertThat(STRICT.format(DecimalUtil.builder(2, 1).build()), is("DECIMAL(2, 1)"));
  }

  @Test
  public void shouldFormatArray() {
    // Given:
    final Schema schema = SchemaBuilder
        .array(Schema.FLOAT64_SCHEMA)
        .build();

    // Then:
    assertThat(DEFAULT.format(schema),
        is("ARRAY<DOUBLE>"));
    assertThat(STRICT.format(schema),
        is("ARRAY<DOUBLE NOT NULL> NOT NULL"));
  }

  @Test
  public void shouldFormatOptionalArray() {
    // Given:
    final Schema schema = SchemaBuilder
        .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
        .optional()
        .build();

    // Then:
    assertThat(DEFAULT.format(schema),
        is("ARRAY<DOUBLE>"));
    assertThat(STRICT.format(schema),
        is("ARRAY<DOUBLE>"));
  }

  @Test
  public void shouldFormatMap() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA)
        .build();

    // Then:
    assertThat(DEFAULT.format(schema),
        is("MAP<VARCHAR, DOUBLE>"));

    assertThat(STRICT.format(schema),
        is("MAP<VARCHAR NOT NULL, DOUBLE NOT NULL> NOT NULL"));
  }

  @Test
  public void shouldFormatOptionalMap() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .optional()
        .build();

    // Then:
    assertThat(DEFAULT.format(schema), is("MAP<VARCHAR, DOUBLE>"));
    assertThat(STRICT.format(schema), is("MAP<VARCHAR, DOUBLE>"));
  }

  @Test
  public void shouldFormatStruct() {
    // Given:
    final Schema structSchema = SchemaBuilder.struct()
        .field("COL1", Schema.STRING_SCHEMA)
        .field("COL4", SchemaBuilder
            .array(Schema.FLOAT64_SCHEMA)
            .build())
        .field("COL5", SchemaBuilder
            .map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA)
            .build())
        .build();

    // Then:
    assertThat(DEFAULT.format(structSchema), is(
        "STRUCT<"
            + "COL1 VARCHAR, "
            + "COL4 ARRAY<DOUBLE>, "
            + "COL5 MAP<VARCHAR, DOUBLE>"
            + ">"));

    assertThat(STRICT.format(structSchema), is(
        "STRUCT<"
            + "COL1 VARCHAR NOT NULL, "
            + "COL4 ARRAY<DOUBLE NOT NULL> NOT NULL, "
            + "COL5 MAP<VARCHAR NOT NULL, DOUBLE NOT NULL> NOT NULL"
            + "> NOT NULL"));
  }

  @Test
  public void shouldEscapeReservedWords() {
    // Given:
    final Schema structSchema = SchemaBuilder.struct()
        .field("COL1", Schema.STRING_SCHEMA)
        .field("COL2", SchemaBuilder
            .struct()
            .field("COL3", Schema.STRING_SCHEMA)
            .build())
        .build();

    final Predicate<String> escaper = name -> !name.equalsIgnoreCase("COL1");

    final SqlSchemaFormatter formatter = new SqlSchemaFormatter(escaper);

    // Then:
    assertThat(formatter.format(structSchema), is(
        "STRUCT<"
            + "COL1 VARCHAR, "
            + "`COL2` STRUCT<`COL3` VARCHAR>"
            + ">"));
  }

  @Test
  public void shouldFormatOptionalStruct() {
    // Given:
    final Schema structSchema = SchemaBuilder.struct()
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL4", SchemaBuilder
            .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build())
        .field("COL5", SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build())
        .optional()
        .build();

    // Then:
    assertThat(DEFAULT.format(structSchema), is(
        "STRUCT<"
            + "COL1 VARCHAR, "
            + "COL4 ARRAY<DOUBLE>, "
            + "COL5 MAP<VARCHAR, DOUBLE>"
            + ">"));

    assertThat(STRICT.format(structSchema), is(
        "STRUCT<"
            + "COL1 VARCHAR, "
            + "COL4 ARRAY<DOUBLE>, "
            + "COL5 MAP<VARCHAR, DOUBLE>"
            + ">"));
  }

  @Test
  public void shouldFormatOptionalStructAsColumns() {
    // Given:
    final Schema structSchema = SchemaBuilder.struct()
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL4", SchemaBuilder
            .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build())
        .field("COL5", SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build())
        .optional()
        .build();

    final SqlSchemaFormatter formatter = new SqlSchemaFormatter(
        DO_NOT_ESCAPE_COLUMN_NAMES,
        Option.AS_COLUMN_LIST,
        Option.APPEND_NOT_NULL
    );

    // When:
    final String result = formatter.format(structSchema);

    // Then:
    assertThat(result, is(
        "COL1 VARCHAR, "
            + "COL4 ARRAY<DOUBLE>, "
            + "COL5 MAP<VARCHAR, DOUBLE>"));
  }

  @Test
  public void shouldFormatRequiredStructAsColumns() {
    // Given:
    final Schema structSchema = SchemaBuilder.struct()
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL4", SchemaBuilder
            .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build())
        .field("COL5", SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build())
        .build();

    final SqlSchemaFormatter formatter = new SqlSchemaFormatter(
        DO_NOT_ESCAPE_COLUMN_NAMES,
        Option.AS_COLUMN_LIST,
        Option.APPEND_NOT_NULL
    );

    // When:
    final String result = formatter.format(structSchema);

    // Then:
    assertThat(result, is(
        "COL1 VARCHAR, "
            + "COL4 ARRAY<DOUBLE>, "
            + "COL5 MAP<VARCHAR, DOUBLE>"));
  }
}