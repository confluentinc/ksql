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

package io.confluent.ksql.rest.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.FieldInfo.FieldType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class EntityUtilTest {

  @Test
  public void shouldBuildCorrectIntegerField() {
    shouldBuildCorrectPrimitiveField(SqlTypes.INTEGER, "INTEGER");
  }

  @Test
  public void shouldBuildCorrectBigintField() {
    shouldBuildCorrectPrimitiveField(SqlTypes.BIGINT, "BIGINT");
  }

  @Test
  public void shouldBuildCorrectDoubleField() {
    shouldBuildCorrectPrimitiveField(SqlTypes.DOUBLE, "DOUBLE");
  }

  @Test
  public void shouldBuildCorrectStringField() {
    shouldBuildCorrectPrimitiveField(SqlTypes.STRING, "STRING");
  }

  @Test
  public void shouldBuildCorrectBooleanField() {
    shouldBuildCorrectPrimitiveField(SqlTypes.BOOLEAN, "BOOLEAN");
  }

  @Test
  public void shouldBuildCorrectMapField() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field"), SqlTypes.map(SqlTypes.BIGINT, SqlTypes.INTEGER))
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(1));
    assertThat(fields.get(0).getName(), equalTo("field"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("MAP"));
    assertThat(fields.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(fields.get(0).getSchema().getMemberSchema().get().getTypeName(),
        equalTo("INTEGER"));
  }

  @Test
  public void shouldBuildCorrectDecimalField() {
    // Given:
    final SqlDecimal decimal  = SqlTypes.decimal(10, 9);
    final LogicalSchema schema = LogicalSchema.builder()
            .valueColumn(ColumnName.of("field"), decimal)
            .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(1));
    assertThat(fields.get(0).getName(), equalTo("field"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("DECIMAL"));
    assertThat(fields.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(fields.get(0).getSchema().getParameters().get(SqlDecimal.SCALE), equalTo(decimal.getScale()));
    assertThat(fields.get(0).getSchema().getParameters().get(SqlDecimal.PRECISION), equalTo(decimal.getPrecision()));
  }


  @Test
  public void shouldBuildCorrectArrayField() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field"), SqlTypes.array(SqlTypes.BIGINT))
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(1));
    assertThat(fields.get(0).getName(), equalTo("field"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("ARRAY"));
    assertThat(fields.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(fields.get(0).getSchema().getMemberSchema().get().getTypeName(),
        equalTo("BIGINT"));
  }

  @Test
  public void shouldBuildCorrectStructField() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field"), SqlTypes.struct()
            .field("innerField", SqlTypes.STRING)
            .build())
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(1));
    assertThat(fields.get(0).getName(), equalTo("field"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("STRUCT"));
    assertThat(fields.get(0).getSchema().getFields().get().size(), equalTo(1));
    final FieldInfo inner = fields.get(0).getSchema().getFields().get().get(0);
    assertThat(inner.getSchema().getTypeName(), equalTo("STRING"));
    assertThat(inner.getType(), equalTo(Optional.empty()));
    assertThat(fields.get(0).getSchema().getMemberSchema(), equalTo(Optional.empty()));
  }

  @Test
  public void shouldBuildMiltipleFieldsCorrectly() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("field2"), SqlTypes.BIGINT)
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(2));
    assertThat(fields.get(0).getName(), equalTo("field1"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("INTEGER"));
    assertThat(fields.get(1).getName(), equalTo("field2"));
    assertThat(fields.get(1).getSchema().getTypeName(), equalTo("BIGINT"));
  }

  @Test
  public void shouldSupportRowTimeAndKeyInValueSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("ROWTIME"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(3));
    assertThat(fields.get(0).getName(), equalTo("ROWKEY"));
    assertThat(fields.get(0).getType(), equalTo(Optional.empty()));
    assertThat(fields.get(1).getName(), equalTo("ROWTIME"));
    assertThat(fields.get(1).getType(), equalTo(Optional.empty()));
  }

  @Test
  public void shouldNotExposeMetaColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(1));
  }

  @Test
  public void shouldSupportSchemasWithKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(1));
    assertThat(fields.get(0).getName(), equalTo("field1"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("INTEGER"));
    assertThat(fields.get(0).getType(), equalTo(Optional.of(FieldType.KEY)));
  }

  @Test
  public void shouldSupportSchemasWithAllHeadersColumn() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .headerColumn(ColumnName.of("field1"), Optional.empty())
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(1));
    assertThat(fields.get(0).getName(), equalTo("field1"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("ARRAY"));
    assertThat(fields.get(0).getType(), equalTo(Optional.of(FieldType.HEADER)));
    assertThat(fields.get(0).getHeaderKey(), equalTo(Optional.empty()));
  }

  @Test
  public void shouldSupportSchemasWithExtractedHeaderColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .headerColumn(ColumnName.of("field1"), Optional.of("abc"))
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(1));
    assertThat(fields.get(0).getName(), equalTo("field1"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("BYTES"));
    assertThat(fields.get(0).getType(), equalTo(Optional.of(FieldType.HEADER)));
    assertThat(fields.get(0).getHeaderKey(), equalTo(Optional.of("abc")));
  }


  @Test
  public void shouldMaintainColumnOrder() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field0"), SqlTypes.DOUBLE)
        .keyColumn(ColumnName.of("field1"), SqlTypes.INTEGER)
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields, hasSize(2));
    assertThat(fields.get(0).getName(), equalTo("field0"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("DOUBLE"));
    assertThat(fields.get(1).getName(), equalTo("field1"));
    assertThat(fields.get(1).getSchema().getTypeName(), equalTo("INTEGER"));
  }

  private static void shouldBuildCorrectPrimitiveField(
      final SqlType primitiveSchema,
      final String schemaName
  ) {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field"), primitiveSchema)
        .build();

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(fields.get(0).getName(), equalTo("field"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo(schemaName));
    assertThat(fields.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(fields.get(0).getSchema().getMemberSchema(), equalTo(Optional.empty()));
    assertThat(fields.get(0).getType(), equalTo(Optional.empty()));
  }
}

