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
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class EntityUtilTest {

  private static final FieldInfo ROWTIME_FIELD =
      new FieldInfo(
          SchemaUtil.ROWTIME_NAME,
          new SchemaInfo(SqlBaseType.BIGINT, null, null)
      );

  private static final FieldInfo ROWKEY_FIELD =
      new FieldInfo(
          SchemaUtil.ROWKEY_NAME,
          new SchemaInfo(SqlBaseType.STRING, null, null)
      );

  @Test
  public void shouldBuildCorrectIntegerField() {
    shouldBuildCorrectPrimitiveField(Schema.OPTIONAL_INT32_SCHEMA, "INTEGER");
  }

  @Test
  public void shouldBuildCorrectBigintField() {
    shouldBuildCorrectPrimitiveField(Schema.OPTIONAL_INT64_SCHEMA, "BIGINT");
  }

  @Test
  public void shouldBuildCorrectDoubleField() {
    shouldBuildCorrectPrimitiveField(Schema.OPTIONAL_FLOAT64_SCHEMA, "DOUBLE");
  }

  @Test
  public void shouldBuildCorrectStringField() {
    shouldBuildCorrectPrimitiveField(Schema.OPTIONAL_STRING_SCHEMA, "STRING");
  }

  @Test
  public void shouldBuildCorrectBooleanField() {
    shouldBuildCorrectPrimitiveField(Schema.OPTIONAL_BOOLEAN_SCHEMA, "BOOLEAN");
  }

  @Test
  public void shouldBuildCorrectMapField() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field("field", SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .build());

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema, true);

    // Then:
    assertThat(fields, hasSize(1));
    assertThat(fields.get(0).getName(), equalTo("field"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("MAP"));
    assertThat(fields.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(fields.get(0).getSchema().getMemberSchema().get().getTypeName(),
        equalTo("INTEGER"));
  }

  @Test
  public void shouldBuildCorrectArrayField() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field("field", SchemaBuilder
            .array(SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build())
        .build());

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema, true);

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
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field(
            "field",
            SchemaBuilder.
                struct()
                .field("innerField", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build())
        .build());

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema, true);

    // Then:
    assertThat(fields, hasSize(1));
    assertThat(fields.get(0).getName(), equalTo("field"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("STRUCT"));
    assertThat(fields.get(0).getSchema().getFields().get().size(), equalTo(1));
    final FieldInfo inner = fields.get(0).getSchema().getFields().get().get(0);
    assertThat(inner.getSchema().getTypeName(), equalTo("STRING"));
    assertThat(fields.get(0).getSchema().getMemberSchema(), equalTo(Optional.empty()));
  }

  @Test
  public void shouldBuildMiltipleFieldsCorrectly() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field("field1", Schema.OPTIONAL_INT32_SCHEMA)
        .field("field2", Schema.OPTIONAL_INT64_SCHEMA)
        .build());

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema, true);

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
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field("ROWKEY", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ROWTIME", Schema.OPTIONAL_INT32_SCHEMA)
        .field("field1", Schema.OPTIONAL_INT32_SCHEMA)
        .build());

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema, true);

    // Then:
    assertThat(fields, hasSize(3));
    assertThat(fields.get(0).getName(), equalTo("ROWKEY"));
    assertThat(fields.get(1).getName(), equalTo("ROWTIME"));
  }

  @Test
  public void shouldSupportGettingFullSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field("field1", Schema.OPTIONAL_INT32_SCHEMA)
        .build());

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema, false);

    // Then:
    assertThat(fields, hasSize(3));
    assertThat(fields.get(0).getName(), equalTo("ROWTIME"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo("BIGINT"));
    assertThat(fields.get(1).getName(), equalTo("ROWKEY"));
    assertThat(fields.get(1).getSchema().getTypeName(), equalTo("STRING"));
    assertThat(fields.get(2).getName(), equalTo("field1"));
  }

  private static void shouldBuildCorrectPrimitiveField(
      final Schema primitiveSchema,
      final String schemaName
  ) {
    // Given:
    final LogicalSchema schema = LogicalSchema.of(SchemaBuilder
        .struct()
        .field("field", primitiveSchema)
        .build());

    // When:
    final List<FieldInfo> fields = EntityUtil.buildSourceSchemaEntity(schema, true);

    // Then:
    assertThat(fields.get(0).getName(), equalTo("field"));
    assertThat(fields.get(0).getSchema().getTypeName(), equalTo(schemaName));
    assertThat(fields.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(fields.get(0).getSchema().getMemberSchema(), equalTo(Optional.empty()));
  }
}

