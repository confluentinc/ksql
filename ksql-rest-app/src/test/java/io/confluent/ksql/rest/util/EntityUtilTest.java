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
import static org.junit.Assert.assertThat;

import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class EntityUtilTest {
  private static void shouldBuildCorrectPrimitiveField(
      final Schema primitiveSchema,
      final String schemaName
  ) {
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder
        .struct()
        .field("field", primitiveSchema)
        .build());

    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getTypeName(), equalTo(schemaName));
    assertThat(entity.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(entity.get(0).getSchema().getMemberSchema(), equalTo(Optional.empty()));
  }

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
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder
        .struct()
        .field("field", SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .build());

    // When:
    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getTypeName(), equalTo("MAP"));
    assertThat(entity.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(entity.get(0).getSchema().getMemberSchema().get().getTypeName(), equalTo("INTEGER"));
  }

  @Test
  public void shouldBuildCorrectArrayField() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder
        .struct()
        .field("field", SchemaBuilder
            .array(SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build())
        .build());

    // When:
    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getTypeName(), equalTo("ARRAY"));
    assertThat(entity.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(entity.get(0).getSchema().getMemberSchema().get().getTypeName(), equalTo("BIGINT"));
  }

  @Test
  public void shouldBuildCorrectStructField() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder
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
    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getTypeName(), equalTo("STRUCT"));
    assertThat(entity.get(0).getSchema().getFields().get().size(), equalTo(1));
    final FieldInfo inner = entity.get(0).getSchema().getFields().get().get(0);
    assertThat(inner.getSchema().getTypeName(), equalTo("STRING"));
    assertThat(entity.get(0).getSchema().getMemberSchema(), equalTo(Optional.empty()));
  }

  @Test
  public void shouldBuildMiltipleFieldsCorrectly() {
    // Given:
    final KsqlSchema schema = KsqlSchema.of(SchemaBuilder
        .struct()
        .field("field1", Schema.OPTIONAL_INT32_SCHEMA)
        .field("field2", Schema.OPTIONAL_INT64_SCHEMA)
        .build());

    // When:
    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    // Then:
    assertThat(entity.size(), equalTo(2));
    assertThat(entity.get(0).getName(), equalTo("field1"));
    assertThat(entity.get(0).getSchema().getTypeName(), equalTo("INTEGER"));
    assertThat(entity.get(1).getName(), equalTo("field2"));
    assertThat(entity.get(1).getSchema().getTypeName(), equalTo("BIGINT"));
  }
}
