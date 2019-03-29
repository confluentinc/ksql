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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.confluent.ksql.util.DecimalUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class EntityUtilTest {
  private void shouldBuildCorrectPrimitiveField(final Schema primitiveSchema,
                                                final String schemaName,
                                                final List<String> schemaParameters) {
    final Schema schema = SchemaBuilder
        .struct()
        .field("field", primitiveSchema)
        .build();

    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getTypeName(), equalTo(schemaName));
    assertThat(entity.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(entity.get(0).getSchema().getMemberSchema(), equalTo(Optional.empty()));
    assertThat(entity.get(0).getSchema().getTypeParameters(),
        equalTo(Optional.ofNullable(schemaParameters)));
  }

  @Test
  public void shouldBuildCorrectIntegerField() {
    shouldBuildCorrectPrimitiveField(Schema.INT32_SCHEMA, "INTEGER", null);
  }

  @Test
  public void shouldBuildCorrectBigintField() {
    shouldBuildCorrectPrimitiveField(Schema.INT64_SCHEMA, "BIGINT", null);
  }

  @Test
  public void shouldBuildCorrectDoubleField() {
    shouldBuildCorrectPrimitiveField(Schema.FLOAT64_SCHEMA, "DOUBLE", null);
  }

  @Test
  public void shouldBuildCorrectStringField() {
    shouldBuildCorrectPrimitiveField(Schema.STRING_SCHEMA, "STRING", null);
  }

  @Test
  public void shouldBuildCorrectBooleanField() {
    shouldBuildCorrectPrimitiveField(Schema.BOOLEAN_SCHEMA, "BOOLEAN", null);
  }

  @Test
  public void shouldBuildCorrectDecimalField() {
    final int precision = 6;
    final int scale = 2;
    shouldBuildCorrectPrimitiveField(DecimalUtil.schema(precision, scale), "DECIMAL",
        Arrays.asList(Integer.toString(precision), Integer.toString(scale)));
  }

  @Test
  public void shouldBuildCorrectMapField() {
    final Schema schema = SchemaBuilder
        .struct()
        .field("field", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
        .build();

    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getTypeName(), equalTo("MAP"));
    assertThat(entity.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(entity.get(0).getSchema().getMemberSchema().get().getTypeName(), equalTo("INTEGER"));
  }

  @Test
  public void shouldBuildCorrectArrayField() {
    final Schema schema = SchemaBuilder
        .struct()
        .field("field", SchemaBuilder.array(SchemaBuilder.INT64_SCHEMA))
        .build();

    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getTypeName(), equalTo("ARRAY"));
    assertThat(entity.get(0).getSchema().getFields(), equalTo(Optional.empty()));
    assertThat(entity.get(0).getSchema().getMemberSchema().get().getTypeName(), equalTo("BIGINT"));
  }

  @Test
  public void shouldBuildCorrectStructField() {
    final Schema schema = SchemaBuilder
        .struct()
        .field(
            "field",
            SchemaBuilder.
                struct()
                .field("innerField", Schema.STRING_SCHEMA)
                .build())
        .build();


    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

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
    final Schema schema = SchemaBuilder
        .struct()
        .field("field1", Schema.INT32_SCHEMA)
        .field("field2", Schema.INT64_SCHEMA)
        .build();


    final List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(2));
    assertThat(entity.get(0).getName(), equalTo("field1"));
    assertThat(entity.get(0).getSchema().getTypeName(), equalTo("INTEGER"));
    assertThat(entity.get(1).getName(), equalTo("field2"));
    assertThat(entity.get(1).getSchema().getTypeName(), equalTo("BIGINT"));
  }
}
