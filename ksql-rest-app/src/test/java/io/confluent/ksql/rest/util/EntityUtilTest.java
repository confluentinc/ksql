package io.confluent.ksql.rest.util;

import io.confluent.ksql.rest.entity.FieldInfo;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class EntityUtilTest {
  private void shouldBuildCorrectPrimitiveField(Schema primitiveSchema, String schemaName) {
    Schema schema = SchemaBuilder
        .struct()
        .field("field", primitiveSchema)
        .build();

    List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getType(), equalTo(schemaName));
    assertThat(entity.get(0).getSchema().getFields(), equalTo(null));
    assertThat(entity.get(0).getSchema().getMemberSchema(), equalTo(null));
  }

  @Test
  public void shouldBuildCorrectIntegerField() {
    shouldBuildCorrectPrimitiveField(Schema.INT32_SCHEMA, "INTEGER");
  }

  @Test
  public void shouldBuildCorrectBigintField() {
    shouldBuildCorrectPrimitiveField(Schema.INT64_SCHEMA, "BIGINT");
  }

  @Test
  public void shouldBuildCorrectDoubleField() {
    shouldBuildCorrectPrimitiveField(Schema.FLOAT64_SCHEMA, "DOUBLE");
  }

  @Test
  public void shouldBuildCorrectStringField() {
    shouldBuildCorrectPrimitiveField(Schema.STRING_SCHEMA, "VARCHAR(STRING)");
  }

  @Test
  public void shouldBuildCorrectBooleanField() {
    shouldBuildCorrectPrimitiveField(Schema.BOOLEAN_SCHEMA, "BOOLEAN");
  }

  @Test
  public void shouldBuildCorrectMapField() {
    Schema schema = SchemaBuilder
        .struct()
        .field("field", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
        .build();

    List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getType(), equalTo("MAP"));
    assertThat(entity.get(0).getSchema().getFields(), equalTo(null));
    assertThat(entity.get(0).getSchema().getMemberSchema().getType(), equalTo("INTEGER"));
  }

  @Test
  public void shouldBuildCorrectArrayField() {
    Schema schema = SchemaBuilder
        .struct()
        .field("field", SchemaBuilder.array(SchemaBuilder.INT64_SCHEMA))
        .build();

    List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getType(), equalTo("ARRAY"));
    assertThat(entity.get(0).getSchema().getFields(), equalTo(null));
    assertThat(entity.get(0).getSchema().getMemberSchema().getType(), equalTo("BIGINT"));
  }

  @Test
  public void shouldBuildCorrectStructField() {
    Schema schema = SchemaBuilder
        .struct()
        .field(
            "field",
            SchemaBuilder.
                struct()
                .field("innerField", Schema.STRING_SCHEMA)
                .build())
        .build();


    List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(1));
    assertThat(entity.get(0).getName(), equalTo("field"));
    assertThat(entity.get(0).getSchema().getType(), equalTo("STRUCT"));
    assertThat(entity.get(0).getSchema().getFields().size(), equalTo(1));
    FieldInfo inner = entity.get(0).getSchema().getFields().get(0);
    assertThat(inner.getSchema().getType(), equalTo("VARCHAR(STRING)"));
    assertThat(entity.get(0).getSchema().getMemberSchema(), equalTo(null));
  }

  @Test
  public void shouldBuildMiltipleFieldsCorrectly() {
    Schema schema = SchemaBuilder
        .struct()
        .field("field1", Schema.INT32_SCHEMA)
        .field("field2", Schema.INT64_SCHEMA)
        .build();


    List<FieldInfo> entity = EntityUtil.buildSourceSchemaEntity(schema);

    assertThat(entity.size(), equalTo(2));
    assertThat(entity.get(0).getName(), equalTo("field1"));
    assertThat(entity.get(0).getSchema().getType(), equalTo("INTEGER"));
    assertThat(entity.get(1).getName(), equalTo("field2"));
    assertThat(entity.get(1).getSchema().getType(), equalTo("BIGINT"));
  }
}
