package io.confluent.ksql.serde.avro;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class AvroDataTranslatorTest {
  @Test
  public void shoudRenameSourceDereference() {
    final Schema schema = SchemaBuilder.struct()
        .field("STREAM_NAME.COLUMN_NAME", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final AvroDataTranslator dataTranslator = new AvroDataTranslator(schema);
    final GenericRow ksqlRow = new GenericRow(ImmutableList.of(123));
    final Struct struct = dataTranslator.toConnectRow(ksqlRow);

    assertThat(
        struct.schema(),
        equalTo(
            SchemaBuilder.struct()
                .name(struct.schema().name())
                .field("STREAM_NAME_COLUMN_NAME", Schema.OPTIONAL_INT32_SCHEMA)
                .optional()
                .build()
        )
    );
    assertThat(struct.get("STREAM_NAME_COLUMN_NAME"), equalTo(123));

    final GenericRow translatedRow = dataTranslator.toKsqlRow(struct.schema(), struct);
    assertThat(translatedRow, equalTo(ksqlRow));
  }

  @Test
  public void shouldAddNamesToSchema() {
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

    final AvroDataTranslator dataTranslator = new AvroDataTranslator(schema);
    final GenericRow ksqlRow = new GenericRow(
        ImmutableList.of(arrayInnerStruct),
        ImmutableMap.of("bar", mapInnerStruct),
        structInnerStruct
    );
    final Struct struct = dataTranslator.toConnectRow(ksqlRow);
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

    final GenericRow translatedRow = dataTranslator.toKsqlRow(struct.schema(), struct);
    assertThat(translatedRow, equalTo(ksqlRow));
  }

  @Test
  public void shouldReplaceNullWithNull() {
    final Schema schema = SchemaBuilder.struct()
        .field(
            "COLUMN_NAME",
            SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build())
        .optional()
        .build();

    final AvroDataTranslator dataTranslator = new AvroDataTranslator(schema);
    final GenericRow ksqlRow = new GenericRow(Collections.singletonList(null));
    final Struct struct = dataTranslator.toConnectRow(ksqlRow);

    assertThat(struct.get("COLUMN_NAME"), nullValue());

    final GenericRow translatedRow = dataTranslator.toKsqlRow(struct.schema(), struct);
    assertThat(translatedRow, equalTo(ksqlRow));
  }

  @Test
  public void shoudlReplacePrimitivesCorrectly() {
    final Schema schema = SchemaBuilder.struct()
        .field("COLUMN_NAME", Schema.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();

    final AvroDataTranslator dataTranslator = new AvroDataTranslator(schema);
    final GenericRow ksqlRow = new GenericRow(Collections.singletonList(123L));
    final Struct struct = dataTranslator.toConnectRow(ksqlRow);

    assertThat(struct.get("COLUMN_NAME"), equalTo(123L));

    final GenericRow translatedRow = dataTranslator.toKsqlRow(struct.schema(), struct);
    assertThat(translatedRow, equalTo(ksqlRow));
  }
}
