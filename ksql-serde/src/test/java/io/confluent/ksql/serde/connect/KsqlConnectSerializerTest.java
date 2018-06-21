package io.confluent.ksql.serde.connect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.easymock.Capture;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.capture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;


public class KsqlConnectSerializerTest {
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

    final Capture<Schema> capturedSchema = Capture.newInstance();
    final Capture<Struct> capturedRow = Capture.newInstance();
    final Converter converter = mock(Converter.class);
    expect(
        converter.fromConnectData(eq("topic"), capture(capturedSchema), capture(capturedRow)))
        .andReturn(new byte[32]);
    replay(converter);

    final KsqlConnectSerializer serializer = new KsqlConnectSerializer(schema, converter);

    serializer.serialize("topic", new GenericRow(
        ImmutableList.of(arrayInnerStruct),
        ImmutableMap.of("bar", mapInnerStruct),
        structInnerStruct));

    final Schema namedSchema = capturedSchema.getValue();
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
    assertThat(namedArrayInner.name(), notNullValue());

    final Schema namedMapInner = namedSchema.field("MAP").schema().valueSchema();
    assertThat(namedMapInner.type(), equalTo(Schema.Type.STRUCT));
    assertThat(namedMapInner.fields().size(), equalTo(1));
    assertThat(
        namedMapInner.field("MAP_INNER").schema(),
        equalTo(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(namedMapInner.name(), notNullValue());

    final Schema namedStructInner = namedSchema.field("MAP").schema().valueSchema();
    assertThat(namedStructInner.type(), equalTo(Schema.Type.STRUCT));
    assertThat(namedStructInner.fields().size(), equalTo(1));
    assertThat(
        namedStructInner.field("MAP_INNER").schema(),
        equalTo(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(namedStructInner.name(), notNullValue());

    final Struct struct = capturedRow.getValue();
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
  }
}
