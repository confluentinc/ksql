package io.confluent.ksql.serde.avro;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

public class AvroSRSchemaDataTranslatorTest {

  private static final Schema ORIGINAL_SCHEMA = SchemaBuilder.struct()
      .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .field("f2", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
      .build();

  @Test
  public void shouldTransformStruct() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("f2", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("f3", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .build();
    final Struct struct = new Struct(ORIGINAL_SCHEMA)
        .put("f1", "abc")
        .put("f2", 12);

    // When:
    final Object object = new AvroSRSchemaDataTranslator(schema).toConnectRow(struct);

    // Then:
    assertThat(object, instanceOf(Struct.class));
    assertThat(((Struct) object).schema(), sameInstance(schema));
    assertThat(((Struct) object).get("f3"), is(nullValue()));
  }

  @Test
  public void shouldTransformStructWithDefaultValue() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("f2", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("f3", SchemaBuilder.int64().defaultValue(123L))
        .build();
    final Struct struct = new Struct(ORIGINAL_SCHEMA)
        .put("f1", "abc")
        .put("f2", 12);

    // When:
    final Object object = new AvroSRSchemaDataTranslator(schema).toConnectRow(struct);

    // Then:
    assertThat(object, instanceOf(Struct.class));
    assertThat(((Struct) object).schema(), sameInstance(schema));
    assertThat(((Struct) object).get("f3"), is(123L));
  }

  @Test
  public void shouldNotTransformOtherType() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("f2", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("f3", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .build();
    final List<Integer> list = Collections.emptyList();

    // When:
    final Object object = new AvroSRSchemaDataTranslator(schema).toConnectRow(list);

    // Then:
    assertThat(object, sameInstance(list));
  }

  @Test
  public void shouldThrowIfExtraFieldNotOptionalOrDefault() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("f2", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("f3", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("f4", SchemaBuilder.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(ORIGINAL_SCHEMA)
        .put("f1", "abc")
        .put("f2", 12);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new AvroSRSchemaDataTranslator(schema).toConnectRow(struct)
    );

    // Then:
    assertThat(e.getMessage(), is("Missing default value for required Avro field: [f4]. "
        + "This field appears in Avro schema in Schema Registry"));
  }

  @Test
  public void shouldThrowIfMissingField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("f3", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .build();
    final Struct struct = new Struct(ORIGINAL_SCHEMA)
        .put("f1", "abc")
        .put("f2", 12);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new AvroSRSchemaDataTranslator(schema).toConnectRow(struct)
    );

    // Then:
    assertThat(e.getMessage(), is("Schema from Schema Registry misses field with name: f2"));
  }

  @Test
  public void shouldThrowIfConvertInvalidValue() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.STRING_SCHEMA)
        .field("f2", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("f3", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .build();
    final Struct struct = new Struct(ORIGINAL_SCHEMA)
        .put("f1", null)
        .put("f2", 12);

    // When:
    final Exception e = assertThrows(
        DataException.class,
        () -> new AvroSRSchemaDataTranslator(schema).toConnectRow(struct)
    );

    // Then:
    assertThat(e.getMessage(), is("Invalid value: null used for required field: \"f1\", "
        + "schema type: STRING"));
  }

  @Test
  public void shouldTransformStructWithNestedStructs() {
    // Given:
    final Schema innerStructSchemaWithoutOptional = getInnerStructSchema(false);
    final Schema innerStructSchemaWithOptional = getInnerStructSchema(true);

    Struct innerInnerStructWithOptional = getNestedData(innerStructSchemaWithOptional);
    Struct innerInnerStructWithoutOptional = getNestedData(innerStructSchemaWithoutOptional);

    final Schema structSchemaInnerWithOptional = getStructSchemaWithNestedStruct(innerStructSchemaWithOptional, true);
    final Schema structSchemaInnerWithOutOptional = getStructSchemaWithNestedStruct(innerStructSchemaWithoutOptional, false);

    // Physical Schema retrieved from SR
    final Schema schema = SchemaBuilder.struct()
        .field("string_field", SchemaBuilder.STRING_SCHEMA)
        .field("struct_field", structSchemaInnerWithOutOptional)
        .build();

    // Logical Schema created by Ksql
    final Schema ORIGINAL_SCHEMA = SchemaBuilder.struct()
        .field("string_field", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("struct_field", structSchemaInnerWithOptional)
        .optional()
        .build();

    Struct innerStructWithoutOptional = getInnerStructData(structSchemaInnerWithOutOptional, innerInnerStructWithoutOptional);
    Struct innerStructWithOptional = getInnerStructData(structSchemaInnerWithOptional, innerInnerStructWithOptional);

    final Struct struct = new Struct(ORIGINAL_SCHEMA)
        .put("string_field", "abc")
        .put("struct_field", innerStructWithOptional);

    // When:
    final Object object = new AvroSRSchemaDataTranslator(schema).toConnectRow(struct);

    // Then:
    assertThat(object, instanceOf(Struct.class));
    assertThat(((Struct) object).schema(), sameInstance(schema));
    assertThat(((Struct) object).get("string_field"), is("abc"));
    assertThat(((Struct) object).get("struct_field"), equalTo(innerStructWithoutOptional));
  }

  @Test
  public void shouldTransformStructWithArrayOfStructs() {
    // Given:
    final Schema innerStructSchemaWithoutOptional = getInnerStructSchema(false);
    final Schema innerStructSchemaWithOptional = getInnerStructSchema(true);

    Struct innerInnerStructWithOptional = getNestedData(innerStructSchemaWithOptional);
    Struct innerInnerStructWithoutOptional = getNestedData(innerStructSchemaWithoutOptional);

    final Schema structSchemaInnerWithOptional = getStructSchemaWithNestedStruct(innerStructSchemaWithOptional, true);
    final Schema structSchemaInnerWithOutOptional = getStructSchemaWithNestedStruct(innerStructSchemaWithoutOptional, false);

    final Schema arraySchemaWithoutOptional = getArraySchema(false);
    final Schema arraySchemaWithOptional = getArraySchema(true);

    // Physical Schema retrieved from SR
    final Schema schema = SchemaBuilder.struct()
        .field("string_field", SchemaBuilder.STRING_SCHEMA)
        .field("array_field", SchemaBuilder.array(arraySchemaWithoutOptional))
        .field("struct_field", structSchemaInnerWithOutOptional)
        .build();

    // Logical Schema created by Ksql
    final Schema ORIGINAL_SCHEMA = SchemaBuilder.struct()
        .field("string_field", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("array_field", SchemaBuilder.array(arraySchemaWithOptional))
        .field("struct_field", structSchemaInnerWithOptional)
        .optional()
        .build();

    List<Struct> arrayListWithOptional = getArrayData(arraySchemaWithOptional);
    List<Struct> arrayListWithoutOptional = getArrayData(arraySchemaWithoutOptional);

    Struct innerStructWithoutOptional = getInnerStructData(structSchemaInnerWithOutOptional, innerInnerStructWithoutOptional);
    Struct innerStructWithOptional = getInnerStructData(structSchemaInnerWithOptional, innerInnerStructWithOptional);

    final Struct struct = new Struct(ORIGINAL_SCHEMA)
        .put("string_field", "abc")
        .put("array_field", arrayListWithOptional)
        .put("struct_field", innerStructWithOptional);

    // When:
    final Object object = new AvroSRSchemaDataTranslator(schema).toConnectRow(struct);

    // Then:
    assertThat(object, instanceOf(Struct.class));
    assertThat(((Struct) object).schema(), sameInstance(schema));
    assertThat(((Struct) object).get("string_field"), is("abc"));
    assertThat(((Struct) object).get("array_field"), equalTo(arrayListWithoutOptional));
    assertThat(((Struct) object).get("struct_field"), equalTo(innerStructWithoutOptional));
  }

  @Test
  public void shouldTransformStructWithMapOfStructs() {
    // Given:
    final Schema innerStructSchemaWithoutOptional = getInnerStructSchema(false);
    final Schema innerStructSchemaWithOptional = getInnerStructSchema(true);

    // Physical Schema retrieved from SR
    final Schema schema = SchemaBuilder.struct()
        .field("string_field", SchemaBuilder.STRING_SCHEMA)
        .field("map_field", SchemaBuilder.map(Schema.STRING_SCHEMA, innerStructSchemaWithoutOptional))
        .build();

    // Logical Schema created by Ksql
    final Schema ORIGINAL_SCHEMA = SchemaBuilder.struct()
        .field("string_field", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("map_field", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, innerStructSchemaWithOptional))
        .optional()
        .build();

    final Struct struct = new Struct(ORIGINAL_SCHEMA)
        .put("string_field", "abc")
        .put("map_field", ImmutableMap.of(
            "key1", getNestedData(innerStructSchemaWithOptional),
            "key2", getNestedData(innerStructSchemaWithOptional)));

    final Map<String, Object> mapWithoutOptional = ImmutableMap.of(
        "key1", getNestedData(innerStructSchemaWithoutOptional),
        "key2", getNestedData(innerStructSchemaWithoutOptional));

    // When:
    final Object object = new AvroSRSchemaDataTranslator(schema).toConnectRow(struct);

    // Then:
    assertThat(object, instanceOf(Struct.class));
    assertThat(((Struct) object).schema(), sameInstance(schema));
    assertThat(((Struct) object).get("string_field"), is("abc"));
    assertThat(((Struct) object).get("map_field"), equalTo(mapWithoutOptional));
  }

  private Schema getInnerStructSchema(boolean optional) {
    SchemaBuilder builder = SchemaBuilder.struct()
        .field("ss1", optional ? SchemaBuilder.OPTIONAL_STRING_SCHEMA : SchemaBuilder.STRING_SCHEMA)
        .field("ss2", optional ? SchemaBuilder.OPTIONAL_INT64_SCHEMA : SchemaBuilder.INT64_SCHEMA);

    if (optional) {
      builder.optional();
    }

    return builder.build();
  }

  private Schema getStructSchemaWithNestedStruct(Schema innerStructSchema, boolean optional) {
    SchemaBuilder builder = SchemaBuilder.struct()
        .field("s1", optional ? SchemaBuilder.OPTIONAL_STRING_SCHEMA : SchemaBuilder.STRING_SCHEMA)
        .field("s2", optional ? SchemaBuilder.OPTIONAL_INT64_SCHEMA : SchemaBuilder.INT64_SCHEMA)
        .field("s3", innerStructSchema.schema());

    if (optional) {
      builder.optional();
    }

    return builder.build();
  }

  private Schema getArraySchema(boolean optional) {
    SchemaBuilder builder = SchemaBuilder.struct()
        .field("a1", optional ? SchemaBuilder.OPTIONAL_STRING_SCHEMA : SchemaBuilder.STRING_SCHEMA)
        .field("a2", optional ? SchemaBuilder.OPTIONAL_INT64_SCHEMA : SchemaBuilder.INT64_SCHEMA);

    if (optional) {
      builder.optional();
    }

    return builder.build();
  }

  private Struct getNestedData(Schema schema) {
    return new Struct(schema)
        .put("ss1", "ss1")
        .put("ss2", 54444L);
  }

  private Struct getInnerStructData(Schema innerSchema, Struct nestedSchema) {
    return new Struct(innerSchema)
        .put("s1", "s1")
        .put("s2", 64L)
        .put("s3", nestedSchema);
  }

  private List<Struct> getArrayData(Schema schema) {
    List<Struct> arrayData = new ArrayList<>();
    arrayData.add(new Struct(schema)
        .put("a1", "array_val1")
        .put("a2", 23L));

    return arrayData;
  }

}