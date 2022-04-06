package io.confluent.ksql.serde.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.json.JsonFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

public class ConnectSRSchemaDataTranslatorTest {

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
    final Object object =
        new ConnectSRSchemaDataTranslator(schema, JsonFormat.NAME).toConnectRow(struct);

    // Then:
    assertThat(object, instanceOf(Struct.class));
    assertThat(((Struct) object).schema(), sameInstance(schema));
    assertThat(((Struct) object).get("f3"), is(nullValue()));
  }

  @Test
  public void shouldTransformStructWithAvroDefaultValue() {
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
    final Object object =
        new ConnectSRSchemaDataTranslator(schema, AvroFormat.NAME).toConnectRow(struct);

    // Then:
    assertThat(object, instanceOf(Struct.class));
    assertThat(((Struct) object).schema(), sameInstance(schema));
    assertThat(((Struct) object).get("f3"), is(123L));
  }

  @Test
  public void shouldTransformStructWithProtobufDefaultValue() {
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
    final Object object =
        new ConnectSRSchemaDataTranslator(schema, ProtobufFormat.NAME).toConnectRow(struct);

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
    final Object object =
        new ConnectSRSchemaDataTranslator(schema, JsonFormat.NAME).toConnectRow(list);

    // Then:
    assertThat(object, sameInstance(list));
  }

  @Test
  public void shouldThrowIfExtraAvroFieldNotOptionalOrDefault() {
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
        () -> new ConnectSRSchemaDataTranslator(schema, AvroFormat.NAME).toConnectRow(struct)

    );

    // Then:
    assertThat(e.getMessage(), is("Missing default value for required field: [f4]. "
        + "This field appears in the schema in Schema Registry"));
  }

  @Test
  public void shouldThrowIfExtraProtobufFieldNotOptionalOrDefault() {
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
        () -> new ConnectSRSchemaDataTranslator(schema, ProtobufFormat.NAME).toConnectRow(struct)

    );

    // Then:
    assertThat(e.getMessage(), is("Missing default value for required field: [f4]. "
        + "This field appears in the schema in Schema Registry"));
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
        () -> new ConnectSRSchemaDataTranslator(schema, JsonFormat.NAME).toConnectRow(struct)
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
        () -> new ConnectSRSchemaDataTranslator(schema, JsonFormat.NAME).toConnectRow(struct)
    );

    // Then:
    assertThat(e.getMessage(), is("Invalid value: null used for required field: \"f1\", "
        + "schema type: STRING"));
  }
}