package io.confluent.ksql.serde.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    final Object object = new ConnectSRSchemaDataTranslator(schema).toConnectRow(struct);

    // Then:
    assertThat(object, instanceOf(Struct.class));
    assertThat(((Struct) object).schema(), sameInstance(schema));
    assertThat(((Struct) object).get("f3"), is(nullValue()));
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
    final Object object = new ConnectSRSchemaDataTranslator(schema).toConnectRow(list);

    // Then:
    assertThat(object, sameInstance(list));
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
        () -> new ConnectSRSchemaDataTranslator(schema).toConnectRow(struct)
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
        () -> new ConnectSRSchemaDataTranslator(schema).toConnectRow(struct)
    );

    // Then:
    assertThat(e.getMessage(), is("Invalid value: null used for required field: \"f1\", "
        + "schema type: STRING"));
  }

  @Test
  public void shouldTransformStructWithDocMismatch() {
    // Given:
    final Schema sourceSchema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema targetSchema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.string().optional().doc("some doc").build())
        .build();
    final Struct source = new Struct(sourceSchema).put("f1", "hello");

    // When:
    final Object result = new ConnectSRSchemaDataTranslator(targetSchema).toConnectRow(source);

    // Then:
    assertThat(result, instanceOf(Struct.class));
    final Struct resultStruct = (Struct) result;
    assertThat(resultStruct.schema(), sameInstance(targetSchema));
    assertThat(resultStruct.get("f1"), is("hello"));
  }

  @Test
  public void shouldTransformNestedStructWithDocMismatch() {
    // Given:
    final Schema sourceInnerSchema = SchemaBuilder.struct()
        .field("val", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema targetInnerSchema = SchemaBuilder.struct()
        .doc("inner doc")
        .field("val", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema sourceSchema = SchemaBuilder.struct()
        .field("nested", sourceInnerSchema)
        .build();
    final Schema targetSchema = SchemaBuilder.struct()
        .field("nested", targetInnerSchema)
        .build();

    final Struct innerSource = new Struct(sourceInnerSchema).put("val", "hello");
    final Struct source = new Struct(sourceSchema).put("nested", innerSource);

    // When:
    final Object result = new ConnectSRSchemaDataTranslator(targetSchema).toConnectRow(source);

    // Then:
    assertThat(result, instanceOf(Struct.class));
    final Struct resultStruct = (Struct) result;
    assertThat(resultStruct.schema(), sameInstance(targetSchema));
    final Struct nested = (Struct) resultStruct.get("nested");
    assertThat(nested.get("val"), is("hello"));
  }

  @Test
  public void shouldTransformArrayOfStructsWithDocMismatch() {
    // Given:
    final Schema sourceItemSchema = SchemaBuilder.struct()
        .field("val", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema targetItemSchema = SchemaBuilder.struct()
        .doc("item doc")
        .field("val", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema sourceSchema = SchemaBuilder.struct()
        .field("items", SchemaBuilder.array(sourceItemSchema).optional().build())
        .build();
    final Schema targetSchema = SchemaBuilder.struct()
        .field("items", SchemaBuilder.array(targetItemSchema).optional().build())
        .build();

    final Struct itemSource = new Struct(sourceItemSchema).put("val", "a");
    final Struct source = new Struct(sourceSchema)
        .put("items", Collections.singletonList(itemSource));

    // When:
    final Object result = new ConnectSRSchemaDataTranslator(targetSchema).toConnectRow(source);

    // Then:
    assertThat(result, instanceOf(Struct.class));
    final Struct resultStruct = (Struct) result;
    @SuppressWarnings("unchecked")
    final List<Struct> items = (List<Struct>) resultStruct.get("items");
    assertThat(items.size(), is(1));
    assertThat(items.get(0).get("val"), is("a"));
  }

  @Test
  public void shouldTransformMapWithStructValueAndDocMismatch() {
    // Given:
    final Schema sourceValueSchema = SchemaBuilder.struct()
        .field("val", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema targetValueSchema = SchemaBuilder.struct()
        .doc("value doc")
        .field("val", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema sourceSchema = SchemaBuilder.struct()
        .field("data", SchemaBuilder.map(Schema.STRING_SCHEMA, sourceValueSchema).optional().build())
        .build();
    final Schema targetSchema = SchemaBuilder.struct()
        .field("data", SchemaBuilder.map(Schema.STRING_SCHEMA, targetValueSchema).optional().build())
        .build();

    final Struct valueSource = new Struct(sourceValueSchema).put("val", "hello");
    final Struct source = new Struct(sourceSchema)
        .put("data", Collections.singletonMap("key", valueSource));

    // When:
    final Object result = new ConnectSRSchemaDataTranslator(targetSchema).toConnectRow(source);

    // Then:
    assertThat(result, instanceOf(Struct.class));
    final Struct resultStruct = (Struct) result;
    @SuppressWarnings("unchecked")
    final Map<String, Struct> data = (Map<String, Struct>) resultStruct.get("data");
    assertThat(data.get("key").get("val"), is("hello"));
  }

  @Test
  public void shouldHandleExtraOptionalFieldInTargetSchema() {
    // Given:
    final Schema sourceSchema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema targetSchema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("f2", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct source = new Struct(sourceSchema).put("f1", "hello");

    // When:
    final Object result = new ConnectSRSchemaDataTranslator(targetSchema).toConnectRow(source);

    // Then:
    assertThat(result, instanceOf(Struct.class));
    final Struct resultStruct = (Struct) result;
    assertThat(resultStruct.schema(), sameInstance(targetSchema));
    assertThat(resultStruct.get("f1"), is("hello"));
    assertThat(resultStruct.get("f2"), is(nullValue()));
  }

  @Test
  public void shouldThrowForExtraRequiredFieldInTargetSchemaWithNoDefault() {
    // Given:
    final Schema sourceSchema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema targetSchema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("f2", SchemaBuilder.STRING_SCHEMA)
        .build();
    final Struct source = new Struct(sourceSchema).put("f1", "hello");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new ConnectSRSchemaDataTranslator(targetSchema).toConnectRow(source)
    );

    // Then:
    assertThat(e.getMessage(), is(
        "Missing default value for required field: [f2]."
            + " This field appears in JSON_SR schema in Schema Registry"));
  }
}
