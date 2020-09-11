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

package io.confluent.ksql.serde.json;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class KsqlJsonSerializerTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String SOME_TOPIC = "bob";

  private static final String ORDERTIME = "ORDERTIME";
  private static final String ORDERID = "ORDERID";
  private static final String ITEMID = "ITEMID";
  private static final String ORDERUNITS = "ORDERUNITS";
  private static final String ARRAYCOL = "ARRAYCOL";
  private static final String MAPCOL = "MAPCOL";
  private static final String DECIMALCOL = "DECIMALCOL";

  private static final Schema ORDER_SCHEMA = SchemaBuilder.struct()
      .field(ORDERTIME, Schema.OPTIONAL_INT64_SCHEMA)
      .field(ORDERID, Schema.OPTIONAL_INT64_SCHEMA)
      .field(ITEMID, Schema.OPTIONAL_STRING_SCHEMA)
      .field(ORDERUNITS, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field(ARRAYCOL, SchemaBuilder
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field(MAPCOL, SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field(DECIMALCOL, Decimal.builder(5).optional().build())
      .build();

  private static final Schema ADDRESS_SCHEMA = SchemaBuilder.struct()
      .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
      .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
      .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
      .optional()
      .build();

  private static final Schema CATEGORY_SCHEMA = SchemaBuilder.struct()
      .field("ID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build();

  private static final Schema ITEM_SCHEMA = SchemaBuilder.struct()
      .field(ITEMID, Schema.OPTIONAL_INT64_SCHEMA)
      .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CATEGORIES", SchemaBuilder.array(CATEGORY_SCHEMA).optional().build())
      .optional()
      .build();

  private static final Schema SCHEMA_WITH_STRUCT = SchemaBuilder.struct()
      .field("ordertime", Schema.OPTIONAL_INT64_SCHEMA)
      .field("orderid", Schema.OPTIONAL_INT64_SCHEMA)
      .field("itemid", ITEM_SCHEMA)
      .field("orderunits", Schema.OPTIONAL_INT32_SCHEMA)
      .field("arraycol", SchemaBuilder
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field("mapcol", SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field("address", ADDRESS_SCHEMA)
      .build();

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{"Plain JSON", false}, {"Magic byte prefixed", true}});
  }

  @Parameter
  public String suiteName;

  @Parameter(1)
  public boolean useSchemas;

  private KsqlConfig config;
  private SchemaRegistryClient srClient;
  private Serializer<Object> serializer;

  @Before
  public void before() {
    config = new KsqlConfig(ImmutableMap.of());
    srClient = new MockSchemaRegistryClient();
    givenSerializerForSchema(ORDER_SCHEMA);
  }

  @Test
  public void shouldSerializeNullValue() {
    // Given:
    givenSerializerForSchema(ORDER_SCHEMA);

    // When:
    final byte[] serializedRow = serializer.serialize(SOME_TOPIC, null);

    // Then:
    assertThat(serializedRow, is(CoreMatchers.nullValue()));
  }

  @Test
  public void shouldSerializeStructCorrectly() {
    // Given:
    final Struct struct = new Struct(ORDER_SCHEMA)
        .put(ORDERTIME, 1511897796092L)
        .put(ORDERID, 1L)
        .put(ITEMID, "item_1")
        .put(ORDERUNITS, 10.0)
        .put(ARRAYCOL, Collections.singletonList(100.0))
        .put(MAPCOL, Collections.singletonMap("key1", 100.0))
        .put(DECIMALCOL, new BigDecimal("1.12345"));

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, struct);

    // Then:
    final String mapCol = useSchemas
        ? "[{\"key\":\"key1\",\"value\":100.0}]"
        : "{\"key1\":100.0}";

    assertThat(asJsonString(bytes), equalTo(
        "{"
            + "\"ORDERTIME\":1511897796092,"
            + "\"ORDERID\":1,"
            + "\"ITEMID\":\"item_1\","
            + "\"ORDERUNITS\":10.0,"
            + "\"ARRAYCOL\":[100.0],"
            + "\"MAPCOL\":" + mapCol + ","
            + "\"DECIMALCOL\":1.12345"
            + "}"));
  }

  @Test
  public void shouldThrowIfNotStruct() {
    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, 10)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Invalid type for STRUCT: class java.lang.Integer"))));
  }

  @Test
  public void shouldHandleNestedStruct() throws IOException {
    // Given:
    givenSerializerForSchema(SCHEMA_WITH_STRUCT);
    final Struct struct = buildWithNestedStruct();

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, struct);

    // Then:
    final JsonNode jsonNode = useSchemas
        ? JsonSerdeUtils.readJsonSR(bytes, OBJECT_MAPPER, JsonNode.class)
        : OBJECT_MAPPER.readTree(bytes);

    assertThat(jsonNode.size(), equalTo(7));
    assertThat(jsonNode.get("ordertime").asLong(), is(1234567L));
    assertThat(jsonNode.get("itemid").get("NAME").asText(), is("Item_10"));
  }

  @Test
  public void shouldSerializeBoolean() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, true);

    // Then:
    assertThat(asJsonString(bytes), is("true"));
  }

  @Test
  public void shouldThrowIfNotBoolean() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, 10)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for BOOLEAN: class java.lang.Integer"))));
  }

  @Test
  public void shouldSerializeInt() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT32_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, 62);

    // Then:
    assertThat(asJsonString(bytes), is("62"));
  }

  @Test
  public void shouldThrowIfNotInt() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT32_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for INT32: class java.lang.Boolean"))));
  }

  @Test
  public void shouldSerializeBigInt() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, 62L);

    // Then:
    assertThat(asJsonString(bytes), is("62"));
  }

  @Test
  public void shouldThrowIfNotBigInt() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for INT64: class java.lang.Boolean"))));
  }

  @Test
  public void shouldSerializeDouble() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, 62.0);

    // Then:
    assertThat(asJsonString(bytes), is("62.0"));
  }

  @Test
  public void shouldSerializeDecimal() {
    // Given:
    givenSerializerForSchema(DecimalUtil.builder(20, 19).build());

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, new BigDecimal("1.234567890123456789"));

    // Then:
    assertThat(asJsonString(bytes), is("1.234567890123456789"));
  }

  @Test
  public void shouldSerializeDecimalsWithoutStrippingTrailingZeros() {
    // Given:
    givenSerializerForSchema(DecimalUtil.builder(3, 1).build());

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, new BigDecimal("12.0"));

    // Then:
    assertThat(asJsonString(bytes), is("12.0"));
  }

  @Test
  public void shouldThrowIfNotDouble() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, new Struct(ORDER_SCHEMA))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for FLOAT64: class org.apache.kafka.connect.data.Struct"))));
  }

  @Test
  public void shouldSerializeString() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_STRING_SCHEMA);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, "a string");

    // Then:
    assertThat(asJsonString(bytes), is("\"a string\""));
  }

  @Test
  public void shouldThrowIfNotString() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_STRING_SCHEMA);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, new Struct(ORDER_SCHEMA))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for STRING: class org.apache.kafka.connect.data.Struct"))));
  }

  @Test
  public void shouldSerializeArray() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .array(Schema.BOOLEAN_SCHEMA)
        .build()
    );

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ImmutableList.of(true, false));

    // Then:
    assertThat(asJsonString(bytes), is("[true,false]"));
  }

  @Test
  public void shouldThrowIfNotArray() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .array(Schema.BOOLEAN_SCHEMA)
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for ARRAY: class java.lang.Boolean"))));
  }

  @Test
  public void shouldThrowOnWrongElementType() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .array(Schema.BOOLEAN_SCHEMA)
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, ImmutableList.of("not boolean"))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for BOOLEAN: class java.lang.String"))));
  }

  @Test
  public void shouldSerializeMap() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
        .build()
    );

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, ImmutableMap.of("a", 1, "b", 2));

    // Then:
    if (useSchemas) {
      assertThat(asJsonString(bytes), is("[{\"key\":\"a\",\"value\":1},{\"key\":\"b\",\"value\":2}]"));
    } else {
      assertThat(asJsonString(bytes), is("{\"a\":1,\"b\":2}"));
    }
  }

  @Test
  public void shouldThrowIfNotMap() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for MAP: class java.lang.Boolean"))));
  }

  @Test
  public void shouldThrowIfKeyWrongType() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, ImmutableMap.of(1, 2))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for STRING: class java.lang.Integer"))));
  }

  @Test
  public void shouldThrowIfValueWrongType() {
    // Given:
    givenSerializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize(SOME_TOPIC, ImmutableMap.of("a", false))
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(CoreMatchers.is(
        "Invalid type for INT64: class java.lang.Boolean"))));
  }

  @Test
  public void shouldThrowOnMapSchemaWithNonStringKeys() {
    // Given:
    final PersistenceSchema physicalSchema = PersistenceSchema.from(
        (ConnectSchema) SchemaBuilder
            .struct()
            .field("f0", SchemaBuilder
                .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
                .optional()
                .build())
            .build(),
        true
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new KsqlJsonSerdeFactory(false).createSerde(physicalSchema, config, () -> null)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "JSON only supports MAP types with STRING keys"));
  }

  @Test
  public void shouldThrowOnNestedMapSchemaWithNonStringKeys() {
    // Given:
    final PersistenceSchema physicalSchema = PersistenceSchema.from(
        (ConnectSchema) SchemaBuilder
            .struct()
            .field("f0", SchemaBuilder
                .struct()
                .field("f1", SchemaBuilder
                    .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
                    .optional()
                    .build())
                .build())
            .build(),
        true
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new KsqlJsonSerdeFactory(false).createSerde(physicalSchema, config, () -> null)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "JSON only supports MAP types with STRING keys"));
  }

  @Test
  public void shouldSerializeNullAsNull() {
    assertThat(serializer.serialize(SOME_TOPIC, null), is(nullValue()));
  }

  @Test
  public void shouldHandleNulls() {
    // Given:
    final Struct struct = new Struct(ORDER_SCHEMA)
        .put(ORDERTIME, 1511897796092L)
        .put(ORDERID, 1L)
        .put(ITEMID, "item_1")
        .put(ORDERUNITS, 10.0)
        .put(ARRAYCOL, null)
        .put(MAPCOL, null)
        .put(DECIMALCOL, null);

    // When:
    final byte[] bytes = serializer.serialize(SOME_TOPIC, struct);

    // Then:
    assertThat(asJsonString(bytes), equalTo(
        "{"
            + "\"ORDERTIME\":1511897796092,"
            + "\"ORDERID\":1,"
            + "\"ITEMID\":\"item_1\","
            + "\"ORDERUNITS\":10.0,"
            + "\"ARRAYCOL\":null,"
            + "\"MAPCOL\":null,"
            + "\"DECIMALCOL\":null"
            + "}"));
  }

  @Test
  public void shouldIncludeTopicNameInException() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> serializer.serialize(SOME_TOPIC, true)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        SOME_TOPIC));
  }

  @Test
  public void shouldNotIncludeBadValueInExceptionAsThatWouldBeASecurityIssue() {
    // Given:
    givenSerializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    try {

      // When:
      serializer.serialize(SOME_TOPIC, "personal info: do not log me");

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  private String asJsonString(final byte[] bytes) {
    if (useSchemas) {
      return new String(Arrays.copyOfRange(bytes, 5, bytes.length), StandardCharsets.UTF_8);
    } else {
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  private static Struct buildWithNestedStruct() {
    final Struct topLevel = new Struct(SCHEMA_WITH_STRUCT);

    topLevel.put("ordertime", 1234567L);
    topLevel.put("orderid", 10L);

    final Struct category = new Struct(CATEGORY_SCHEMA);
    category.put("ID", Math.random() > 0.5 ? 1L : 2L);
    category.put("NAME", Math.random() > 0.5 ? "Produce" : "Food");

    final Struct item = new Struct(ITEM_SCHEMA);
    item.put(ITEMID, 10L);
    item.put("NAME", "Item_10");
    item.put("CATEGORIES", Collections.singletonList(category));

    topLevel.put("itemid", item);
    topLevel.put("orderunits", 10);
    topLevel.put("arraycol", Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0));

    final Map<String, Double> map = new HashMap<>();
    map.put("key1", 10.0);
    map.put("key2", 20.0);
    map.put("key3", 30.0);
    topLevel.put("mapcol", map);

    final Struct address = new Struct(ADDRESS_SCHEMA);
    address.put("NUMBER", 101L);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301L);

    topLevel.put("address", address);

    return topLevel;
  }

  private void givenSerializerForSchema(final Schema serializedSchema) {
    final boolean unwrap = serializedSchema.type() != Type.STRUCT;
    final Schema ksqlSchema = unwrap
        ? SchemaBuilder.struct().field("f", serializedSchema).build()
        : serializedSchema;

    final PersistenceSchema persistenceSchema = PersistenceSchema
        .from((ConnectSchema) ksqlSchema, unwrap);

    serializer = new KsqlJsonSerdeFactory(useSchemas)
        .createSerde(persistenceSchema, config, () -> srClient)
        .serializer();
  }
}
