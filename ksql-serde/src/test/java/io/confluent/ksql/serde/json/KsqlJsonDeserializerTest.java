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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlJsonDeserializerTest {

  private static final String SOME_TOPIC = "bob";

  private static final String ORDERTIME = "ORDERTIME";
  private static final String ORDERID = "ORDERID";
  private static final String ITEMID = "ITEMID";
  private static final String ORDERUNITS = "ORDERUNITS";
  private static final String ARRAYCOL = "ARRAYCOL";
  private static final String MAPCOL = "MAPCOL";

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
      .build();

  private static final Map<String, Object> AN_ORDER = ImmutableMap.<String, Object>builder()
      .put("ordertime", 1511897796092L)
      .put("@orderid", 1L)
      .put("itemid", "Item_1")
      .put("orderunits", 10.0)
      .put("arraycol", ImmutableList.of(10.0, 20.0))
      .put("mapcol", Collections.singletonMap("key1", 10.0))
      .build();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private Struct expectedOrder;
  private Schema connectSchema;
  private KsqlJsonDeserializer deserializer;

  @Before
  public void before() {
    expectedOrder = new Struct(ORDER_SCHEMA)
        .put(ORDERTIME, 1511897796092L)
        .put(ORDERID, 1L)
        .put(ITEMID, "Item_1")
        .put(ORDERUNITS, 10.0)
        .put(ARRAYCOL, ImmutableList.of(10.0, 20.0))
        .put(MAPCOL, ImmutableMap.of("key1", 10.0));

    givenDeserializerForSchema(ORDER_SCHEMA);
  }

  @Test
  public void shouldDeserializeJsonObjectCorrectly() {
    // Given:
    final byte[] bytes = serializeJson(AN_ORDER);

    // When:
    final Struct result = (Struct) deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(expectedOrder));
  }

  @Test
  public void shouldCoerceFieldValues() {
    // Given:
    final Map<String, Object> anOrder = new HashMap<>(AN_ORDER);
    anOrder.put("orderId", 1); // <-- int, rather than required long in ORDER_SCHEMA.

    final byte[] bytes = serializeJson(anOrder);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(expectedOrder));
  }

  @Test
  public void shouldThrowIfNotAnObject() {
    // Given:
    final byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(containsString(
        "Can't convert type. sourceType: Boolean, requiredType: STRUCT<ORDERTIME BIGINT")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldThrowIfFieldCanNotBeCoerced() {
    // Given:
    final Map<String, Object> value = new HashMap<>(AN_ORDER);
    value.put("ordertime", true);

    final byte[] bytes = serializeJson(value);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Can't convert type. sourceType: Boolean, requiredType: BIGINT")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializeJsonObjectWithRedundantFields() {
    // Given:
    final Map<String, Object> orderRow = new HashMap<>(AN_ORDER);
    orderRow.put("extraField", "should be ignored");

    final byte[] bytes = serializeJson(orderRow);

    // When:
    final Struct result = (Struct) deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(expectedOrder));
  }

  @Test
  public void shouldDeserializeJsonObjectWithMissingFields() {
    // Given:
    final Map<String, Object> orderRow = new HashMap<>(AN_ORDER);
    orderRow.remove("ordertime");

    final byte[] bytes = serializeJson(orderRow);

    // When:
    final Struct result = (Struct) deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(expectedOrder
        .put(ORDERTIME, null)
    ));
  }

  @Test
  public void shouldDeserializeNullAsNull() {
    assertThat(deserializer.deserialize(SOME_TOPIC, null), is(nullValue()));
  }

  @Test
  public void shouldTreatNullAsNull() {
    // Given:
    final Map<String, Object> row = new HashMap<>();
    row.put("ordertime", null);
    row.put("@orderid", null);
    row.put("itemid", null);
    row.put("orderunits", null);
    row.put("arrayCol", new Double[]{0.0, null});
    row.put("mapCol", null);

    final byte[] bytes = serializeJson(row);

    // When:
    final Struct result = (Struct) deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(expectedOrder
        .put(ORDERTIME, null)
        .put(ORDERID, null)
        .put(ITEMID, null)
        .put(ORDERUNITS, null)
        .put(ARRAYCOL, Arrays.asList(0.0, null))
        .put(MAPCOL, null)
    ));
  }

  @Test
  public void shouldCreateJsonStringForStructIfDefinedAsVarchar() {
    // Given:
    givenDeserializerForSchema(
        SchemaBuilder.struct()
            .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
            .build()
    );

    final byte[] bytes = ("{"
        + "\"itemid\": {"
        + "    \"CATEGORY\": {"
        + "      \"ID\":2,"
        + "      \"NAME\":\"Food\""
        + "    },"
        + "    \"ITEMID\":6,"
        + "    \"NAME\":\"Item_6\""
        + "  }"
        + "}").getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct result = (Struct) deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result.schema(), is(connectSchema));
    assertThat(result.get(ITEMID),
        is("{\"CATEGORY\":{\"ID\":2,\"NAME\":\"Food\"},\"ITEMID\":6,\"NAME\":\"Item_6\"}"));
  }

  @Test
  public void shouldDeserializedJsonBoolean() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    final byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(true));
  }

  @Test
  public void shouldThrowIfCanNotCoerceToBoolean() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    final byte[] bytes = "24".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Can't convert type. sourceType: Long, requiredType: BOOLEAN")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedJsonNumberAsInt() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT32_SCHEMA);

    final List<String> validCoercions = ImmutableList.of(
        "41",
        "41.456",
        "\"41\""
    );

    validCoercions.forEach(value -> {

      final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(41));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToInt() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT32_SCHEMA);

    final byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Can't convert type. sourceType: Boolean, requiredType: INTEGER")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedJsonNumberAsBigInt() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    final List<String> validCoercions = ImmutableList.of(
        "42",
        "42.456",
        "\"42\""
    );

    validCoercions.forEach(value -> {

      final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(42L));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToBigInt() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    final byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Can't convert type. sourceType: Boolean, requiredType: BIGINT")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedJsonNumberAsDouble() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);

    final List<String> validCoercions = ImmutableList.of(
        "42",
        "42.000",
        "\"42\"",
        "\"42.000\""
    );

    validCoercions.forEach(value -> {

      final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(42.0));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToDouble() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);

    final byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Can't convert type. sourceType: Boolean, requiredType: DOUBLE")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedJsonText() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_STRING_SCHEMA);

    final Map<String, String> validCoercions = ImmutableMap.<String, String>builder()
        .put("true", "true")
        .put("42", "42")
        .put("42.000", "42.0")
        .put("42.001", "42.001")
        .put("\"just a string\"", "just a string")
        .put("{\"json\": \"object\"}", "{\"json\":\"object\"}")
        .put("[\"json\", \"array\"]", "[json, array]")
        .build();

    validCoercions.forEach((jsonValue, expectedValue) -> {

      final byte[] bytes = jsonValue.getBytes(StandardCharsets.UTF_8);

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(expectedValue));
    });
  }

  @Test
  public void shouldDeserializedJsonArray() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .array(Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    final byte[] bytes = serializeJson(ImmutableList.of(42, 42.000, "42"));

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(ImmutableList.of(42L, 42L, 42L)));
  }

  @Test
  public void shouldThrowIfNotAnArray() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .array(Schema.OPTIONAL_STRING_SCHEMA)
        .build()
    );

    final byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Can't convert type. sourceType: Boolean, requiredType: ARRAY<VARCHAR>")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldThrowIfCanNotCoerceArrayElement() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .array(Schema.OPTIONAL_INT32_SCHEMA)
        .build()
    );

    final List<String> expected = ImmutableList.of("not", "numbers");

    final byte[] bytes = serializeJson(expected);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Can't coerce string to type. targetType: INTEGER")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldDeserializedJsonObjectAsMap() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
        .build()
    );

    final byte[] bytes = serializeJson(ImmutableMap.of("a", 42, "b", 42L, "c", "42"));

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(ImmutableMap.of("a", 42L, "b", 42L, "c", 42L)));
  }

  @Test
  public void shouldThrowIfNotAnMap() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
        .build()
    );

    final byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Can't convert type. sourceType: Boolean, requiredType: MAP<VARCHAR, INT>")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldThrowIfCanNotCoerceMapValue() {
    // Given:
    givenDeserializerForSchema(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
        .build()
    );

    final byte[] bytes = serializeJson(ImmutableMap.of("a", 1, "b", true));

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is(
        "Can't convert type. sourceType: Boolean, requiredType: INTEGER")));

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldThrowOnMapSchemaWithNonStringKeys() {
    // Given:
    final PersistenceSchema physicalSchema = PersistenceSchema.of(
        (ConnectSchema) SchemaBuilder
            .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
            .build()
    );

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Only MAPs with STRING keys are supported");

    // When:
    new KsqlJsonDeserializer(physicalSchema);
  }

  @Test
  public void shouldThrowOnNestedMapSchemaWithNonStringKeys() {
    // Given:
    final PersistenceSchema physicalSchema = PersistenceSchema.of(
        (ConnectSchema) SchemaBuilder
            .struct()
            .field("f0", SchemaBuilder
                .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
                .optional()
                .build())
            .build()
    );

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Only MAPs with STRING keys are supported");

    // When:
    new KsqlJsonDeserializer(physicalSchema);
  }

  @Test
  public void shouldIncludeTopicNameInException() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    final byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expectMessage(SOME_TOPIC);

    // When:
    deserializer.deserialize(SOME_TOPIC, bytes);
  }

  @Test
  public void shouldNotIncludeBadValueInExceptionAsThatWouldBeASecurityIssue() {
    // Given:
    givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA);

    final byte[] bytes = "\"personal info: do not log me\"".getBytes(StandardCharsets.UTF_8);

    try {

      // When:
      deserializer.deserialize(SOME_TOPIC, bytes);

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  private void givenDeserializerForSchema(final Schema connectSchema) {
    final PersistenceSchema physicalSchema = PersistenceSchema.of((ConnectSchema) connectSchema);
    this.connectSchema = connectSchema;
    deserializer = new KsqlJsonDeserializer(physicalSchema);
  }

  private static byte[] serializeJson(final Object expected) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(expected);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
