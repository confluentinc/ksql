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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@SuppressWarnings("rawtypes")
@RunWith(Parameterized.class)
public class KsqlJsonDeserializerTest {

  private static final String SOME_TOPIC = "bob";

  private static final String ORDERTIME = "ORDERTIME";
  private static final String ORDERID = "@ORDERID";
  private static final String ITEMID = "ITEMID";
  private static final String ORDERUNITS = "ORDERUNITS";
  private static final String ARRAYCOL = "ARRAYCOL";
  private static final String MAPCOL = "MAPCOL";
  private static final String CASE_SENSITIVE_FIELD = "caseField";

  private static final Schema ORDER_SCHEMA = SchemaBuilder.struct()
      .field(ORDERTIME, Schema.OPTIONAL_INT64_SCHEMA)
      .field(ORDERID, Schema.OPTIONAL_INT64_SCHEMA)
      .field(ITEMID, Schema.OPTIONAL_STRING_SCHEMA)
      .field(ORDERUNITS, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field(CASE_SENSITIVE_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
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
      .put("caseField", 1L)
      .build();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{"Plain JSON", false}, {"Magic byte prefixed", true}});
  }

  @Parameter
  public String suiteName;

  @Parameter(1)
  public boolean useSchemas;

  private Struct expectedOrder;
  private KsqlJsonDeserializer<Struct> deserializer;

  @Before
  public void before() {
    expectedOrder = new Struct(ORDER_SCHEMA)
        .put(ORDERTIME, 1511897796092L)
        .put(ORDERID, 1L)
        .put(ITEMID, "Item_1")
        .put(ORDERUNITS, 10.0)
        .put(ARRAYCOL, ImmutableList.of(10.0, 20.0))
        .put(MAPCOL, ImmutableMap.of("key1", 10.0))
        .put(CASE_SENSITIVE_FIELD, 1L);

    deserializer = givenDeserializerForSchema(ORDER_SCHEMA, Struct.class);
  }

  @Test
  public void shouldDeserializeJsonObjectCorrectly() {
    // Given:
    final byte[] bytes = serializeJson(AN_ORDER);

    // When:
    final Struct result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(expectedOrder));
  }

  @Test
  public void shouldIgnoreDeserializeJsonObjectCaseMismatch() {
    // Given:
    final Map<String, Object> anOrder = ImmutableMap.<String, Object>builder()
        .put("CASEFIELD", 1L)
        .build();
    
    final byte[] bytes = serializeJson(anOrder);

    // When:
    final Struct result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(new Struct(ORDER_SCHEMA)));
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
    final byte[] bytes = serializeJson(BooleanNode.valueOf(true));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString(
        "Can't convert type. sourceType: BooleanNode, requiredType: STRUCT<ORDERTIME BIGINT"))));
  }

  @Test
  public void shouldThrowIfFieldCanNotBeCoerced() {
    // Given:
    final Map<String, Object> value = new HashMap<>(AN_ORDER);
    value.put("ordertime", true);

    final byte[] bytes = serializeJson(value);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't convert type. sourceType: BooleanNode, requiredType: BIGINT"))));
  }

  @Test
  public void shouldDeserializeJsonObjectWithRedundantFields() {
    // Given:
    final Map<String, Object> orderRow = new HashMap<>(AN_ORDER);
    orderRow.put("extraField", "should be ignored");

    final byte[] bytes = serializeJson(orderRow);

    // When:
    final Struct result = deserializer.deserialize(SOME_TOPIC, bytes);

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
    final Struct result = deserializer.deserialize(SOME_TOPIC, bytes);

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
    final HashMap<String, Object> mapValue = new HashMap<>();
    mapValue.put("a", 1.0);
    mapValue.put("b", null);

    final Map<String, Object> row = new HashMap<>();
    row.put("ordertime", null);
    row.put("@orderid", null);
    row.put("itemid", null);
    row.put("orderunits", null);
    row.put("arrayCol", new Double[]{0.0, null});
    row.put("mapCol", mapValue);

    final byte[] bytes = serializeJson(row);

    // When:
    final Struct result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(expectedOrder
        .put(ORDERTIME, null)
        .put(ORDERID, null)
        .put(ITEMID, null)
        .put(ORDERUNITS, null)
        .put(ARRAYCOL, Arrays.asList(0.0, null))
        .put(MAPCOL, mapValue)
        .put(CASE_SENSITIVE_FIELD, null)
    ));
  }

  @Test
  public void shouldCreateJsonStringForStructIfDefinedAsVarchar() {
    // Given:
    final KsqlJsonDeserializer<Struct> deserializer = givenDeserializerForSchema(
        SchemaBuilder.struct()
            .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
            .build(),
        Struct.class
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
    final Struct result = deserializer.deserialize(SOME_TOPIC, addMagic(bytes));

    // Then:
    assertThat(result.get(ITEMID),
        is("{\"CATEGORY\":{\"ID\":2,\"NAME\":\"Food\"},\"ITEMID\":6,\"NAME\":\"Item_6\"}"));
  }

  @Test
  public void shouldDeserializedJsonBoolean() {
    // Given:
    final KsqlJsonDeserializer<Boolean> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA, Boolean.class);

    final byte[] bytes = serializeJson(BooleanNode.valueOf(true));

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(true));
  }

  @Test
  public void shouldThrowIfCanNotCoerceToBoolean() {
    // Given:
    final KsqlJsonDeserializer<Boolean> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA, Boolean.class);

    final byte[] bytes = serializeJson(IntNode.valueOf(23));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't convert type. sourceType: IntNode, requiredType: BOOLEAN"))));
  }

  @Test
  public void shouldDeserializedJsonNumberAsInt() {
    // Given:
    final KsqlJsonDeserializer<Integer> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_INT32_SCHEMA, Integer.class);

    final List<String> validCoercions = ImmutableList.of(
        "41",
        "41.456",
        "\"41\""
    );

    validCoercions.forEach(value -> {

      final byte[] bytes = addMagic(value.getBytes(StandardCharsets.UTF_8));

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(41));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToInt() {
    // Given:
    final KsqlJsonDeserializer<Integer> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_INT32_SCHEMA, Integer.class);

    final byte[] bytes = serializeJson(BooleanNode.valueOf(true));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't convert type. sourceType: BooleanNode, requiredType: INTEGER"))));
  }

  @Test
  public void shouldDeserializedJsonNumberAsBigInt() {
    // Given:
    final KsqlJsonDeserializer<Long> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA, Long.class);

    final List<String> validCoercions = ImmutableList.of(
        "42",
        "42.456",
        "\"42\""
    );

    validCoercions.forEach(value -> {

      final byte[] bytes = addMagic(value.getBytes(StandardCharsets.UTF_8));

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(42L));
    });
  }


  @Test
  public void shouldThrowIfCanNotCoerceToBigInt() {
    // Given:
    final KsqlJsonDeserializer<Long> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA, Long.class);

    final byte[] bytes = serializeJson(BooleanNode.valueOf(true));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't convert type. sourceType: BooleanNode, requiredType: BIGINT"))));
  }

  @Test
  public void shouldDeserializedJsonNumberAsDouble() {
    // Given:
    final KsqlJsonDeserializer<Double> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA, Double.class);

    final List<String> validCoercions = ImmutableList.of(
        "42",
        "42.000",
        "\"42\"",
        "\"42.000\""
    );

    validCoercions.forEach(value -> {

      final byte[] bytes = addMagic(value.getBytes(StandardCharsets.UTF_8));

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(42.0));
    });
  }

  @Test
  public void shouldThrowIfCanNotCoerceToDouble() {
    // Given:
    final KsqlJsonDeserializer<Double> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA, Double.class);

    final byte[] bytes = serializeJson(BooleanNode.valueOf(true));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't convert type. sourceType: BooleanNode, requiredType: DOUBLE"))));
  }

  @Test
  public void shouldDeserializedJsonText() {
    // Given:
    final KsqlJsonDeserializer<String> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_STRING_SCHEMA, String.class);

    final Map<String, String> validCoercions = ImmutableMap.<String, String>builder()
        .put("true", "true")
        .put("42", "42")
        .put("42.000", "42.000")
        .put("42.001", "42.001")
        .put("\"just a string\"", "just a string")
        .put("{\"json\": \"object\"}", "{\"json\":\"object\"}")
        .put("[\"json\", \"array\"]", "[json, array]")
        .build();

    validCoercions.forEach((jsonValue, expectedValue) -> {

      final byte[] bytes = addMagic(jsonValue.getBytes(StandardCharsets.UTF_8));

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(expectedValue));
    });
  }

  @Test
  public void shouldDeserializedJsonNumberAsBigDecimal() {
    // Given:
    final KsqlJsonDeserializer<BigDecimal> deserializer = 
        givenDeserializerForSchema(DecimalUtil.builder(20, 19).build(), BigDecimal.class);

    final List<String> validCoercions = ImmutableList.of(
        "1.1234512345123451234",
        "\"1.1234512345123451234\""
    );

    validCoercions.forEach(value -> {

      final byte[] bytes = addMagic(value.getBytes(StandardCharsets.UTF_8));

      // When:
      final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

      // Then:
      assertThat(result, is(new BigDecimal("1.1234512345123451234")));
    });
  }

  @Test
  public void shouldDeserializeDecimalsWithoutStrippingTrailingZeros() {
    // Given:
    final KsqlJsonDeserializer<BigDecimal> deserializer = 
        givenDeserializerForSchema(DecimalUtil.builder(3, 1).build(), BigDecimal.class);

    final byte[] bytes = addMagic("10.0".getBytes(UTF_8));

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(new BigDecimal("10.0")));
  }

  @Test
  public void shouldFixScaleWhenDeserializingDecimalsWithTooSmallAScale() {
    // Given:
    final KsqlJsonDeserializer<BigDecimal> deserializer =
        givenDeserializerForSchema(DecimalUtil.builder(4, 3).build(), BigDecimal.class);

    final byte[] bytes = addMagic("1.1".getBytes(UTF_8));

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(new BigDecimal("1.100")));
  }

  @Test
  public void shouldThrowIfDecimalHasLargerScale() {
    // Given:
    final KsqlJsonDeserializer<BigDecimal> deserializer =
        givenDeserializerForSchema(DecimalUtil.builder(4, 1).build(), BigDecimal.class);

    final byte[] bytes = addMagic("1.12".getBytes(UTF_8));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Cannot fit decimal '1.12' into DECIMAL(4, 1) without rounding."));
  }

  @Test
  public void shouldDeserializeScientificNotation() {
    // Given:
    final KsqlJsonDeserializer<BigDecimal> deserializer = 
        givenDeserializerForSchema(DecimalUtil.builder(3, 1).build(), BigDecimal.class);

    final byte[] bytes = addMagic("1E+1".getBytes(UTF_8));

    // When:
    final Object result = deserializer.deserialize(SOME_TOPIC, bytes);

    // Then:
    assertThat(result, is(new BigDecimal("10.0")));
  }

  @Test
  public void shouldThrowIfCanNotCoerceToBigDecimal() {
    // Given:
    final KsqlJsonDeserializer<BigDecimal> deserializer = 
        givenDeserializerForSchema(DecimalUtil.builder(20, 19).build(), BigDecimal.class);

    final byte[] bytes = serializeJson(BooleanNode.valueOf(true));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't convert type. sourceType: BooleanNode, requiredType: DECIMAL(20, 19)"))));
  }

  @Test
  public void shouldDeserializedJsonArray() {
    // Given:
    final KsqlJsonDeserializer<List> deserializer = givenDeserializerForSchema(
        SchemaBuilder
            .array(Schema.OPTIONAL_INT64_SCHEMA)
            .build(),
        List.class
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
    final KsqlJsonDeserializer<List> deserializer = givenDeserializerForSchema(
        SchemaBuilder
            .array(Schema.OPTIONAL_STRING_SCHEMA)
            .build(),
        List.class
    );

    final byte[] bytes = serializeJson(BooleanNode.valueOf(true));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't convert type. sourceType: BooleanNode, requiredType: ARRAY<VARCHAR>"))));
  }

  @Test
  public void shouldThrowIfCanNotCoerceArrayElement() {
    // Given:
    final KsqlJsonDeserializer<List> deserializer = givenDeserializerForSchema(
        SchemaBuilder
            .array(Schema.OPTIONAL_INT32_SCHEMA)
            .build(),
        List.class
    );

    final List<String> expected = ImmutableList.of("not", "numbers");

    final byte[] bytes = serializeJson(expected);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't coerce string to type. targetType: INTEGER"))));
  }

  @Test
  public void shouldDeserializedJsonObjectAsMap() {
    // Given:
    final KsqlJsonDeserializer<Map> deserializer = givenDeserializerForSchema(
        SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .build(),
        Map.class
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
    final KsqlJsonDeserializer<Map> deserializer = givenDeserializerForSchema(
        SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
            .build(),
        Map.class
    );

    final byte[] bytes = serializeJson(BooleanNode.valueOf(true));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't convert type. sourceType: BooleanNode, requiredType: MAP<VARCHAR, INT>"))));
  }

  @Test
  public void shouldThrowIfCanNotCoerceMapValue() {
    // Given:
    final KsqlJsonDeserializer<Map> deserializer = givenDeserializerForSchema(
        SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
            .build(),
        Map.class
    );

    final byte[] bytes = serializeJson(ImmutableMap.of("a", 1, "b", true));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(startsWith(
        "Can't convert type. sourceType: BooleanNode, requiredType: INTEGER"))));
  }

  @Test
  public void shouldThrowOnMapSchemaWithNonStringKeys() {
    // Given:
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", SchemaBuilder
            .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
            .optional()
            .build())
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new KsqlJsonDeserializer<>(schema, false, Struct.class)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "JSON only supports MAP types with STRING keys"));
  }

  @Test
  public void shouldThrowOnNestedMapSchemaWithNonStringKeys() {
    // Given:
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", SchemaBuilder
            .struct()
            .field("f1", SchemaBuilder
                .map(Schema.OPTIONAL_INT32_SCHEMA, Schema.INT32_SCHEMA)
                .optional()
                .build())
            .build())
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new KsqlJsonDeserializer<>(schema, false, Struct.class)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "JSON only supports MAP types with STRING keys"));
  }

  @Test
  public void shouldIncludeTopicNameInException() {
    // Given:
    final KsqlJsonDeserializer<Long> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA, Long.class);

    final byte[] bytes = "true".getBytes(StandardCharsets.UTF_8);

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        SOME_TOPIC));
  }

  @Test
  public void shouldNotIncludeBadValueInExceptionAsThatWouldBeASecurityIssue() {
    // Given:
    final KsqlJsonDeserializer<Long> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_INT64_SCHEMA, Long.class);

    final byte[] bytes = "\"personal info: do not log me\"".getBytes(StandardCharsets.UTF_8);

    try {

      // When:
      deserializer.deserialize(SOME_TOPIC, bytes);

      fail("Invalid test: should throw");

    } catch (final Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), not(containsString("personal info")));
    }
  }

  @Test
  public void shouldIncludePathForErrorsInRootNode() {
    // Given:
    final KsqlJsonDeserializer<Double> deserializer = 
        givenDeserializerForSchema(Schema.OPTIONAL_FLOAT64_SCHEMA, Double.class);

    final byte[] bytes = serializeJson(BooleanNode.valueOf(true));

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(endsWith(", path: $"))));
  }

  @Test
  public void shouldIncludePathForErrorsInObjectFieldsValue() {
    // Given:
    final Map<String, Object> value = new HashMap<>(AN_ORDER);
    value.put("ordertime", true);

    final byte[] bytes = serializeJson(value);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(endsWith(", path: $.ORDERTIME"))));
  }

  @Test
  public void shouldIncludePathForErrorsInArrayElements() {
    // Given:
    final KsqlJsonDeserializer<List> deserializer = givenDeserializerForSchema(
        SchemaBuilder
            .array(Schema.OPTIONAL_INT32_SCHEMA)
            .build(),
        List.class
    );

    final List<?> expected = ImmutableList.of(0, "not", "numbers");

    final byte[] bytes = serializeJson(expected);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(endsWith("path: $[1]"))));
  }

  @Test
  public void shouldIncludePathForErrorsInMapValues() {
    // Given:
    final KsqlJsonDeserializer<Map> deserializer = givenDeserializerForSchema(
        SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
            .build(),
        Map.class
    );

    final byte[] bytes = serializeJson(ImmutableMap.of("a", 1, "b", true));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize(SOME_TOPIC, bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(endsWith("path: $.b.value"))));
  }
  
  private <T> KsqlJsonDeserializer<T> givenDeserializerForSchema(
      final Schema schema, 
      final Class<T> type
  ) {
    return new KsqlJsonDeserializer<>((ConnectSchema) schema, useSchemas, type);
  }

  private byte[] serializeJson(final Object expected) {
    try {
      return addMagic(OBJECT_MAPPER.writeValueAsBytes(expected));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] addMagic(final byte[] json) {
    if (useSchemas) {
      return ArrayUtils.addAll(new byte[]{/*magic*/ 0x00, /*schema*/ 0x00, 0x00, 0x00, 0x01}, json);
    } else {
      return json;
    }
  }
}
