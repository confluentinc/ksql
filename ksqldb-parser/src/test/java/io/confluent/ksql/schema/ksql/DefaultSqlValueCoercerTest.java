/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.SqlValueCoercer.Result;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DefaultSqlValueCoercerTest {

  private static final Map<SqlBaseType, SqlType> TYPES = ImmutableMap
      .<SqlBaseType, SqlType>builder()
      .put(SqlBaseType.BOOLEAN, SqlTypes.BOOLEAN)
      .put(SqlBaseType.INTEGER, SqlTypes.INTEGER)
      .put(SqlBaseType.BIGINT, SqlTypes.BIGINT)
      .put(SqlBaseType.DECIMAL, SqlTypes.decimal(2, 1))
      .put(SqlBaseType.DOUBLE, SqlTypes.DOUBLE)
      .put(SqlBaseType.STRING, SqlTypes.STRING)
      .put(SqlBaseType.ARRAY, SqlTypes.array(SqlTypes.BIGINT))
      .put(SqlBaseType.MAP, SqlTypes.map(SqlTypes.INTEGER, SqlTypes.BIGINT))
      .put(SqlBaseType.STRUCT, SqlTypes.struct().field("fred", SqlTypes.INTEGER).build())
      .build();

  private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
      .field("fred", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  private static final Map<SqlBaseType, Object> INSTANCES = ImmutableMap
      .<SqlBaseType, Object>builder()
      .put(SqlBaseType.BOOLEAN, false)
      .put(SqlBaseType.INTEGER, 1)
      .put(SqlBaseType.BIGINT, 2L)
      .put(SqlBaseType.DECIMAL, BigDecimal.ONE)
      .put(SqlBaseType.DOUBLE, 3.0D)
      .put(SqlBaseType.STRING, "4.1")
      .put(SqlBaseType.ARRAY, ImmutableList.of(1L, 2L))
      .put(SqlBaseType.MAP, ImmutableMap.of(10, 1L))
      .put(SqlBaseType.STRUCT, new Struct(STRUCT_SCHEMA).put("fred", 11))
      .build();

  private final DefaultSqlValueCoercer coercer;

  @Parameters(name = "{0}")
  public static Iterable<Object[]> data() {
    return Arrays.stream(DefaultSqlValueCoercer.values())
        .map(coercer -> new Object[]{coercer})
        .collect(Collectors.toList());
  }

  public DefaultSqlValueCoercerTest(final DefaultSqlValueCoercer coercer) {
    this.coercer = coercer;
  }

  @Test
  public void shouldCoerceNullToAnything() {
    TYPES.values().forEach(type ->
        assertThat(type.toString(), coercer.coerce(null, type), is(Result.nullResult())));
  }

  @Test
  public void shouldCoerceToBoolean() {
    assertThat(coercer.coerce(true, SqlTypes.BOOLEAN), is(Result.of(true)));
  }

  @Test
  public void shouldNotCoerceToBoolean() {
    assertThat(coercer.coerce("true", SqlTypes.BOOLEAN), is(Result.failure()));
    assertThat(coercer.coerce(1, SqlTypes.BOOLEAN), is(Result.failure()));
    assertThat(coercer.coerce(1L, SqlTypes.BOOLEAN), is(Result.failure()));
    assertThat(coercer.coerce(1.0d, SqlTypes.BOOLEAN), is(Result.failure()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.BOOLEAN), is(Result.failure()));
  }

  @Test
  public void shouldCoerceToInteger() {
    assertThat(coercer.coerce(1, SqlTypes.INTEGER), is(Result.of(1)));
  }

  @Test
  public void shouldNotCoerceToInteger() {
    assertThat(coercer.coerce(true, SqlTypes.INTEGER), is(Result.failure()));
    assertThat(coercer.coerce(1L, SqlTypes.INTEGER), is(Result.failure()));
    assertThat(coercer.coerce(1.0d, SqlTypes.INTEGER), is(Result.failure()));
    assertThat(coercer.coerce("1", SqlTypes.INTEGER), is(Result.failure()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.INTEGER), is(Result.failure()));
  }

  @Test
  public void shouldCoerceToBigInt() {
    assertThat(coercer.coerce(1, SqlTypes.BIGINT), is(Result.of(1L)));
    assertThat(coercer.coerce(1L, SqlTypes.BIGINT), is(Result.of(1L)));
  }

  @Test
  public void shouldNotCoerceToBigInt() {
    assertThat(coercer.coerce(true, SqlTypes.BIGINT), is(Result.failure()));
    assertThat(coercer.coerce(1.0d, SqlTypes.BIGINT), is(Result.failure()));
    assertThat(coercer.coerce("1", SqlTypes.BIGINT), is(Result.failure()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.BIGINT), is(Result.failure()));
  }

  @Test
  public void shouldCoerceToDecimal() {
    final SqlType decimalType = SqlTypes.decimal(2, 1);
    assertThat(coercer.coerce(1, decimalType), is(Result.of(new BigDecimal("1.0"))));
    assertThat(coercer.coerce(1L, decimalType), is(Result.of(new BigDecimal("1.0"))));
    assertThat(coercer.coerce(new BigDecimal("1.0"), decimalType),
        is(Result.of(new BigDecimal("1.0"))));
    if (coercer.isAllowCastStringAndDoubleToDecimal()) {
      assertThat(coercer.coerce("1.0", decimalType), is(Result.of(new BigDecimal("1.0"))));
      assertThat(coercer.coerce(1.0d, decimalType), is(Result.of(new BigDecimal("1.0"))));
    }
  }

  @Test
  public void shouldNotCoerceToDecimal() {
    final SqlType decimalType = SqlTypes.decimal(2, 1);
    assertThat(coercer.coerce(true, decimalType), is(Result.failure()));
    assertThat(coercer.coerce(1234L, decimalType), is(Result.failure()));
    if (!coercer.isAllowCastStringAndDoubleToDecimal()) {
      assertThat(coercer.coerce("1.0", decimalType), is(Result.failure()));
      assertThat(coercer.coerce(1.0d, decimalType), is(Result.failure()));
    }
  }

  @Test
  public void shouldCoerceToDouble() {
    assertThat(coercer.coerce(1, SqlTypes.DOUBLE), is(Result.of(1.0d)));
    assertThat(coercer.coerce(1L, SqlTypes.DOUBLE), is(Result.of(1.0d)));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.DOUBLE), is(Result.of(123.0d)));
    assertThat(coercer.coerce(1.0d, SqlTypes.DOUBLE), is(Result.of(1.0d)));
  }

  @Test
  public void shouldNotCoerceToDouble() {
    assertThat(coercer.coerce(true, SqlTypes.DOUBLE), is(Result.failure()));
    assertThat(coercer.coerce("1", SqlTypes.DOUBLE), is(Result.failure()));
  }

  @Test
  public void shouldCoerceToArray() {
    final SqlType arrayType = SqlTypes.array(SqlTypes.DOUBLE);
    assertThat(coercer.coerce(ImmutableList.of(1), arrayType), is(Result.of(ImmutableList.of(1d))));
    assertThat(coercer.coerce(ImmutableList.of(1L), arrayType), is(Result.of(ImmutableList.of(1d))));
    assertThat(coercer.coerce(ImmutableList.of(1.1), arrayType),
        is(Result.of(ImmutableList.of(1.1d))));
    assertThat(coercer.coerce(Collections.singletonList(null), arrayType),
        is(Result.of(Collections.singletonList(null))));

    assertThat(coercer.coerce(new JsonArray().add(1), arrayType), is(Result.of(ImmutableList.of(1d))));
    assertThat(coercer.coerce(new JsonArray().add(1L), arrayType), is(Result.of(ImmutableList.of(1d))));
    assertThat(coercer.coerce(new JsonArray().add(1.1), arrayType), is(Result.of(ImmutableList.of(1.1d))));
    assertThat(coercer.coerce(new JsonArray().addNull(), arrayType), is(Result.of(Collections.singletonList(null))));
  }

  @Test
  public void shouldNotCoerceToArray() {
    final SqlType arrayType = SqlTypes.array(SqlTypes.DOUBLE);
    assertThat(coercer.coerce(true, arrayType), is(Result.failure()));
    assertThat(coercer.coerce(1L, arrayType), is(Result.failure()));
    assertThat(coercer.coerce("foo", arrayType), is(Result.failure()));
    assertThat(coercer.coerce(ImmutableMap.of("foo", 1), arrayType), is(Result.failure()));
    assertThat(coercer.coerce(new JsonObject().put("foo", 1), arrayType), is(Result.failure()));
  }

  @Test
  public void shouldCoerceToMap() {
    final SqlType mapType = SqlTypes.map(SqlTypes.STRING, SqlTypes.DOUBLE);
    assertThat(coercer.coerce(ImmutableMap.of("foo", 1), mapType), is(Result.of(ImmutableMap.of("foo", 1d))));
    assertThat(coercer.coerce(ImmutableMap.of("foo", 1L), mapType), is(Result.of(ImmutableMap.of("foo", 1d))));
    assertThat(coercer.coerce(ImmutableMap.of("foo", 1.1), mapType),
        is(Result.of(ImmutableMap.of("foo", 1.1d))));
    assertThat(coercer.coerce(Collections.singletonMap("foo", null), mapType),
        is(Result.of(Collections.singletonMap("foo", null))));

    assertThat(coercer.coerce(new JsonObject().put("foo", 1), mapType), is(Result.of(ImmutableMap.of("foo", 1d))));
    assertThat(coercer.coerce(new JsonObject().put("foo", 1L), mapType), is(Result.of(ImmutableMap.of("foo", 1d))));
    assertThat(coercer.coerce(new JsonObject().put("foo", 1.1), mapType), is(Result.of(ImmutableMap.of("foo", 1.1d))));
    assertThat(coercer.coerce(new JsonObject().putNull("foo"), mapType), is(Result.of(Collections.singletonMap("foo", null))));
  }

  @Test
  public void shouldNotCoerceToMap() {
    final SqlType mapType = SqlTypes.map(SqlTypes.STRING, SqlTypes.DOUBLE);
    assertThat(coercer.coerce(true, mapType), is(Result.failure()));
    assertThat(coercer.coerce(1L, mapType), is(Result.failure()));
    assertThat(coercer.coerce("foo", mapType), is(Result.failure()));
    assertThat(coercer.coerce(ImmutableList.of("foo"), mapType), is(Result.failure()));
    assertThat(coercer.coerce(new JsonArray().add("foo"), mapType), is(Result.failure()));
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceToStruct() {
    // Given:
    final Schema schema = SchemaBuilder.struct().field("foo", Schema.INT64_SCHEMA);
    final Struct struct = new Struct(schema).put("foo", 2L);
    final SqlType structType = SqlTypes.struct().field("foo", SqlTypes.decimal(2, 1)).build();

    // When:
    final Result result = coercer.coerce(struct, structType);

    // Then:
    assertThat("", !result.failed());
    final Optional<Struct> coerced = (Optional<Struct>) result.value();
    assertThat(coerced.get().get("foo"), is(new BigDecimal("2.0")));
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceToStructWithNullValues() {
    // Given:
    final Schema schema = SchemaBuilder.struct().field("foo", Schema.OPTIONAL_INT64_SCHEMA);
    final Struct struct = new Struct(schema).put("foo", null);
    final SqlType structType = SqlTypes.struct().field("foo", SqlTypes.decimal(2, 1)).build();

    // When:
    final Result result = coercer.coerce(struct, structType);

    // Then:
    assertThat("", !result.failed());
    final Optional<Struct> coerced = (Optional<Struct>) result.value();
    assertThat(coerced.get().get("foo"), is(nullValue()));
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldSubsetCoerceToStruct() {
    // Given:
    final Schema schema = SchemaBuilder.struct().field("foo", Schema.STRING_SCHEMA);
    final Struct struct = new Struct(schema).put("foo", "val1");
    final SqlType structType = SqlTypes.struct()
        .field("foo", SqlTypes.STRING)
        .field("bar", SqlTypes.STRING).build();

    // When:
    final Result result = coercer.coerce(struct, structType);

    // Then:
    assertThat("", !result.failed());
    final Optional<Struct> coerced = (Optional<Struct>) result.value();
    assertThat(coerced.get().get("foo"), is("val1"));
    assertThat(coerced.get().get("bar"), nullValue());
  }

  @Test
  public void shouldNotCoerceToStruct() {
    // Given:
    final SqlType structType = SqlTypes.struct().field("foo", SqlTypes.STRING).build();

    // When / Then:
    assertThat(coercer.coerce(ImmutableMap.of("foo", "bar"), structType), is(Result.failure()));
  }

  @Test
  public void shouldNotCoerceToStructIfAnyFieldFailsToCoerce() {
    // Given:
    final Schema schema = SchemaBuilder.struct().field("foo", Schema.INT64_SCHEMA);
    final Struct struct = new Struct(schema).put("foo", 2L);
    final SqlType structType = SqlTypes.struct().field("foo", SqlTypes.array(SqlTypes.INTEGER)).build();

    // When:
    final Result result = coercer.coerce(struct, structType);

    // Then:
    assertThat(result, is(Result.failure()));
  }

  @Test
  public void shouldCoerceToString() {
    assertThat(coercer.coerce("foobar", SqlTypes.STRING), is(Result.of("foobar")));
  }

  @Test
  public void shouldNotCoerceToString() {
    assertThat(coercer.coerce(true, SqlTypes.STRING), is(Result.failure()));
    assertThat(coercer.coerce(1, SqlTypes.STRING), is(Result.failure()));
    assertThat(coercer.coerce(1L, SqlTypes.STRING), is(Result.failure()));
    assertThat(coercer.coerce(1.0d, SqlTypes.STRING), is(Result.failure()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.STRING), is(Result.failure()));
  }

  @Test
  public void shouldCoerceUsingSameRulesAsBaseTypeUpCastRules() {
    for (final SqlBaseType fromBaseType : supportedTypes()) {
      // Given:
      final Map<Boolean, List<SqlBaseType>> partitioned = supportedTypes().stream()
          .collect(Collectors
              .partitioningBy(toBaseType -> coercionShouldBeSupported(fromBaseType, toBaseType)));

      final List<SqlBaseType> shouldUpCast = partitioned.getOrDefault(true, ImmutableList.of());
      final List<SqlBaseType> shouldNotUpCast = partitioned.getOrDefault(false, ImmutableList.of());

      if (coercer.isAllowCastStringAndDoubleToDecimal()
          && (fromBaseType == SqlBaseType.STRING || fromBaseType == SqlBaseType.DOUBLE)) {
        shouldNotUpCast.remove(SqlBaseType.DECIMAL);
        shouldUpCast.add(SqlBaseType.DECIMAL);
      }

      // Then:
      shouldUpCast.forEach(toBaseType -> assertThat(
          "should coerce " + fromBaseType + " to " + toBaseType,
          coercer.coerce(getInstance(fromBaseType), getType(toBaseType)),
          is(not(Result.failure()))
      ));

      shouldNotUpCast.forEach(toBaseType -> assertThat(
          "should not coerce " + fromBaseType + " to " + toBaseType,
          coercer.coerce(getInstance(fromBaseType), getType(toBaseType)),
          is(Result.failure())
      ));
    }
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceJsonObjectToStruct() {
    // Given:
    final SqlType structType = SqlTypes.struct()
        .field("FOO", SqlTypes.BIGINT)
        .field("NULL", SqlTypes.BIGINT)
        .build();

    // When:
    final Result result = coercer.coerce(new JsonObject().put("FOO", 12), structType);

    // Then:
    assertThat("", !result.failed());
    final Struct coerced = ((Optional<Struct>) result.value()).get();
    assertThat(coerced.get("FOO"), is(12L));
    assertThat(coerced.get("NULL"), is(nullValue()));
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceJsonObjectToNestedStruct() {
    // Given:
    final SqlType structType = SqlTypes.struct()
        .field("F1", SqlTypes.struct()
            .field("G1", SqlTypes.BIGINT)
            .field("NULL", SqlTypes.BIGINT)
            .build())
        .field("F2", SqlTypes.array(SqlTypes.STRING))
        .field("F3", SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING))
        .build();
    final JsonObject obj = new JsonObject()
        .put("F1", new JsonObject().put("G1", 12))
        .put("F2", new JsonArray().add("v1").add("v2"))
        .put("F3", new JsonObject().put("k1", "v1"));

    // When:
    final Result result = coercer.coerce(obj, structType);

    // Then:
    assertThat("", !result.failed());
    final Struct coerced = ((Optional<Struct>) result.value()).get();

    final Struct innerStruct = (Struct) coerced.get("F1");
    assertThat(innerStruct.get("G1"), is(12L));
    assertThat(innerStruct.get("NULL"), is(nullValue()));

    final List<?> innerList = (List<?>) coerced.get("F2");
    assertThat(innerList, is(ImmutableList.of("v1", "v2")));

    final Map<?, ?> innerMap = (Map<?, ?>) coerced.get("F3");
    assertThat(innerMap, is(ImmutableMap.of("k1", "v1")));
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceJsonObjectToNestedMap() {
    // Given:
    final SqlType mapType = SqlTypes.map(SqlTypes.STRING, SqlTypes.struct()
        .field("F1", SqlTypes.BIGINT)
        .field("F2", SqlTypes.STRING)
        .build());
    final JsonObject obj = new JsonObject()
        .put("k1", new JsonObject().put("F1", 1).put("F2", "foo"))
        .put("k2", new JsonObject().put("F1", 2))
        .put("k3", new JsonObject())
        .putNull("k4");

    // When:
    final Result result = coercer.coerce(obj, mapType);

    // Then:
    assertThat("", !result.failed());
    final Map<?, ?> coerced = ((Optional<Map<?, ?>>) result.value()).get();
    assertThat(((Struct) coerced.get("k1")).get("F1"), is(1L));
    assertThat(((Struct) coerced.get("k1")).get("F2"), is("foo"));
    assertThat(((Struct) coerced.get("k2")).get("F1"), is(2L));
    assertThat(((Struct) coerced.get("k2")).get("F2"), is(nullValue()));
    assertThat(((Struct) coerced.get("k3")).get("F1"), is(nullValue()));
    assertThat(((Struct) coerced.get("k3")).get("F2"), is(nullValue()));
    assertThat(coerced.get("k4"), is(nullValue()));
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceJsonArrayToNestedArray() {
    // Given:
    final SqlType arrayType = SqlTypes.array(SqlTypes.struct()
        .field("F1", SqlTypes.BIGINT)
        .field("F2", SqlTypes.STRING)
        .build());
    final JsonArray array = new JsonArray()
        .add(new JsonObject().put("F1", 1).put("F2", "foo"))
        .add(new JsonObject().put("F1", 2))
        .add(new JsonObject())
        .addNull();

    // When:
    final Result result = coercer.coerce(array, arrayType);

    // Then:
    assertThat("", !result.failed());
    final List<?> coerced = ((Optional<List<?>>) result.value()).get();
    assertThat(((Struct) coerced.get(0)).get("F1"), is(1L));
    assertThat(((Struct) coerced.get(0)).get("F2"), is("foo"));
    assertThat(((Struct) coerced.get(1)).get("F1"), is(2L));
    assertThat(((Struct) coerced.get(1)).get("F2"), is(nullValue()));
    assertThat(((Struct) coerced.get(2)).get("F1"), is(nullValue()));
    assertThat(((Struct) coerced.get(2)).get("F2"), is(nullValue()));
    assertThat(coerced.get(3), is(nullValue()));
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceJsonStructToNestedDecimal() {
    // Given:
    final SqlType structType = SqlTypes.struct()
        .field("INT", SqlTypes.decimal(2, 1))
        .field("LONG", SqlTypes.decimal(2, 1))
        .field("STRING", SqlTypes.decimal(2, 1))
        .field("DOUBLE", SqlTypes.decimal(2, 1))
        .build();
    final JsonObject obj = new JsonObject()
        .put("INT", 1)
        .put("LONG", 1L);
    if (coercer.isAllowCastStringAndDoubleToDecimal()) {
      obj.put("STRING", "1.1")
          .put("DOUBLE", 1.2);
    }

    // When:
    final Result result = coercer.coerce(obj, structType);

    // Then:
    assertThat("", !result.failed());
    final Struct coerced = ((Optional<Struct>) result.value()).get();
    assertThat(coerced.get("INT"), is(new BigDecimal("1.0")));
    assertThat(coerced.get("LONG"), is(new BigDecimal("1.0")));
    if (coercer.isAllowCastStringAndDoubleToDecimal()) {
      assertThat(coerced.get("STRING"), is(new BigDecimal("1.1")));
      assertThat(coerced.get("DOUBLE"), is(new BigDecimal("1.2")));
    }
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceJsonStructWithCaseInsensitiveFields() {
    // Given:
    final SqlType structType = SqlTypes.struct()
        .field("FOO", SqlTypes.BIGINT)
        .field("foo", SqlTypes.STRING)
        .field("bar", SqlTypes.STRING)
        .build();
    final JsonObject obj = new JsonObject()
        .put("foo", 12)
        .put("`foo`", "v1")
        .put("\"bar\"", "v2");

    // When:
    final Result result = coercer.coerce(obj, structType);

    // Then:
    assertThat("", !result.failed());
    final Struct coerced = ((Optional<Struct>) result.value()).get();
    assertThat(coerced.get("FOO"), is(12L));
    assertThat(coerced.get("foo"), is("v1"));
    assertThat(coerced.get("bar"), is("v2"));
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceNestedJsonStructWithCaseInsensitiveFields() {
    // Given:
    final SqlType structType = SqlTypes.struct().field("F1", SqlTypes.struct()
        .field("FOO", SqlTypes.BIGINT)
        .field("foo", SqlTypes.STRING)
        .field("bar", SqlTypes.STRING)
        .build()).build();
    final JsonObject obj = new JsonObject().put("F1", new JsonObject()
        .put("foo", 12)
        .put("`foo`", "v1")
        .put("\"bar\"", "v2"));

    // When:
    final Result result = coercer.coerce(obj, structType);

    // Then:
    assertThat("", !result.failed());
    final Struct coerced = ((Optional<Struct>) result.value()).get();
    final Struct innerStruct = ((Struct) coerced.get("F1"));
    assertThat(innerStruct.get("FOO"), is(12L));
    assertThat(innerStruct.get("foo"), is("v1"));
    assertThat(innerStruct.get("bar"), is("v2"));
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  @Test
  public void shouldCoerceNestedJsonMapWithCaseSensitiveKeys() {
    // Given:
    final SqlType structType = SqlTypes.struct()
        .field("F1", SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT))
        .build();
    final JsonObject obj = new JsonObject().put("F1", new JsonObject().put("foo", 12));

    // When:
    final Result result = coercer.coerce(obj, structType);

    // Then:
    assertThat("", !result.failed());
    final Struct coerced = ((Optional<Struct>) result.value()).get();
    final Map<?, ?> innerMap = ((Map<?, ?>) coerced.get("F1"));
    assertThat(innerMap.get("foo"), is(12L));
    assertThat(innerMap.get("FOO"), is(nullValue()));
  }

  private static boolean coercionShouldBeSupported(
      final SqlBaseType fromBaseType,
      final SqlBaseType toBaseType
  ) {
    return fromBaseType.canImplicitlyCast(toBaseType);
  }

  private static List<SqlBaseType> supportedTypes() {
    return ImmutableList.copyOf(SqlBaseType.values());
  }

  private static Object getInstance(final SqlBaseType baseType) {
    final Object instance = INSTANCES.get(baseType);
    assertThat(
        "invalid test: need instance for base type:" + baseType,
        instance,
        is(notNullValue())
    );
    return instance;
  }

  private static SqlType getType(final SqlBaseType baseType) {
    final SqlType type = TYPES.get(baseType);
    assertThat(
        "invalid test: need type for base type:" + baseType,
        type,
        is(notNullValue())
    );
    return type;
  }
}