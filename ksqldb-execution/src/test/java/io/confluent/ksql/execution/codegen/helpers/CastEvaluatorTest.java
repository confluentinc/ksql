/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.codegen.helpers;

import static io.confluent.ksql.schema.ksql.types.SqlBaseType.ARRAY;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.BYTES;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.DATE;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.MAP;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.STRUCT;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.TIME;
import static io.confluent.ksql.schema.ksql.types.SqlBaseType.TIMESTAMP;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.codegen.CodeGenTestUtil;
import io.confluent.ksql.execution.codegen.CodeGenTestUtil.Evaluator;
import io.confluent.ksql.execution.codegen.CodeGenUtil;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercerTest;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercerTest.LaxValueCoercionTest.LaxOnly;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlValueCoercer.Result;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Builder;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.MockitoRule;


/**
 * Cast is also tested by `cast.json`.
 */
@RunWith(Enclosed.class)
public class CastEvaluatorTest {

  private static final String INNER_CODE = "val0";

  @RunWith(Parameterized.class)
  public static class CombinationTest {

    @Parameterized.Parameters(name = "{0} -> {1}")
    public static Collection<SqlBaseType[]> testCases() {
      return Arrays.stream(SqlBaseType.values())
          .flatMap(from1 -> Arrays.stream(SqlBaseType.values())
              .map(targetType -> new SqlBaseType[]{from1, targetType}))
          .collect(Collectors.toList());
    }

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private KsqlConfig config;

    private final SqlType from;
    private final SqlType to;
    private final Class<?> returnJavaType;
    private final boolean supportedCast;

    @SuppressWarnings("unused") // Invoked by Parameterized
    public CombinationTest(final SqlBaseType from, final SqlBaseType to) {
      this(
          TypeInstances.typeInstanceFor(from),
          TypeInstances.typeInstanceFor(to),
          SupportedCasts.supported(from, to)
      );
    }

    CombinationTest(
        final SqlType from,
        final SqlType to,
        final boolean supportedCast
    ) {
      this.from = requireNonNull(from, "from");
      this.to = requireNonNull(to, "to");
      this.returnJavaType = SchemaConverters.sqlToJavaConverter().toJavaType(to);
      this.supportedCast = supportedCast;
    }

    @Test
    public void shouldThrowOnUnsupported() {
      if (!supportedCast) {
        assertUnsupported(from, to, config);
      }
    }

    @Test
    public void shouldEvalCodeWithNonNullArgument() {
      if (supportedCast) {
        // Given:
        final Object argument = InstanceInstances.instanceFor(from, to);

        // When:
        final Object result = eval(from, to, config, argument);

        // Then:
        assertThat("return type mismatch", result, instanceOf(returnJavaType));
      }
    }

    @Test
    public void shouldEvalCodeWithNullArgument() {
      if (supportedCast) {
        // Given:
        final Matcher<Object> expected = returnJavaType.equals(String.class)
            && !from.equals(SqlTypes.STRING)
            && !from.equals(SqlTypes.TIME)
            && !from.equals(SqlTypes.DATE)
            && !from.equals(SqlTypes.TIMESTAMP)
            && !from.baseType().equals(SqlBaseType.DECIMAL)
            && !config.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE)
            ? is("null")
            : is(nullValue());

        // When:
        final Object result = eval(from, to, config, null);

        // Then:
        assertThat("null should return null", result, expected);
      }
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static final class DoubleTest {

    @Mock
    private KsqlConfig config;

    @Test
    public void shouldNotCastPositiveInfinite() {
      // Given:
      final Evaluator evaluator = cookCode(SqlTypes.STRING, SqlTypes.DOUBLE, config);
      // When:
      final Exception exception = assertThrows(
          NumberFormatException.class,
          () -> evaluator.rawEvaluate(INNER_CODE, "Infinity")
      );

      // Then:
      assertThat(exception.getMessage(), containsString("Invalid double value: Infinity"));
    }

    @Test
    public void shouldNotCastNegativeInfinite() {
      // Given:
      final Evaluator evaluator = cookCode(SqlTypes.STRING, SqlTypes.DOUBLE, config);
      // When:
      final Exception exception = assertThrows(
          NumberFormatException.class,
          () -> evaluator.rawEvaluate(INNER_CODE, "-Infinity")
      );

      // Then:
      assertThat(exception.getMessage(), containsString("Invalid double value: -Infinity"));
    }

    @Test
    public void shouldNotCastNaN() {
      // Given:
      final Evaluator evaluator = cookCode(SqlTypes.STRING, SqlTypes.DOUBLE, config);
      // When:
      final Exception exception = assertThrows(
          NumberFormatException.class,
          () -> evaluator.rawEvaluate(INNER_CODE, "NaN")
      );

      // Then:
      assertThat(exception.getMessage(), containsString("Invalid double value: NaN"));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static final class TimeTest {

    @Mock
    private KsqlConfig config;

    @Test
    public void shouldNotCastIncorrectlyFormattedString() {
      // Given:
      final Evaluator evaluator = cookCode(SqlTypes.STRING, SqlTypes.TIME, config);
      // When:
      final Exception exception = assertThrows(
          KsqlException.class,
          () -> evaluator.rawEvaluate(INNER_CODE, "woof")
      );

      // Then:
      assertThat(exception.getMessage(), containsString("Required format is: \"HH:mm:ss.SSS\""));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static final class DateTest {

    @Mock
    private KsqlConfig config;

    @Test
    public void shouldNotCastIncorrectlyFormattedString() {
      // Given:
      final Evaluator evaluator = cookCode(SqlTypes.STRING, SqlTypes.DATE, config);
      // When:
      final Exception exception = assertThrows(
          KsqlException.class,
          () -> evaluator.rawEvaluate(INNER_CODE, "woof")
      );

      // Then:
      assertThat(exception.getMessage(), containsString("Required format is: \"yyyy-MM-dd\""));
    }

    @Test
    public void shouldCastDateToTimestamp() throws Exception {
      // Given:
      final Evaluator evaluator = cookCode(SqlTypes.DATE, SqlTypes.TIMESTAMP, config);

      // Then:
      assertThat(evaluator.rawEvaluate(INNER_CODE, new Date(864000000)), is(new Timestamp(864000000)));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static final class TimestampTest {

    @Mock
    private KsqlConfig config;

    @Test
    public void shouldNotCastIncorrectlyFormattedString() {
      // Given:
      final Evaluator evaluator = cookCode(SqlTypes.STRING, SqlTypes.TIMESTAMP, config);
      // When:
      final Exception exception = assertThrows(
          KsqlException.class,
          () -> evaluator.rawEvaluate(INNER_CODE, "woof")
      );

      // Then:
      assertThat(exception.getMessage(), containsString("Required format is: \"yyyy-MM-dd'T'HH:mm:ss.SSS\", with an optional numeric"));
    }

    @Test
    public void shouldCastTimestampToDate() throws Exception {
      // Given:
      final Evaluator evaluator = cookCode(SqlTypes.TIMESTAMP, SqlTypes.DATE, config);

      // Then:
      assertThat(evaluator.rawEvaluate(INNER_CODE, new Timestamp(864033000)), is(new Date(864000000)));
    }

    @Test
    public void shouldCastTimestampToTime() throws Exception {
      // Given:
      final Evaluator evaluator = cookCode(SqlTypes.TIMESTAMP, SqlTypes.TIME, config);

      // Then:
      assertThat(evaluator.rawEvaluate(INNER_CODE, new Timestamp(864033000)), is(new Time(33000)));
    }
  }

  // Run CombinationTest again, for arrays with different element types:
  public static final class ArrayCombinationTest extends CombinationTest {

    public ArrayCombinationTest(final SqlBaseType from, final SqlBaseType to) {
      super(
          SqlTypes.array(TypeInstances.typeInstanceFor(from)),
          SqlTypes.array(TypeInstances.typeInstanceFor(to)),
          SupportedCasts.supported(from, to)
      );
    }
  }

  public static final class ArrayEvalTest {

    private final Function<Long, Integer> mapper = l -> l == null ? null : l.intValue();

    @Test
    public void shouldMapNull() {
      assertThat(CastEvaluator.castArray(null, mapper), is(nullValue()));
    }

    @Test
    public void shouldMapElements() {
      // Given:
      final List<Long> array = Arrays.asList(1L, 2L, 3L);

      // When:
      final List<Integer> result = CastEvaluator.castArray(array, mapper);

      // Then:
      assertThat(result, is(Arrays.asList(1, 2, 3)));
    }

    @Test
    public void shouldHandleNulls() {
      // Given:
      final List<Long> array = Arrays.asList(null, null);

      // When:
      final List<Integer> result = CastEvaluator.castArray(array, mapper);

      // Then:
      assertThat(result, is(Arrays.asList(null, null)));
    }
  }

  // Run CombinationTest again, for maps with different key types:
  public static final class MapKeyCombinationTest extends CombinationTest {

    public MapKeyCombinationTest(final SqlBaseType from, final SqlBaseType to) {
      super(
          SqlTypes.map(TypeInstances.typeInstanceFor(from), SqlTypes.STRING),
          SqlTypes.map(TypeInstances.typeInstanceFor(to), SqlTypes.STRING),
          SupportedCasts.supported(from, to)
      );
    }
  }

  // Run CombinationTest again, for maps with different value types:
  public static final class MapValueCombinationTest extends CombinationTest {

    public MapValueCombinationTest(final SqlBaseType from, final SqlBaseType to) {
      super(
          SqlTypes.map(SqlTypes.STRING, TypeInstances.typeInstanceFor(from)),
          SqlTypes.map(SqlTypes.STRING, TypeInstances.typeInstanceFor(to)),
          SupportedCasts.supported(from, to)
      );
    }
  }

  public static final class MapEvalTest {

    private final Function<Long, Integer> keyMapper = l -> l == null ? null : l.intValue();
    private final Function<Integer, Long> valMapper = i -> i == null ? null : i.longValue();

    @Test
    public void shouldMapNull() {
      assertThat(CastEvaluator.castMap(null, keyMapper, valMapper), is(nullValue()));
    }

    @Test
    public void shouldMapKeysAndValues() {
      // Given:
      final Map<Long, Integer> map = ImmutableMap.of(1L, 2, 3L, 4);

      // When:
      final Map<Integer, Long> result = CastEvaluator.castMap(map, keyMapper, valMapper);

      // Then:
      assertThat(result, is(ImmutableMap.of(1, 2L, 3, 4L)));
    }

    @Test
    public void shouldHandleNulls() {
      // Given:
      final Map<Long, Integer> map = new HashMap<>();
      map.put(null, null);

      // When:
      final Map<Integer, Long> result = CastEvaluator.castMap(map, keyMapper, valMapper);

      // Then:
      assertThat(result.keySet(), contains(nullValue()));
      assertThat(result.get(null), is(nullValue()));
    }
  }

  // Run CombinationTest again, for structs with different field types:
  public static final class StructCombinationTest extends CombinationTest {

    public StructCombinationTest(final SqlBaseType from, final SqlBaseType to) {
      super(
          SqlTypes.struct().field("a", TypeInstances.typeInstanceFor(from)).build(),
          SqlTypes.struct().field("a", TypeInstances.typeInstanceFor(to)).build(),
          SupportedCasts.supported(from, to)
      );
    }
  }

  public static final class StructEvalTest {

    private static final SqlStruct FROM = SqlTypes.struct()
        .field("a", SqlTypes.BIGINT)
        .build();

    private static final SqlStruct TO = SqlTypes.struct()
        .field("a", SqlTypes.INTEGER)
        .build();

    private static final Schema FROM_CONNECT_SCHEMA = SchemaConverters.sqlToConnectConverter()
        .toConnectSchema(FROM);

    private static final Schema TO_CONNECT_SCHEMA = SchemaConverters.sqlToConnectConverter()
        .toConnectSchema(TO);

    private final ImmutableMap<String, Function<Object, Object>> mappers = ImmutableMap
        .of("a", l -> l == null ? null : ((Long) l).intValue());

    @Test
    public void shouldMapNull() {
      assertThat(CastEvaluator.castStruct(null, mappers, TO_CONNECT_SCHEMA), is(nullValue()));
    }

    @Test
    public void shouldReturnStructWithCorrectSchema() {
      // Given:
      final Struct struct = (Struct) InstanceInstances.instanceFor(FROM, TO);

      // When:
      final Struct result = CastEvaluator.castStruct(struct, mappers, TO_CONNECT_SCHEMA);

      // Then:
      assertThat(result.schema(), is(TO_CONNECT_SCHEMA));
    }

    @Test
    public void shouldMapFields() {
      // Given:
      final Struct struct = (Struct) InstanceInstances.instanceFor(FROM, TO);

      // When:
      final Struct result = CastEvaluator.castStruct(struct, mappers, TO_CONNECT_SCHEMA);

      // Then:
      assertThat(result, is(new Struct(TO_CONNECT_SCHEMA).put("a", 99)));
    }

    @Test
    public void shouldHandleNullFieldValues() {
      // Given:
      final Struct struct = new Struct(FROM_CONNECT_SCHEMA);

      // When:
      final Struct result = CastEvaluator.castStruct(struct, mappers, TO_CONNECT_SCHEMA);

      // Then:
      assertThat(result.get("a"), is(nullValue()));
    }
  }

  // Tests covering different fields in source and target struct types:
  @RunWith(MockitoJUnitRunner.class)
  public static final class StructMismatchedFieldsTest {

    @Mock
    private KsqlConfig config;

    @Test
    public void shouldOnlyCopyFieldsThatExistInBothSides() {
      // Given:
      final SqlStruct from = SqlTypes.struct()
          .field("from", SqlTypes.BIGINT)
          .field("common1", SqlTypes.INTEGER)
          .field("common2", SqlTypes.STRING)
          .build();

      final SqlStruct to = SqlTypes.struct()
          .field("common1", SqlTypes.STRING)
          .field("common2", SqlTypes.INTEGER)
          .field("to", SqlTypes.BIGINT)
          .build();

      final Schema schema = SchemaConverters.sqlToConnectConverter().toConnectSchema(to);

      final Object argument = InstanceInstances.instanceFor(from, to);

      // When:
      final Object result = eval(from, to, config, argument);

      // Then:

      assertThat("type", result, instanceOf(Struct.class));
      final Struct struct = (Struct) result;
      assertThat("schema", struct.schema(), is(schema));
      assertThat("common1", struct.get("common1"), is("10"));
      assertThat("common2", struct.get("common2"), is(11));
      assertThat("to", struct.get("to"), is(nullValue()));
    }
  }

  // Run CombinationTest again, for nested structured types with different element types:
  public static final class NestedCombinationTest extends CombinationTest {

    public NestedCombinationTest(final SqlBaseType from, final SqlBaseType to) {
      super(
          SqlTypes.struct()
              .field("f0", SqlTypes.map(
                  SqlTypes.STRING,
                  SqlTypes.array(TypeInstances.typeInstanceFor(from))
              ))
              .field("f1", SqlTypes.struct()
                  .field("a", TypeInstances.typeInstanceFor(from))
                  .build()
              )
              .build(),
          SqlTypes.struct()
              .field("f0", SqlTypes.map(
                  SqlTypes.STRING,
                  SqlTypes.array(TypeInstances.typeInstanceFor(to))
              ))
              .field("f1", SqlTypes.struct()
                  .field("a", TypeInstances.typeInstanceFor(to))
                  .build()
              )
              .build(),
          SupportedCasts.supported(from, to)
      );
    }
  }

  @RunWith(Parameterized.class)
  public static final class CastShouldBeASuperSetOfLaxCoercionTest {

    private final Object value;
    private final SqlType from;
    private final SqlType to;
    private final Object expected;
    private final KsqlConfig config = mock(KsqlConfig.class);

    @Parameterized.Parameters(name = "{0} coerced to {1} should be {2}")
    public static Collection<Object[]> testCases() {
      return DefaultSqlValueCoercerTest.LaxValueCoercionTest.testCases().stream()
          // Ignore failure cases:
          .filter(a -> includedTestCase(a[2]))
          .collect(Collectors.toList());
    }

    public CastShouldBeASuperSetOfLaxCoercionTest(
        final Object value,
        final SqlType to,
        final Object expected
    ) {
      this.value = requireNonNull(value, "value");
      this.from = fromType(value);
      this.to = requireNonNull(to, "to");
      this.expected = LaxOnly.unwrap(expected).value().orElseThrow(IllegalStateException::new);
    }

    @Test
    public void shouldCastAnythingThatCanBeCoerced() {
      // When:
      final Object result = eval(from, to, config, value);

      // Then:
      assertThat(result, is(expected));
    }

    private static SqlType fromType(final Object value) {
      final SqlBaseType baseType = SchemaConverters.javaToSqlConverter()
          .toSqlType(value.getClass());

      switch (baseType) {
        case DECIMAL:
          final BigDecimal decimal = (BigDecimal) value;
          return DecimalUtil.fromValue(decimal);
        case ARRAY:
          return SqlTypes.array(subType((Collection<?>) value));
        case MAP:
          return SqlTypes.map(
              subType(((Map<?, ?>) value).keySet()),
              subType(((Map<?, ?>) value).values())
          );
        case STRUCT:
          final Builder builder = SqlTypes.struct();
          ((Struct) value).schema().fields().forEach(f -> builder.field(
              f.name(),
              SchemaConverters.connectToSqlConverter().toSqlType(f.schema())
          ));
          return builder.build();
        default:
          return TypeInstances.typeInstanceFor(baseType);
      }
    }

    private static SqlType subType(final Collection<?> values) {
      return values.stream()
          .filter(Objects::nonNull)
          .findFirst()
          .map(CastShouldBeASuperSetOfLaxCoercionTest::fromType)
          .orElse(SqlTypes.INTEGER);
    }

    private static boolean includedTestCase(final Object expected) {
      final Result result = LaxOnly.unwrap(expected);
      if (Result.failure().equals(result)) {
        return false;
      }

      final Object value = result.value().orElse(null);
      if (value instanceof List && ((List<?>) value).isEmpty()) {
        return false;
      }

      if (value instanceof Map && ((Map<?, ?>) value).isEmpty()) {
        return false;
      }

      if (value instanceof Struct) {
        final Struct struct = (Struct) value;
        return !struct.schema().fields().stream()
            .allMatch(f -> struct.get(f) == null);
      }

      return true;
    }
  }

  public static final class MetaTest {

    @Test
    public void shouldFailIfNewSqlBaseTypeAdded() {
      final Set<SqlBaseType> allTypes = Arrays.stream(SqlBaseType.values())
          .collect(Collectors.toSet());

      assertThat(
          "This test will fail if a new base type is added to remind you to think about what"
              + "CASTs should be supported for the new type.",
          allTypes,
          is(ImmutableSet.of(
              SqlBaseType.BOOLEAN, SqlBaseType.INTEGER, SqlBaseType.BIGINT, SqlBaseType.DECIMAL,
              SqlBaseType.DOUBLE, SqlBaseType.STRING, SqlBaseType.ARRAY, MAP,
              SqlBaseType.STRUCT, SqlBaseType.TIME, SqlBaseType.DATE, SqlBaseType.TIMESTAMP,
              SqlBaseType.BYTES
          ))
      );
    }
  }

  private static Evaluator cookCode(
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    final String paramAccessor = CodeGenUtil.argumentAccessor(INNER_CODE, from);
    final String javaCode = CastEvaluator.generateCode(paramAccessor, from, to, config);

    final Class<?> toJavaType = SchemaConverters.sqlToJavaConverter()
        .toJavaType(to);

    return CodeGenTestUtil.cookCode(javaCode, toJavaType);
  }

  private static Object eval(
      final SqlType from,
      final SqlType to,
      final KsqlConfig config,
      final Object argument
  ) {
    final Map<String, Object> arguments = new HashMap<>();
    arguments.put("val0", argument);
    return cookCode(from, to, config).evaluate(arguments);
  }

  private static void assertUnsupported(
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> {
          final String paramAccessor = CodeGenUtil.argumentAccessor(INNER_CODE, from);
          CastEvaluator.generateCode(paramAccessor, from, to, config);
        }
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString("Cast of " + from + " to " + to + " is not supported")
    );
  }

  private static final class TypeInstances {

    private static final ImmutableMap<SqlBaseType, SqlType> TYPE_INSTANCES = ImmutableMap.
        <SqlBaseType, SqlType>builder()
        .put(SqlBaseType.BOOLEAN, SqlTypes.BOOLEAN)
        .put(SqlBaseType.INTEGER, SqlTypes.INTEGER)
        .put(SqlBaseType.BIGINT, SqlTypes.BIGINT)
        .put(SqlBaseType.DECIMAL, SqlTypes.decimal(4, 2))
        .put(SqlBaseType.DOUBLE, SqlTypes.DOUBLE)
        .put(SqlBaseType.STRING, SqlTypes.STRING)
        .put(SqlBaseType.ARRAY, SqlTypes.array(SqlTypes.BIGINT))
        .put(MAP, SqlTypes.map(SqlTypes.BIGINT, SqlTypes.STRING))
        .put(SqlBaseType.STRUCT, SqlTypes.struct()
            .field("Bob", SqlTypes.STRING)
            .build())
        .put(SqlBaseType.TIME, SqlTypes.TIME)
        .put(SqlBaseType.DATE, SqlTypes.DATE)
        .put(SqlBaseType.TIMESTAMP, SqlTypes.TIMESTAMP)
        .put(SqlBaseType.BYTES, SqlTypes.BYTES)
        .build();

    static SqlType typeInstanceFor(final SqlBaseType baseType) {
      final SqlType sqlType = TYPE_INSTANCES.get(baseType);
      assertThat(
          "Invalid test: missing type instance for " + baseType,
          sqlType,
          is(notNullValue())
      );
      return sqlType;
    }
  }

  private static final class InstanceInstances {

    private static final ImmutableMap<SqlBaseType, Object> INSTANCES = ImmutableMap.
        <SqlBaseType, Object>builder()
        .put(SqlBaseType.BOOLEAN, true)
        .put(SqlBaseType.INTEGER, 10)
        .put(SqlBaseType.BIGINT, 99L)
        .put(SqlBaseType.DECIMAL, new BigDecimal("12.01"))
        .put(SqlBaseType.DOUBLE, 34.98d)
        .put(SqlBaseType.STRING, "\t 11 \t")
        .put(SqlBaseType.TIME, new Time(500L))
        .put(SqlBaseType.DATE, new Date(500L))
        .put(SqlBaseType.TIMESTAMP, new Timestamp(500))
        .put(SqlBaseType.BYTES, ByteBuffer.wrap(new byte[] {123}))
        .build();

    @SuppressWarnings("fallthrough")
    static Object instanceFor(final SqlType type, final SqlType to) {
      switch (type.baseType()) {
        case ARRAY:
          final Object element = (to.baseType() == ARRAY)
              ? instanceFor(((SqlArray) type).getItemType(), ((SqlArray) to).getItemType())
              : instanceFor(((SqlArray) type).getItemType(), to);
          return ImmutableList.of(element);
        case MAP:
          if (to.baseType() == MAP) {
            final Object key = instanceFor(((SqlMap) type).getKeyType(), ((SqlMap) to).getKeyType());
            final Object value = instanceFor(((SqlMap) type).getValueType(), ((SqlMap) to).getValueType());
            return ImmutableMap.of(key, value);
          } else {
            final Object key = instanceFor(((SqlMap) type).getKeyType(), to);
            final Object value = instanceFor(((SqlMap) type).getValueType(), to);
            return ImmutableMap.of(key, value);
          }
        case STRUCT:
          final SqlStruct sqlStruct = (SqlStruct) type;
          final Struct struct = new Struct(
              SchemaConverters.sqlToConnectConverter().toConnectSchema(type));
          if (to.baseType() == STRUCT) {
            sqlStruct.fields()
                .forEach(field -> {
                  final Optional<Field> toField = ((SqlStruct) to).field(field.name());
                  struct.put(
                        field.name(),
                        instanceFor(field.type(), toField.isPresent() ? toField.get().type(): to));
                });
          } else {
            sqlStruct.fields()
                .forEach(field -> struct.put(field.name(), instanceFor(field.type(), to)));
          }

          return struct;
        case STRING:
          if (to.baseType() == TIMESTAMP) {
            return "2020-05-26T07:59:58.000";
          } else if (to.baseType() == TIME) {
            return "07:59:58";
          } else if (to.baseType() == DATE) {
            return "2020-05-26";
          } else if (to.baseType() == BYTES) {
            return "IQ==";
          }
          // Intentional fall through
        default:
          final Object instance = INSTANCES.get(type.baseType());
          assertThat(
              "Invalid test: missing instance for " + type.baseType(),
              instance,
              is(notNullValue())
          );
          return instance;
      }
    }
  }

  private static final class SupportedCasts {

    private static final ImmutableMap<SqlBaseType, ImmutableSet<SqlBaseType>> CODE =
        ImmutableMap.<SqlBaseType, ImmutableSet<SqlBaseType>>builder()
            .put(SqlBaseType.BOOLEAN, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.BOOLEAN)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.INTEGER, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.INTEGER)
                .add(SqlBaseType.BIGINT)
                .add(SqlBaseType.DECIMAL)
                .add(SqlBaseType.DOUBLE)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.BIGINT, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.INTEGER)
                .add(SqlBaseType.BIGINT)
                .add(SqlBaseType.DECIMAL)
                .add(SqlBaseType.DOUBLE)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.DECIMAL, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.INTEGER)
                .add(SqlBaseType.BIGINT)
                .add(SqlBaseType.DECIMAL)
                .add(SqlBaseType.DOUBLE)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.DOUBLE, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.INTEGER)
                .add(SqlBaseType.BIGINT)
                .add(SqlBaseType.DECIMAL)
                .add(SqlBaseType.DOUBLE)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.STRING, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.BOOLEAN)
                .add(SqlBaseType.INTEGER)
                .add(SqlBaseType.BIGINT)
                .add(SqlBaseType.DECIMAL)
                .add(SqlBaseType.DOUBLE)
                .add(SqlBaseType.STRING)
                .add(SqlBaseType.TIME)
                .add(SqlBaseType.DATE)
                .add(SqlBaseType.TIMESTAMP)
                .add(SqlBaseType.BYTES)
                .build())
            .put(SqlBaseType.ARRAY, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.ARRAY)
                .add(SqlBaseType.STRING)
                .build())
            .put(MAP, ImmutableSet.<SqlBaseType>builder()
                .add(MAP)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.STRUCT, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRUCT)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.TIME, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.TIME)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.DATE, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.DATE)
                .add(SqlBaseType.TIMESTAMP)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.TIMESTAMP, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.TIMESTAMP)
                .add(SqlBaseType.TIME)
                .add(SqlBaseType.DATE)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.BYTES, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.BYTES)
                .build())
            .build();

    private static boolean supported(final SqlBaseType from, final SqlBaseType to) {
      final ImmutableSet<SqlBaseType> supportedReturnTypes = CODE.get(from);
      assertThat(
          "Invalid Test: missing expected result for: " + from,
          supportedReturnTypes, is(notNullValue())
      );

      return supportedReturnTypes.contains(to);
    }
  }
}
