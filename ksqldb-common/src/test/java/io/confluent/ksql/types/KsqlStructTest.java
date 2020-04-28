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

package io.confluent.ksql.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.inOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.KsqlStruct;
import io.confluent.ksql.schema.ksql.types.Field;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.DataException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlStructTest {

  private static final SqlStruct SCHEMA = SqlTypes.struct()
      .field("f0", SqlTypes.BIGINT)
      .field(Field.of("v1", SqlTypes.BOOLEAN))
      .build();

  @Mock
  private BiConsumer<? super Field, ? super Optional<?>> consumer;

  @Test
  public void shouldHandleExplicitNulls() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set("f0", Optional.empty())
        .build();

    // Then:
    assertThat(struct.values(), contains(Optional.empty(), Optional.empty()));
  }

  @Test
  public void shouldHandleImplicitNulls() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .build();

    // Then:
    assertThat(struct.values(), contains(Optional.empty(), Optional.empty()));
  }

  @Test
  public void shouldThrowFieldNotKnown() {
    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(SCHEMA)
            .set("??", Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown field: ??"));
  }

  @Test
  public void shouldThrowIfValueWrongType() {
    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(SCHEMA)
            .set("f0", Optional.of("field is BIGINT, so won't like this"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected BIGINT, got STRING"));
  }

  @Test
  public void shouldBuildStruct() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set("f0", Optional.of(10L))
        .set("v1", Optional.of(true))
        .build();

    // Then:
    assertThat(struct.values(), contains(Optional.of(10L), Optional.of(true)));
  }

  @Test
  public void shouldVisitFieldsInOrder() {
    // Given:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set("f0", Optional.of(10L))
        .set("v1", Optional.of(true))
        .build();

    // When:
    struct.forEach(consumer);

    // Then:
    final InOrder inOrder = inOrder(consumer);
    inOrder.verify(consumer).accept(
        struct.schema().fields().get(0),
        struct.values().get(0)
    );
    inOrder.verify(consumer).accept(
        struct.schema().fields().get(1),
        struct.values().get(1)
    );
  }

  @Test
  public void shouldThrowIfStructValueNotStruct() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.struct().build())
        .build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema)
            .set("f0", Optional.of(10L))
            .build()
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected STRUCT, got BIGINT"
    ));
  }

  @Test
  public void shouldThrowIfStructValueHasMismatchedSchema() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.BIGINT)
        .build();

    final SqlStruct mismatching = SqlTypes.struct()
        .field("f0", SqlTypes.DOUBLE)
        .build();

    final KsqlStruct value = KsqlStruct.builder(mismatching)
        .set("f0", Optional.of(10.0D))
        .build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).validateValue(schema, value)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected STRUCT<`f0` BIGINT>, got STRUCT<`f0` DOUBLE>"
    ));
  }

  @Test
  public void shouldNotThrowWhenValidatingNullStructValue() {
    // Given:
    final SqlStruct schema = SqlTypes.struct().field("f0", SqlTypes.BIGINT).build();

    // When:
    KsqlStruct.builder(schema).set("f0", Optional.empty()).build();
  }

  @Test
  public void shouldValidateStructValue() {
    // Given:
    final SqlStruct schema = SqlTypes.struct().field("f0", SqlTypes.BIGINT).build();

    // When:
    KsqlStruct.builder(schema).set("f0", Optional.of(10L)).build();
  }

  @Test
  public void shouldThrowIfNotArrayValueList() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.array(SqlTypes.BIGINT))
        .build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).set("f0", 10L).build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected ARRAY, got BIGINT"));
  }

  @Test
  public void shouldThrowIfAnyElementInArrayValueNotElementType() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.array(SqlTypes.BIGINT))
        .build();

    // Where:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).set("f0", ImmutableList.of(11L, 9)).build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("ARRAY element 2: Expected BIGINT, got INT"));
  }

  @Test
  public void shouldNotThrowWhenValidatingNullArrayValue() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.array(SqlTypes.BIGINT))
        .build();

    // When:
    KsqlStruct.builder(schema).set("f0", Optional.empty()).build();
  }

  @Test
  public void shouldValidateArrayValue() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.array(SqlTypes.BIGINT))
        .build();

    // When:
    KsqlStruct.builder(schema).set("f0", Arrays.asList(19L, null)).build();
  }

  @Test
  public void shouldThrowIfMapValueNotMap() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.map(SqlTypes.BIGINT))
        .build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).set("f0", 10L).build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected MAP, got BIGINT"));
  }

  @Test
  public void shouldThrowIfAnyKeyInMapValueNotKeyType() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.map(SqlTypes.BIGINT))
        .build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).set("f0", ImmutableMap.of("first", 9L, 2, 9L)).build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("MAP key: Expected STRING, got INT"));
  }

  @Test
  public void shouldThrowIfAnyValueInMapValueNotValueType() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.map(SqlTypes.BIGINT))
        .build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).set("f0", ImmutableMap.of("1", 11L, "2", 9)).build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("MAP value for key '2': Expected BIGINT, got INT"));
  }

  @Test
  public void shouldNotThrowWhenValidatingNullMapValue() {
    // Given:
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlTypes.map(SqlTypes.BIGINT))
        .build();

    // When:
    KsqlStruct.builder(schema).set("f0", Optional.empty()).build();
  }

  @Test
  public void shouldValidateMapValue() {
    // Given:
    final SqlMap map = SqlTypes.map(SqlTypes.BIGINT);
    final Map<Object, Object> mapWithNull = new HashMap<>();
    mapWithNull.put("valid", 44L);
    mapWithNull.put(null, 44L);
    mapWithNull.put("v", null);

    final SqlStruct schema = SqlTypes.struct().field("f0", map).build();

    // When:
    KsqlStruct.builder(schema).set("f0", mapWithNull).build();
  }


  @Test
  public void shouldThrowIfDecimalValueNotDecimal() {
    // Given:
    final SqlStruct schema = SqlTypes.struct().field("f0", SqlTypes.decimal(4, 1)).build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).set("f0", 10L).build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected DECIMAL, got BIGINT"));
  }

  @Test
  public void shouldThrowIfDecimalValueHasWrongPrecision() {
    // Given:
    final SqlStruct schema = SqlTypes.struct().field("f0", SqlTypes.decimal(4, 1)).build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).set("f0", new BigDecimal("1234.5")).build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected DECIMAL(4, 1), got precision 5"));
  }

  @Test
  public void shouldThrowIfDecimalValueHasWrongScale() {
    // Given:
    final SqlStruct schema = SqlTypes.struct().field("f0", SqlTypes.decimal(4, 1)).build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).set("f0", new BigDecimal("12.50")).build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected DECIMAL(4, 1), got scale 2"));
  }

  @Test
  public void shouldNotThrowWhenValidatingNullDecimalValue() {
    // Given:
    final SqlStruct schema = SqlTypes.struct().field("f0", SqlTypes.decimal(4, 1)).build();

    // When:
    KsqlStruct.builder(schema).set("f0", Optional.empty()).build();
  }

  @Test
  public void shouldValidateDecimalValue() {
    // Given:
    final SqlStruct schema = SqlTypes.struct().field("f0", SqlTypes.decimal(4, 1)).build();

    // When:
    KsqlStruct.builder(schema).set("f0", new BigDecimal("123.0")).build();
  }

  @Test
  public void shoudlValidatePrimitiveTypes() {
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .field("f1", SqlPrimitiveType.of(SqlBaseType.INTEGER))
        .field("f2", SqlPrimitiveType.of(SqlBaseType.BIGINT))
        .field("f3", SqlPrimitiveType.of(SqlBaseType.DOUBLE))
        .field("f4", SqlPrimitiveType.of(SqlBaseType.STRING))
        .build();

    KsqlStruct.builder(schema).set("f0", true).build();
    KsqlStruct.builder(schema).set("f1", 19).build();
    KsqlStruct.builder(schema).set("f2", 33L).build();
    KsqlStruct.builder(schema).set("f3", 45.0D).build();
    KsqlStruct.builder(schema).set("f4", "").build();
  }

  @SuppressWarnings("UnnecessaryBoxing")
  @Test
  public void shouldValidateBoxedTypes() {
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .field("f1", SqlPrimitiveType.of(SqlBaseType.INTEGER))
        .field("f2", SqlPrimitiveType.of(SqlBaseType.BIGINT))
        .field("f3", SqlPrimitiveType.of(SqlBaseType.DOUBLE))
        .build();

    KsqlStruct.builder(schema).set("f0", Boolean.FALSE).build();
    KsqlStruct.builder(schema).set("f1", Integer.valueOf(19)).build();
    KsqlStruct.builder(schema).set("f2", Long.valueOf(33L)).build();
    KsqlStruct.builder(schema).set("f3", Double.valueOf(45.0D)).build();
  }

  @Test
  public void shouldValidateNullValue() {
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .build();

    KsqlStruct.builder(schema).set("f0", Optional.empty()).build();
  }

  @Test
  public void shouldFailValidationForWrongType() {
    //Given
    final SqlStruct schema = SqlTypes.struct()
        .field("f0", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(schema).set("f0", 10).build()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected BOOLEAN, got INT"));
  }
}