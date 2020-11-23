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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.inOrder;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.DataException;
import io.confluent.ksql.types.KsqlStruct.Accessor;
import io.confluent.ksql.types.KsqlStruct.Builder;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class KsqlStructTest {

  private static final SqlStruct SCHEMA = SqlTypes.struct()
      .field("f0", SqlTypes.BIGINT)
      .field("f1", SqlTypes.BOOLEAN)
      .field("f2", SqlTypes.STRING)
      .field("f4", SqlTypes.INTEGER)
      .build();

  @Mock
  private BiConsumer<? super Field, ? super Optional<?>> consumer;

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            KsqlStruct.builder(SCHEMA).build(),
            KsqlStruct.builder(SCHEMA).build(),
            KsqlStruct.builder(SCHEMA).set(0, (Object) null).build(),
            KsqlStruct.builder(SCHEMA).set(0, Optional.empty()).build()
        )
        .addEqualityGroup(
            KsqlStruct.builder(SCHEMA).set(1, true).build(),
            KsqlStruct.builder(SCHEMA).set(1, Optional.of(true)).build()
        )
        .addEqualityGroup(
            KsqlStruct.builder(SqlTypes.struct().build())
        )
        .testEquals();
  }

  @Test
  public void shouldHandleNullsAndValuesInToString() {
    // Given:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set(0, 12345L)
        .build();

    // When:
    final String text = struct.toString();

    // Then:
    assertThat(text, is("Struct(12345,null,null,null)"));
  }

  @Test
  public void shouldHandleExplicitNulls() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set("f0", (Object) null)
        .set("f1", Optional.empty())
        .set(2, (Object) null)
        .set(3, Optional.empty())
        .build();

    // Then:
    for (int i = 0; i < SCHEMA.fields().size(); i++) {
      assertThat(struct.get(i), is(Optional.empty()));
    }
  }

  @Test
  public void shouldHandleImplicitNulls() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .build();

    // Then:
    for (int i = 0; i < SCHEMA.fields().size(); i++) {
      assertThat(struct.get(i), is(Optional.empty()));
    }
  }

  @Test
  public void shouldBuildStructByFieldName() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set("f1", Optional.of(true))
        .set("f0", Optional.of(10L))
        .build();

    // Then:
    assertThat(struct.get(0), is(Optional.of(10L)));
    assertThat(struct.get(1), is(Optional.of(true)));
  }

  @Test
  public void shouldBuildStructByFieldIndex() {
    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set(1, Optional.of(true))
        .set(0, Optional.of(10L))
        .build();

    // Then:
    assertThat(struct.get(0), is(Optional.of(10L)));
    assertThat(struct.get(1), is(Optional.of(true)));
  }

  @Test
  public void shouldThrowOnSettingUnknownFieldName() {
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
  public void shouldThrowOnSettingTooLargeAFieldIndex() {
    // Given:
    final int indexOverflow = SCHEMA.fields().size();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(SCHEMA)
            .set(indexOverflow, Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid field index: " + indexOverflow));
  }

  @Test
  public void shouldThrowOnSettingNegativeFieldIndex() {
    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> KsqlStruct.builder(SCHEMA)
            .set(-1, Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid field index: -1"));
  }

  @Test
  public void shouldThrowOnGettingUnknownFieldName() {
    // Given:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA).build();
    final Accessor accessor = struct.accessor(SCHEMA);

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> accessor.get("??")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown field: ??"));
  }

  @Test
  public void shouldThrowOnGettingTooLargeAFieldIndex() {
    // Given:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA).build();
    final int indexOverflow = SCHEMA.fields().size();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> struct.get(indexOverflow)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid field index: " + indexOverflow));
  }

  @Test
  public void shouldThrowOnGettingNegativeFieldIndex() {
    // Given:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA).build();

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> struct.get(-1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid field index: -1"));
  }

  @Test
  public void shouldNotThrowOnValueWrongTypeAsTypeCheckingEverySetIsTooExpensive() {
    // Given:
    final Optional<String> value = Optional.of("field is BIGINT and the value is STRING");

    // When:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set("f0", value)
        .build();

    // Then (did not throw):
    assertThat(struct.get(0), is(value));
  }

  @Test
  public void shouldVisitFieldsInOrder() {
    // Given:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set("f0", Optional.of(10L))
        .set("f1", Optional.of(true))
        .build();

    final Accessor accessor = struct.accessor(SCHEMA);

    // When:
    accessor.forEach(consumer);

    // Then:
    final InOrder inOrder = inOrder(consumer);
    for (int i = 0; i < SCHEMA.fields().size(); i++) {
      inOrder.verify(consumer).accept(SCHEMA.fields().get(i), struct.get(i));
    }
  }

  @Test
  public void shouldConvertToMutableBuilder() {
    // Given:
    final Builder builder = KsqlStruct.builder(SCHEMA)
        .set(0, Optional.of(10L))
        .set(1, Optional.of(true))
        .build()
        .asBuilder(SCHEMA);

    // When:
    final KsqlStruct struct = builder
        .set(1, false)
        .set(2, "x")
        .build();

    // Then:
    assertThat(struct.get(0), is(Optional.of(10L)));
    assertThat(struct.get(1), is(Optional.of(false)));
    assertThat(struct.get(2), is(Optional.of("x")));
    assertThat(struct.get(3), is(Optional.empty()));
  }

  @Test
  public void shouldGetFieldByIndex() {
    // Given:
    final KsqlStruct struct = KsqlStruct.builder(SCHEMA)
        .set("f0", Optional.of(10L))
        .set("f1", Optional.of(true))
        .build();

    // Then:
    assertThat(struct.get(1), is(Optional.of(true)));
    assertThat(struct.get(2), is(Optional.empty()));
  }

  @Test
  public void shouldGetFieldByName() {
    // Given:
    final Accessor accessor = KsqlStruct.builder(SCHEMA)
        .set(0, Optional.of(10L))
        .set(1, Optional.of(true))
        .build()
        .accessor(SCHEMA);

    // Then:
    assertThat(accessor.get("f1"), is(Optional.of(true)));
    assertThat(accessor.get("f2"), is(Optional.empty()));
  }
}