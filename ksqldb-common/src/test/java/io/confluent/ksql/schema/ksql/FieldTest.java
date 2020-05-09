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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.schema.ksql.types.Field;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class FieldTest {

  @Test
  public void shouldThrowNPE() {
    new NullPointerTester()
        .setDefault(SqlType.class, SqlTypes.BIGINT)
        .setDefault(String.class, "field0")
        .testAllPublicStaticMethods(Field.class);
  }

  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            Field.of("someName", SqlTypes.INTEGER),
            Field.of("someName", SqlTypes.INTEGER)
        )
        .addEqualityGroup(
            Field.of("someName".toUpperCase(), SqlTypes.INTEGER)
        )
        .addEqualityGroup(
            Field.of("different", SqlTypes.INTEGER)
        )
        .addEqualityGroup(
            Field.of("someName", SqlTypes.DOUBLE)
        )
        .testEquals();
  }

  @Test
  public void shouldReturnName() {
    assertThat(Field.of("SomeName", SqlTypes.BOOLEAN).name(),
        is("SomeName"));
  }

  @Test
  public void shouldReturnType() {
    assertThat(Field.of("SomeName", SqlTypes.BOOLEAN).type(), is(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldToString() {
    assertThat(Field.of("SomeName", SqlTypes.BOOLEAN).toString(),
        is("`SomeName` BOOLEAN"));
  }

  @Test
  public void shouldToStringWithReservedWords() {
    // Given:
    final FormatOptions options = FormatOptions.of(
        identifier -> identifier.equals("reserved")
            || identifier.equals("word")
            || identifier.equals("reserved.name")
    );

    // Then:
    assertThat(Field.of("not-reserved", SqlTypes.BIGINT).toString(options),
        is("not-reserved BIGINT"));

    assertThat(Field.of("reserved", SqlTypes.BIGINT).toString(options),
        is("`reserved` BIGINT"));
  }

  @Test
  public void shouldThrowIfNameIsEmpty() {
    // When:
    final IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> Field.of("", SqlTypes.STRING)
    );

    // Then:
    assertThat(e.getMessage(), containsString("name is empty"));
  }

  @Test
  public void shouldThrowIfNameIsNotTrimmed() {
    // When:
    final IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class,
        () -> Field.of(" bar ", SqlTypes.STRING)
    );

    // Then:
    assertThat(e.getMessage(), containsString("name is not trimmed"));
  }
}