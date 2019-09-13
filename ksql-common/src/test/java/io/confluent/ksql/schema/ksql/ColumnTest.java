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

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ColumnTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldThrowNPE() {
    new NullPointerTester()
        .setDefault(SqlType.class, SqlTypes.BIGINT)
        .setDefault(String.class, "field0")
        .testAllPublicStaticMethods(Column.class);
  }

  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            Column.of("someName", SqlTypes.INTEGER),
            Column.of("someName", SqlTypes.INTEGER)
        )
        .addEqualityGroup(
            Column.of("someName".toUpperCase(), SqlTypes.INTEGER)
        )
        .addEqualityGroup(
            Column.of("someName", SqlTypes.DOUBLE)
        )
        .addEqualityGroup(
            Column.of("someSource", "someName", SqlTypes.INTEGER),
            Column.of("someSource", "someName", SqlTypes.INTEGER)
        )
        .testEquals();
  }

  @Test
  public void shouldReturnName() {
    assertThat(Column.of("SomeName", SqlTypes.BOOLEAN).name(),
        is("SomeName"));

    assertThat(Column.of("SomeSource", "SomeName", SqlTypes.BOOLEAN).name(),
        is("SomeName"));
  }

  @Test
  public void shouldReturnFullName() {
    assertThat(Column.of("SomeName", SqlTypes.BOOLEAN).fullName(),
        is("SomeName"));

    assertThat(Column.of("SomeSource", "SomeName", SqlTypes.BOOLEAN).fullName(),
        is("SomeSource.SomeName"));
  }

  @Test
  public void shouldReturnType() {
    assertThat(Column.of("SomeName", SqlTypes.BOOLEAN).type(), is(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldToString() {
    assertThat(Column.of("SomeName", SqlTypes.BOOLEAN).toString(),
        is("`SomeName` BOOLEAN"));

    assertThat(Column.of("SomeSource", "SomeName", SqlTypes.INTEGER).toString(),
        is("`SomeSource`.`SomeName` INTEGER"));
  }

  @Test
  public void shouldToStringWithReservedWords() {
    // Given:
    final FormatOptions options = FormatOptions.of(
        identifier -> identifier.equals("reserved")
            || identifier.equals("word")
    );

    // Then:
    assertThat(Column.of("not-reserved", SqlTypes.BIGINT).toString(options),
        is("not-reserved BIGINT"));

    assertThat(Column.of("reserved", SqlTypes.BIGINT).toString(options),
        is("`reserved` BIGINT"));

    assertThat(Column.of("reserved", "word", SqlTypes.DOUBLE).toString(options),
        is("`reserved`.`word` DOUBLE"));

    assertThat(Column.of("source", "word", SqlTypes.STRING).toString(options),
        is("source.`word` STRING"));
  }

  @Test
  public void shouldThrowIfNameIsEmpty() {
    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("name is empty");

    // When:
    Column.of("", SqlTypes.STRING);
  }

  @Test
  public void shouldThrowIfNameIsNotTrimmed() {
    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("name is not trimmed");

    // When:
    Column.of(" bar ", SqlTypes.STRING);
  }

  @Test
  public void shouldThrowIfSourceIsEmpty() {
    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("source is empty");

    // When:
    Column.of("", "foo", SqlTypes.STRING);
  }

  @Test
  public void shouldThrowIfSourceIsNotTrimmed() {
    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("source is not trimmed");

    // When:
    Column.of(" bar ", "foo", SqlTypes.STRING);
  }
}