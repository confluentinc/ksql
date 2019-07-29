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
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Test;

public class FieldTest {

  @Test
  public void shouldThrowNPE() {
    new NullPointerTester()
        .setDefault(SqlType.class, SqlTypes.BIGINT)
        .testAllPublicStaticMethods(Field.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnLeadingWhiteSpace() {
    Field.of("  name_with_leading_white_space", SqlTypes.STRING);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnTrailingWhiteSpace() {
    Field.of("name_with_leading_white_space  ", SqlTypes.STRING);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnEmptyName() {
    Field.of("", SqlTypes.STRING);
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
        .addEqualityGroup(
            Field.of("someSource", "someName", SqlTypes.INTEGER),
            Field.of("someSource", "someName", SqlTypes.INTEGER)
        )
        .testEquals();
  }

  @Test
  public void shouldReturnName() {
    assertThat(Field.of("SomeName", SqlTypes.BOOLEAN).name(),
        is("SomeName"));

    assertThat(Field.of("SomeSource", "SomeName", SqlTypes.BOOLEAN).name(),
        is("SomeName"));
  }

  @Test
  public void shouldReturnFullName() {
    assertThat(Field.of("SomeName", SqlTypes.BOOLEAN).fullName(),
        is("SomeName"));

    assertThat(Field.of("SomeSource", "SomeName", SqlTypes.BOOLEAN).fullName(),
        is("SomeSource.SomeName"));
  }

  @Test
  public void shouldReturnSource() {
    assertThat(Field.of("SomeName", SqlTypes.BOOLEAN).source(),
        is(Optional.empty()));

    assertThat(Field.of("SomeSource", "SomeName", SqlTypes.BOOLEAN).source(),
        is(Optional.of("SomeSource")));
  }

  @Test
  public void shouldReturnType() {
    assertThat(Field.of("SomeName", SqlTypes.BOOLEAN).type(), is(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldToString() {
    assertThat(Field.of("SomeName", SqlTypes.BOOLEAN).toString(),
        is("`SomeName` BOOLEAN"));

    assertThat(Field.of("SomeSource", "SomeName", SqlTypes.INTEGER).toString(),
        is("`SomeSource.SomeName` INTEGER"));
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

    assertThat(Field.of("reserved", "word", SqlTypes.DOUBLE).toString(options),
        is("reserved.word DOUBLE"));

    assertThat(Field.of("source", "word", SqlTypes.STRING).toString(options),
        is("source.word STRING"));

    final SqlStruct struct = SqlTypes.struct()
        .field("reserved", SqlTypes.BIGINT)
        .field("other", SqlTypes.BIGINT)
        .build();

    assertThat(Field.of("reserved", "name", struct).toString(options),
        is("`reserved.name` STRUCT<`reserved` BIGINT, other BIGINT>"));
  }
}