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
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ColumnTest {

  private static final SourceName SOME_SOURCE = SourceName.of("SomeSource");
  private static final ColumnName SOME_NAME = ColumnName.of("SomeName");
  private static final ColumnName SOME_OHTER_NAME = ColumnName.of("SOMENAME");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldThrowNPE() {
    new NullPointerTester()
        .setDefault(SqlType.class, SqlTypes.BIGINT)
        .setDefault(ColumnName.class, SOME_NAME)
        .setDefault(SourceName.class, SOME_SOURCE)
        .setDefault(ColumnRef.class, ColumnRef.of(SOME_SOURCE, SOME_NAME))
        .testAllPublicStaticMethods(Column.class);
  }

  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            Column.of(SOME_NAME, SqlTypes.INTEGER),
            Column.of(SOME_NAME, SqlTypes.INTEGER)
        )
        .addEqualityGroup(
            Column.of(SOME_OHTER_NAME, SqlTypes.INTEGER)
        )
        .addEqualityGroup(
            Column.of(SOME_NAME, SqlTypes.DOUBLE)
        )
        .addEqualityGroup(
            Column.of(SOME_SOURCE, SOME_NAME, SqlTypes.INTEGER),
            Column.of(SOME_SOURCE, SOME_NAME, SqlTypes.INTEGER)
        )
        .testEquals();
  }

  @Test
  public void shouldReturnName() {
    assertThat(Column.of(SOME_NAME, SqlTypes.BOOLEAN).name(),
        is(SOME_NAME));

    assertThat(Column.of(SOME_SOURCE, SOME_NAME, SqlTypes.BOOLEAN).name(),
        is(SOME_NAME));
  }

  @Test
  public void shouldReturnType() {
    assertThat(Column.of(SOME_NAME, SqlTypes.BOOLEAN).type(), is(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldToString() {
    assertThat(Column.of(SOME_NAME, SqlTypes.BOOLEAN).toString(),
        is("`SomeName` BOOLEAN"));

    assertThat(Column.of(SOME_SOURCE, SOME_NAME, SqlTypes.INTEGER).toString(),
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
    assertThat(Column.of(ColumnName.of("not-reserved"), SqlTypes.BIGINT).toString(options),
        is("not-reserved BIGINT"));

    assertThat(Column.of(ColumnName.of("reserved"), SqlTypes.BIGINT).toString(options),
        is("`reserved` BIGINT"));

    assertThat(Column.of(SourceName.of("reserved"), ColumnName.of("word"), SqlTypes.DOUBLE).toString(options),
        is("`reserved`.`word` DOUBLE"));

    assertThat(Column.of(SourceName.of("source"), ColumnName.of("word"), SqlTypes.STRING).toString(options),
        is("source.`word` STRING"));
  }
}