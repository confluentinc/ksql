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

import static io.confluent.ksql.schema.ksql.Column.Namespace.KEY;
import static io.confluent.ksql.schema.ksql.Column.Namespace.META;
import static io.confluent.ksql.schema.ksql.Column.Namespace.VALUE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BOOLEAN;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.DOUBLE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.INTEGER;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ColumnTest {

  private static final SourceName SOME_SOURCE = SourceName.of("SomeSource");
  private static final ColumnName SOME_NAME = ColumnName.of("SomeName");
  private static final ColumnName SOME_OHTER_NAME = ColumnName.of("SOMENAME");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowNPE() {
    new NullPointerTester()
        .setDefault(SqlType.class, BIGINT)
        .setDefault(ColumnName.class, SOME_NAME)
        .setDefault(ColumnRef.class, ColumnRef.of(SOME_NAME))
        .testAllPublicStaticMethods(Column.class);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            Column.of(SOME_NAME, INTEGER, VALUE, 10),
            Column.of(SOME_NAME, INTEGER, VALUE, 10)
        )
        .addEqualityGroup(
            Column.of(SOME_OHTER_NAME, INTEGER, VALUE, 10)
        )
        .addEqualityGroup(
            Column.of(SOME_NAME, DOUBLE, VALUE, 10)
        )
        .addEqualityGroup(
            Column.of(SOME_NAME, INTEGER, KEY, 10)
        )
        .addEqualityGroup(
            Column.of(SOME_NAME, INTEGER, VALUE, 5)
        )
        .testEquals();
  }

  @Test
  public void shouldReturnName() {
    assertThat(Column.of(SOME_NAME, BOOLEAN, KEY, 0).name(),
        is(SOME_NAME));

    assertThat(Column.of(SOME_NAME, BOOLEAN, VALUE, 1).name(),
        is(SOME_NAME));
  }

  @Test
  public void shouldReturnType() {
    assertThat(Column.of(SOME_NAME, BOOLEAN, META, 1).type(), is(BOOLEAN));
  }

  @Test
  public void shouldReturnNamespace() {
    assertThat(Column.of(SOME_NAME, BOOLEAN, KEY, 10)
        .namespace(), is(KEY));
  }

  @Test
  public void shouldReturnIndex() {
    assertThat(Column.of(SOME_NAME, BOOLEAN, KEY, 10)
        .index(), is(10));
  }

  @Test
  public void shouldToString() {
    assertThat(Column.of(SOME_NAME, INTEGER, VALUE, 10).toString(),
        is("`SomeName` INTEGER"));

    assertThat(Column.of(SOME_NAME, INTEGER, KEY, 10).toString(),
        is("`SomeName` INTEGER KEY"));

    assertThat(Column.of(SOME_NAME, INTEGER, META, 10).toString(),
        is("`SomeName` INTEGER META"));
  }

  @Test
  public void shouldToStringWithReservedWords() {
    // Given:
    final FormatOptions options = FormatOptions.of(
        identifier -> identifier.equals("reserved")
            || identifier.equals("word")
    );

    // Then:
    assertThat(Column
            .of(ColumnName.of("not-reserved"), BIGINT, VALUE, 0)
            .toString(options),
        is("not-reserved BIGINT"));

    assertThat(Column
            .of(ColumnName.of("reserved"), BIGINT, VALUE, 0)
            .toString(options),
        is("`reserved` BIGINT"));

    assertThat(Column
            .of(ColumnName.of("word"), DOUBLE, VALUE, 0)
            .toString(options),
        is("`word` DOUBLE"));

    assertThat(Column
            .of(ColumnName.of("word"), STRING, VALUE, 0)
            .toString(options),
        is("`word` STRING"));
  }
}