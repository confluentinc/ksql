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

package io.confluent.ksql;

import static io.confluent.ksql.GenericKey.genericKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.GenericKey.Builder;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@SuppressWarnings("UnstableApiUsage")
@RunWith(Enclosed.class)
public class GenericKeyTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("vic"), SqlTypes.BOOLEAN)
      .keyColumn(ColumnName.of("rohan"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("andy"), SqlTypes.BIGINT)
      .build();

  public static final class BuilderTest {

    @Test
    public void shouldBuild() {
      // Given:
      final Builder builder = GenericKey.builder(SCHEMA);

      // When:
      final GenericKey result = builder
          .append(true)
          .append(11)
          .build();

      // Then:
      assertThat(result, is(genericKey(true, 11)));
    }

    @Test
    public void shouldBuildAll() {
      // Given:
      final Builder builder = GenericKey.builder(SCHEMA);

      // When:
      final GenericKey result = builder
          .appendAll(ImmutableList.of(true, 11))
          .build();

      // Then:
      assertThat(result, is(genericKey(true, 11)));
    }

    @Test
    public void shouldThrowIfToFewColumnsSet() {
      // Given:
      final Builder builder = GenericKey.builder(SCHEMA)
          .append(true);

      // When:
      assertThrows(
          IllegalStateException.class,
          builder::build
      );
    }

    @Test
    public void shouldThrowIfToManyColumnsSet() {
      // Given:
      final Builder builder = GenericKey.builder(SCHEMA)
          .append(true)
          .append(10)
          .append("extra");

      // When:
      assertThrows(
          IllegalStateException.class,
          builder::build
      );
    }

    @Test
    public void shouldPopulateAllWithNulls() {
      // Given:
      final Builder builder = GenericKey.builder(SCHEMA);

      // When:
      final GenericKey result = builder
          .appendNulls()
          .build();

      // Then:
      assertThat(result, is(genericKey(null, null)));
    }

    @Test
    public void shouldPopulateRemainingWithNulls() {
      // Given:
      final Builder builder = GenericKey.builder(SCHEMA)
          .append(true);

      // When:
      final GenericKey result = builder
          .appendNulls()
          .build();

      // Then:
      assertThat(result, is(genericKey(true, null)));
    }

    @Test
    public void shouldNotPerformTypeValidationAsItIsUnnecessaryAndExpensive() {
      // Given:
      final Builder builder = GenericKey.builder(SCHEMA);

      // When:
      final GenericKey result = builder
          .append("not a boolean")
          .append("not an int")
          .build();

      // Then:
      assertThat(result, is(genericKey("not a boolean", "not an int")));
    }
 }

  public static final class GenKeyTest {

    private GenericKey key;

    @Before
    public void setUp() {
      key = GenericKey.genericKey("a", 11);
    }

    @Test
    public void shouldImplementHashCodeAndEquals() {
      new EqualsTester()
          .addEqualityGroup(
              GenericKey.genericKey("a", true, 11),
              GenericKey.genericKey("a", true, 11)
          )
          .addEqualityGroup(
              GenericKey.genericKey("a", true, 12)
          )
          .testEquals();
    }

    @Test
    public void shouldReturnSize() {
      assertThat(key.size(), is(2));
    }

    @Test
    public void shouldReturnColumnByIndex() {
      assertThat(key.get(0), is("a"));
      assertThat(key.get(1), is(11));
    }

    @Test
    public void shouldReturnValues() {
      assertThat(key.values(), is(ImmutableList.of("a", 11)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldReturnImmutableValues() {
      // Given:
      final List<Object> values = (List) key.values();

      // Then:
      assertThrows(
          UnsupportedOperationException.class,
          () -> values.add(1)
      );
    }

    @Test
    public void shouldReturnColumnsInToString() {
      assertThat(key.toString(), is("['a', 11]"));
    }
  }
}