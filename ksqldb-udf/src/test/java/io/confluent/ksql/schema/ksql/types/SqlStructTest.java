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

package io.confluent.ksql.schema.ksql.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.utils.DataException;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.Optional;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@SuppressWarnings("UnstableApiUsage")
@RunWith(Enclosed.class)
public class SqlStructTest {

  public static class StructTest {

    @Test
    public void shouldImplementHashCodeAndEqualsProperly() {
      new EqualsTester()
          .addEqualityGroup(
              SqlStruct.builder().field("f0", SqlTypes.INTEGER).build(),
              SqlStruct.builder().field("f0", SqlTypes.INTEGER).build()
          )
          .addEqualityGroup(
              SqlStruct.builder().field("f1", SqlTypes.INTEGER).build()
          )
          .addEqualityGroup(
              SqlStruct.builder().field("f0", SqlTypes.BIGINT).build()
          )
          .testEquals();
    }

    @Test
    public void shouldReturnSqlType() {
      assertThat(SqlStruct.builder().field("f0", SqlTypes.BIGINT).build().baseType(),
          is(SqlBaseType.STRUCT));
    }

    @Test
    public void shouldReturnFields() {
      // When:
      final SqlStruct struct = SqlStruct.builder()
          .field("f0", SqlTypes.BIGINT)
          .field("f1", SqlTypes.DOUBLE)
          .build();

      // Then:
      assertThat(struct.fields(), contains(
          new Field("f0", SqlTypes.BIGINT, 0),
          new Field("f1", SqlTypes.DOUBLE, 1)
      ));
      assertFalse(struct.unionType().isPresent());
    }

    @Test
    public void shouldRecognizeOneOfUnionType() {
      // When:
      final SqlStruct struct = SqlStruct.builder()
          .field("io.confluent.connect.json.OneOf.field.0", SqlTypes.BIGINT)
          .field("io.confluent.connect.json.OneOf.field.1", SqlTypes.DOUBLE)
          .build();

      // Then:
      assertTrue(struct.unionType().isPresent());
      assertEquals(SqlStruct.UnionType.ONE_OF_TYPE, struct.unionType().get());
    }

    @Test
    public void shouldRecognizeGeneralizedUnionType() {
      // When:
      final SqlStruct struct = SqlStruct.builder()
          .field("connect_union_field_0", SqlTypes.BIGINT)
          .field("connect_union_field_1", SqlTypes.DOUBLE)
          .build();

      // Then:
      assertTrue(struct.unionType().isPresent());
      assertEquals(SqlStruct.UnionType.GENERALIZED_TYPE, struct.unionType().get());
    }

    @Test
    public void shouldNotThrowIfNoFields() {
      SqlStruct struct = SqlStruct.builder().build();
      assertFalse(struct.unionType().isPresent());
    }

    @Test
    public void shouldThrowOnDuplicateFieldName() {
      // When:
      final DataException e = assertThrows(
          DataException.class,
          () -> SqlStruct.builder()
              .field("F0", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
              .field("F0", SqlPrimitiveType.of(SqlBaseType.INTEGER))
      );

      // Then:
      assertThat(e.getMessage(), containsString(
          "Duplicate field names found in STRUCT: '`F0` BOOLEAN' and '`F0` INTEGER'"
      ));
    }

    @Test
    public void shouldNotThrowIfTwoFieldsHaveSameNameButDifferentCase() {
      // When:
      final SqlStruct struct = SqlStruct.builder()
          .field("f0", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
          .field("F0", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
          .build();

      // Then: did not throw
      assertThat(struct.fields(), contains(
          new Field("f0", SqlTypes.BOOLEAN, 0),
          new Field("F0", SqlTypes.BOOLEAN, 1)
      ));
      assertFalse(struct.unionType().isPresent());
    }

    @Test
    public void shouldImplementToString() {
      // Given:
      final SqlStruct struct = SqlStruct.builder()
          .field("f0", SqlTypes.BIGINT)
          .field("F1", SqlTypes.array(SqlTypes.DOUBLE))
          .build();

      // When:
      final String sql = struct.toString();

      // Then:
      assertThat(sql, is(
          "STRUCT<"
              + "`f0` " + SqlTypes.BIGINT
              + ", `F1` " + SqlTypes.array(SqlTypes.DOUBLE)
              + ">"
      ));
      assertFalse(struct.unionType().isPresent());
    }

    @Test
    public void shouldImplementToStringForEmptyStruct() {
      // Given:
      final SqlStruct emptyStruct = SqlStruct.builder().build();

      // When:
      final String sql = emptyStruct.toString();

      // Then:
      assertThat(sql, is("STRUCT< >"));
      assertFalse(emptyStruct.unionType().isPresent());
    }

    @Test
    public void shouldImplementToStringWithReservedWordHandling() {
      // Given:
      final SqlStruct struct = SqlStruct.builder()
          .field("f0", SqlTypes.BIGINT)
          .field("F1", SqlTypes.array(SqlTypes.DOUBLE))
          .build();

      final FormatOptions formatOptions = FormatOptions.of(word -> word.equals("F1"));

      // When:
      final String sql = struct.toString(formatOptions);

      // Then:
      assertThat(sql, is(
          "STRUCT<"
              + "f0 " + SqlTypes.BIGINT
              + ", `F1` " + SqlTypes.array(SqlTypes.DOUBLE)
              + ">"
      ));
      assertFalse(struct.unionType().isPresent());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void shouldGetKnownField() {
      // Given:
      final SqlStruct schema = SqlTypes.struct()
          .field("f0", SqlTypes.BIGINT)
          .build();

      // When:
      final Optional<SqlStruct.Field> result = schema.field("f0");

      // Then:
      assertThat(result, is(not(Optional.empty())));
      assertThat(result.get().name(), is("f0"));
      assertFalse(schema.unionType().isPresent());
    }

    @Test
    public void shouldReturnEmptyForUnknownField() {
      // Given:
      final SqlStruct schema = SqlTypes.struct()
          .field("f0", SqlTypes.BIGINT)
          .build();

      // When:
      final Optional<SqlStruct.Field> result = schema.field("unknown");

      // Then:
      assertThat(result, is(Optional.empty()));
      assertFalse(schema.unionType().isPresent());
    }

    @Test
    public void shouldReturnEmptyForFieldIfWrongCase() {
      // Given:
      final SqlStruct schema = SqlTypes.struct()
          .field("f0", SqlTypes.BIGINT)
          .build();

      // When:
      final Optional<SqlStruct.Field> result = schema.field("F0");

      // Then:
      assertThat(result, is(Optional.empty()));
      assertFalse(schema.unionType().isPresent());
    }
  }

  public static class FieldTest {

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
              new Field("someName", SqlTypes.INTEGER, 2),
              new Field("someName", SqlTypes.INTEGER, 2)
          )
          .addEqualityGroup(
              new Field("someName".toUpperCase(), SqlTypes.INTEGER, 2)
          )
          .addEqualityGroup(
              new Field("different", SqlTypes.INTEGER, 2)
          )
          .addEqualityGroup(
              new Field("someName", SqlTypes.DOUBLE, 2)
          )
          .addEqualityGroup(
              new Field("someName", SqlTypes.INTEGER, 1)
          )
          .testEquals();
    }

    @Test
    public void shouldReturnName() {
      assertThat(new Field("SomeName", SqlTypes.BOOLEAN, 0).name(),
          is("SomeName"));
    }

    @Test
    public void shouldReturnType() {
      assertThat(new Field("SomeName", SqlTypes.BOOLEAN, 0).type(), is(SqlTypes.BOOLEAN));
    }

    @Test
    public void shouldToString() {
      assertThat(new Field("SomeName", SqlTypes.BOOLEAN, 0).toString(),
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
      assertThat(new Field("not-reserved", SqlTypes.BIGINT, 0).toString(options),
          is("not-reserved BIGINT"));

      assertThat(new Field("reserved", SqlTypes.BIGINT, 0).toString(options),
          is("`reserved` BIGINT"));
    }

    @Test
    public void shouldThrowIfNameIsEmpty() {
      // When:
      final IllegalArgumentException e = assertThrows(
          IllegalArgumentException.class,
          () -> new Field("", SqlTypes.STRING, 0)
      );

      // Then:
      assertThat(e.getMessage(), containsString("name is empty"));
    }

    @Test
    public void shouldThrowIfNameIsNotTrimmed() {
      // When:
      final IllegalArgumentException e = assertThrows(
          IllegalArgumentException.class,
          () -> new Field(" bar ", SqlTypes.STRING, 0)
      );

      // Then:
      assertThat(e.getMessage(), containsString("name is not trimmed"));
    }
  }
}