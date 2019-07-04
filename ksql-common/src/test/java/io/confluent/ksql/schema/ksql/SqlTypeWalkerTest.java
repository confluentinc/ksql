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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.confluent.ksql.schema.ksql.SqlTypeWalker.Visitor;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SqlTypeWalkerTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SqlTypeWalker.Visitor<String, Integer> visitor;

  @Test
  public void shouldVisitBoolean() {
    // Given:
    final SqlPrimitiveType type = SqlTypes.BOOLEAN;
    when(visitor.visitBoolean(any())).thenReturn("Expected");

    // When:
    final String result = SqlTypeWalker.visit(type, visitor);

    // Then:
    verify(visitor).visitBoolean(same(type));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitInt() {
    // Given:
    final SqlPrimitiveType type = SqlTypes.INTEGER;
    when(visitor.visitInt(any())).thenReturn("Expected");

    // When:
    final String result = SqlTypeWalker.visit(type, visitor);

    // Then:
    verify(visitor).visitInt(same(type));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitBigInt() {
    // Given:
    final SqlPrimitiveType type = SqlTypes.BIGINT;
    when(visitor.visitBigInt(any())).thenReturn("Expected");

    // When:
    final String result = SqlTypeWalker.visit(type, visitor);

    // Then:
    verify(visitor).visitBigInt(same(type));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitDouble() {
    // Given:
    final SqlPrimitiveType type = SqlTypes.DOUBLE;
    when(visitor.visitDouble(any())).thenReturn("Expected");

    // When:
    final String result = SqlTypeWalker.visit(type, visitor);

    // Then:
    verify(visitor).visitDouble(same(type));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitString() {
    // Given:
    final SqlPrimitiveType type = SqlTypes.STRING;
    when(visitor.visitString(any())).thenReturn("Expected");

    // When:
    final String result = SqlTypeWalker.visit(type, visitor);

    // Then:
    verify(visitor).visitString(same(type));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitDecimal() {
    // Given:
    final SqlDecimal type = SqlTypes.decimal(10, 2);
    when(visitor.visitDecimal(any())).thenReturn("Expected");

    // When:
    final String result = SqlTypeWalker.visit(type, visitor);

    // Then:
    verify(visitor).visitDecimal(same(type));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitArray() {
    // Given:
    final SqlArray type = SqlTypes.array(SqlTypes.BIGINT);

    when(visitor.visitBigInt(any())).thenReturn("Expected-element");
    when(visitor.visitArray(any(), any())).thenReturn("Expected");

    // When:
    final String result = SqlTypeWalker.visit(type, visitor);

    // Then:
    verify(visitor).visitBigInt(same(SqlTypes.BIGINT));
    verify(visitor).visitArray(same(type), eq("Expected-element"));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitMap() {
    // Given:
    final SqlMap type = SqlTypes.map(SqlTypes.INTEGER);

    when(visitor.visitInt(any())).thenReturn("Expected-value");
    when(visitor.visitMap(any(), any())).thenReturn("Expected");

    // When:
    final String result = SqlTypeWalker.visit(type, visitor);

    // Then:
    verify(visitor).visitInt(same(SqlTypes.INTEGER));
    verify(visitor).visitMap(same(type), eq("Expected-value"));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitStruct() {
    // Given:
    final SqlStruct type = SqlTypes
        .struct()
        .field("0", SqlTypes.DOUBLE)
        .field("1", SqlTypes.INTEGER)
        .build();

    when(visitor.visitDouble(any())).thenReturn("0");
    when(visitor.visitInt(any())).thenReturn("1");
    when(visitor.visitField(any(), any())).thenAnswer(inv -> {
      final int fieldName = Integer.parseInt(inv.<Field>getArgument(0).getName());
      final int expectedArg = Integer.parseInt(inv.getArgument(1));
      assertThat(fieldName, is(expectedArg));
      return fieldName;
    });
    when(visitor.visitStruct(any(), any())).thenReturn("Expected");

    // When:
    final String result = SqlTypeWalker.visit(type, visitor);

    // Then:
    verify(visitor).visitDouble(same(SqlTypes.DOUBLE));
    verify(visitor).visitInt(same(SqlTypes.INTEGER));
    verify(visitor).visitStruct(same(type), eq(ImmutableList.of(0, 1)));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitPrimitives() {
    // Given:
    visitor = new Visitor<String, Integer>() {
      @Override
      public String visitPrimitive(final SqlPrimitiveType type) {
        return "Expected";
      }
    };

    primitiveTypes().forEach(type -> {

      // When:
      final String result = SqlTypeWalker.visit(type, visitor);

      // Then:
      assertThat(result, is("Expected"));
    });
  }

  @Test
  public void shouldVisitAll() {
    // Given:
    visitor = new Visitor<String, Integer>() {
      @Override
      public String visitType(final SqlType type) {
        return "Expected";
      }
    };

    allTypes().forEach(type -> {

      // When:
      final String result = SqlTypeWalker.visit(type, visitor);

      // Then:
      assertThat(result, is("Expected"));
    });
  }

  @Test
  public void shouldThrowByDefaultFromNonStructured() {
    // Given:
    visitor = new Visitor<String, Integer>() {
    };

    nonStructuredTypes().forEach(type -> {

      try {
        // When:
        SqlTypeWalker.visit(type, visitor);

        fail();

      } catch (final UnsupportedOperationException e) {
        // Then:
        assertThat(e.getMessage(), is("Unsupported sql type: " + type));
      }
    });
  }

  @Test
  public void shouldThrowByDefaultFromStructured() {
    // Given:
    visitor = new Visitor<String, Integer>() {
      @Override
      public String visitPrimitive(final SqlPrimitiveType type) {
        return null;
      }
    };

    structuredTypes().forEach(type -> {

      try {
        // When:
        SqlTypeWalker.visit(type, visitor);

        fail();

      } catch (final UnsupportedOperationException e) {
        // Then:
        assertThat(e.getMessage(), is("Unsupported sql type: " + type));
      }
    });
  }

  @Test
  public void shouldThrowOnUnknownType() {
    // Given:
    final SqlBaseType unknownType = mock(SqlBaseType.class, "bob");
    final SqlType type = mock(SqlType.class);
    when(type.baseType()).thenReturn(unknownType);

    // Then:
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Unsupported schema type: bob");

    // When:
    SqlTypeWalker.visit(type, visitor);
  }

  public static Stream<SqlType> primitiveTypes() {
    return Stream.of(
        SqlTypes.BOOLEAN,
        SqlTypes.INTEGER,
        SqlTypes.BIGINT,
        SqlTypes.DOUBLE,
        SqlTypes.STRING
    );
  }

  @SuppressWarnings("UnstableApiUsage")
  private static Stream<SqlType> nonStructuredTypes() {
    return Streams.concat(
        primitiveTypes(),
        Stream.of(SqlTypes.decimal(10, 2))
    );
  }

  private static Stream<SqlType> structuredTypes() {
    return Stream.of(
        SqlTypes.array(SqlTypes.BIGINT),
        SqlTypes.map(SqlTypes.STRING),
        SqlTypes
            .struct()
            .field("f0", SqlTypes.BIGINT)
            .build()
    );
  }

  @SuppressWarnings("UnstableApiUsage")
  private static Stream<SqlType> allTypes() {
    return Streams.concat(
        nonStructuredTypes(),
        structuredTypes()
    );
  }
}