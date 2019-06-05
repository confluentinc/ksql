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

package io.confluent.ksql.schema.connect;

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
import io.confluent.ksql.schema.connect.SchemaWalker.Visitor;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaWalkerTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SchemaWalker.Visitor<String> visitor;

  @Test
  public void shouldVisitBoolean() {
    // Given:
    final Schema schema = Schema.OPTIONAL_BOOLEAN_SCHEMA;
    when(visitor.visitBoolean(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitBoolean(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitInt8() {
    // Given:
    final Schema schema = Schema.OPTIONAL_INT8_SCHEMA;
    when(visitor.visitInt8(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitInt8(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitInt16() {
    // Given:
    final Schema schema = Schema.OPTIONAL_INT16_SCHEMA;
    when(visitor.visitInt16(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitInt16(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitInt32() {
    // Given:
    final Schema schema = Schema.OPTIONAL_INT32_SCHEMA;
    when(visitor.visitInt32(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitInt32(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitInt64() {
    // Given:
    final Schema schema = Schema.OPTIONAL_INT64_SCHEMA;
    when(visitor.visitInt64(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitInt64(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitFloat32() {
    // Given:
    final Schema schema = Schema.OPTIONAL_FLOAT32_SCHEMA;
    when(visitor.visitFloat32(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitFloat32(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitFloat64() {
    // Given:
    final Schema schema = Schema.OPTIONAL_FLOAT64_SCHEMA;
    when(visitor.visitFloat64(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitFloat64(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitString() {
    // Given:
    final Schema schema = Schema.OPTIONAL_STRING_SCHEMA;
    when(visitor.visitString(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitString(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitBytes() {
    // Given:
    final Schema schema = Schema.OPTIONAL_BYTES_SCHEMA;
    when(visitor.visitBytes(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitBytes(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitArray() {
    // Given:
    final Schema schema = SchemaBuilder
        .array(Schema.INT64_SCHEMA)
        .build();

    when(visitor.visitInt64(any())).thenReturn("Expected-element");
    when(visitor.visitArray(any(), any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitInt64(same(schema.valueSchema()));
    verify(visitor).visitArray(same(schema), eq("Expected-element"));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitMap() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.INT32_SCHEMA)
        .build();

    when(visitor.visitBoolean(any())).thenReturn("Expected-key");
    when(visitor.visitInt32(any())).thenReturn("Expected-value");
    when(visitor.visitMap(any(), any(), any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitBoolean(same(schema.keySchema()));
    verify(visitor).visitInt32(same(schema.valueSchema()));
    verify(visitor).visitMap(same(schema), eq("Expected-key"), eq("Expected-value"));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitStruct() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("f0", Schema.FLOAT64_SCHEMA)
        .field("f1", Schema.INT32_SCHEMA)
        .build();

    when(visitor.visitFloat64(any())).thenReturn("Expected-f0");
    when(visitor.visitInt32(any())).thenReturn("Expected-f1");
    when(visitor.visitField(any(), any())).thenAnswer(inv ->
        inv.<Field>getArgument(0).name() + "->" + inv.getArgument(1));
    when(visitor.visitStruct(any(), any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitFloat64(same(schema.fields().get(0).schema()));
    verify(visitor).visitInt32(same(schema.fields().get(1).schema()));
    verify(visitor).visitStruct(same(schema),
        eq(ImmutableList.of("f0->Expected-f0", "f1->Expected-f1")));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitNonOptionals() {
    // Given:
    final Schema schema = Schema.BOOLEAN_SCHEMA;
    when(visitor.visitBoolean(any())).thenReturn("Expected");

    // When:
    final String result = SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitBoolean(same(schema));
    assertThat(result, is("Expected"));
  }

  @Test
  public void shouldVisitPrimitives() {
    // Given:
    visitor = new Visitor<String>() {
      @Override
      public String visitPrimitive(final Schema schema) {
        return "Expected";
      }
    };

    primitiveSchemas().forEach(schema -> {

      // When:
      final String result = SchemaWalker.visit(schema, visitor);

      // Then:
      assertThat(result, is("Expected"));
    });
  }

  @Test
  public void shouldVisitAll() {
    // Given:
    visitor = new Visitor<String>() {
      @Override
      public String visitSchema(final Schema schema) {
        return "Expected";
      }
    };

    allSchemas().forEach(schema -> {

      // When:
      final String result = SchemaWalker.visit(schema, visitor);

      // Then:
      assertThat(result, is("Expected"));
    });
  }

  @Test
  public void shouldThrowByDefaultFromAll() {
    // Given:
    visitor = new Visitor<String>() {
    };

    allSchemas().forEach(schema -> {

      try {
        // When:
        SchemaWalker.visit(schema, visitor);

        fail();

      } catch (final UnsupportedOperationException e) {
        // Then:
        assertThat(e.getMessage(), is("Unsupported schema type: " + schema));
      }
    });
  }

  @Test
  public void shouldThrowOnUnknownType() {
    // Given:
    final Type unknownType = mock(Type.class, "bob");
    final Schema schema = mock(Schema.class);
    when(schema.type()).thenReturn(unknownType);

    // Then:
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Unsupported schema type: bob");

    // When:
    SchemaWalker.visit(schema, visitor);
  }

  public static Stream<Schema> primitiveSchemas() {
    return Stream.of(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        Schema.OPTIONAL_INT8_SCHEMA,
        Schema.OPTIONAL_INT16_SCHEMA,
        Schema.OPTIONAL_INT32_SCHEMA,
        Schema.OPTIONAL_INT64_SCHEMA,
        Schema.OPTIONAL_FLOAT32_SCHEMA,
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        Schema.OPTIONAL_STRING_SCHEMA
    );
  }

  @SuppressWarnings("UnstableApiUsage")
  public static Stream<Schema> allSchemas() {
    return Streams.concat(
        primitiveSchemas(),
        Stream.of(
            Schema.OPTIONAL_BOOLEAN_SCHEMA,
            Schema.OPTIONAL_INT8_SCHEMA,
            Schema.OPTIONAL_INT16_SCHEMA,
            Schema.OPTIONAL_INT32_SCHEMA,
            Schema.OPTIONAL_INT64_SCHEMA,
            Schema.OPTIONAL_FLOAT32_SCHEMA,
            Schema.OPTIONAL_FLOAT64_SCHEMA,
            Schema.OPTIONAL_STRING_SCHEMA
        )
    );
  }
}