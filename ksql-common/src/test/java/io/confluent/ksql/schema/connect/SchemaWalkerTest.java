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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Streams;
import io.confluent.ksql.schema.connect.SchemaWalker.Visitor;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaWalkerTest {

  @Spy
  private SchemaWalker.Visitor visitor = new Visitor() {
  };

  @Test
  public void shouldVisitBoolean() {
    // Given:
    final Schema schema = Schema.OPTIONAL_BOOLEAN_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitBoolean(same(schema));
  }

  @Test
  public void shouldVisitInt8() {
    // Given:
    final Schema schema = Schema.OPTIONAL_INT8_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitInt8(same(schema));
  }

  @Test
  public void shouldVisitInt16() {
    // Given:
    final Schema schema = Schema.OPTIONAL_INT16_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitInt16(same(schema));
  }

  @Test
  public void shouldVisitInt32() {
    // Given:
    final Schema schema = Schema.OPTIONAL_INT32_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitInt32(same(schema));
  }

  @Test
  public void shouldVisitInt64() {
    // Given:
    final Schema schema = Schema.OPTIONAL_INT64_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitInt64(same(schema));
  }

  @Test
  public void shouldVisitFloat32() {
    // Given:
    final Schema schema = Schema.OPTIONAL_FLOAT32_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitFloat32(same(schema));
  }

  @Test
  public void shouldVisitFloat64() {
    // Given:
    final Schema schema = Schema.OPTIONAL_FLOAT64_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitFloat64(same(schema));
  }

  @Test
  public void shouldVisitString() {
    // Given:
    final Schema schema = Schema.OPTIONAL_STRING_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitString(same(schema));
  }

  @Test
  public void shouldVisitBytes() {
    // Given:
    final Schema schema = Schema.OPTIONAL_BYTES_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitBytes(same(schema));
  }

  @Test
  public void shouldVisitArray() {
    // Given:
    final Schema schema = SchemaBuilder
        .array(Schema.INT64_SCHEMA)
        .build();

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitArray(same(schema));
    verify(visitor).visitInt64(same(schema.valueSchema()));
  }

  @Test
  public void shouldNotVisitArrayElements() {
    // Given:
    final Schema schema = SchemaBuilder
        .array(Schema.INT64_SCHEMA)
        .build();

    when(visitor.visitArray(any())).thenReturn(false);

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor, never()).visitInt64(any());
  }

  @Test
  public void shouldVisitMap() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.INT32_SCHEMA)
        .build();

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitMap(same(schema));
    verify(visitor).visitBoolean(same(schema.keySchema()));
    verify(visitor).visitInt32(same(schema.valueSchema()));
  }

  @Test
  public void shouldVisitNotVisitMapKeyAndValue() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.INT32_SCHEMA)
        .build();

    when(visitor.visitMap(any())).thenReturn(false);

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor, never()).visitBoolean(any());
    verify(visitor, never()).visitInt32(any());
  }

  @Test
  public void shouldVisitStruct() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("f0", Schema.FLOAT64_SCHEMA)
        .field("f1", Schema.INT32_SCHEMA)
        .build();

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitStruct(same(schema));
    verify(visitor).visitFloat64(same(schema.fields().get(0).schema()));
    verify(visitor).visitInt32(same(schema.fields().get(1).schema()));
  }

  @Test
  public void shouldNotVisitStructFields() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("f0", Schema.FLOAT64_SCHEMA)
        .field("f1", Schema.INT32_SCHEMA)
        .build();

    when(visitor.visitStruct(any())).thenReturn(false);

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor, never()).visitFloat64(any());
    verify(visitor, never()).visitInt32(any());
  }

  @Test
  public void shouldVisitNonOptionals() {
    // Given:
    final Schema schema = Schema.BOOLEAN_SCHEMA;

    // When:
    SchemaWalker.visit(schema, visitor);

    // Then:
    verify(visitor).visitBoolean(same(schema));
  }

  @Test
  public void shouldVisitIntegers() {
    integerSchemas().forEach(schema -> {

      // Given:
      clearInvocations(visitor);

      // When:
      SchemaWalker.visit(schema, visitor);

      // Then:
      verify(visitor).visitInt(same(schema));
    });
  }

  @Test
  public void shouldVisitFloats() {
    floatSchemas().forEach(schema -> {

      // Given:
      clearInvocations(visitor);

      // When:
      SchemaWalker.visit(schema, visitor);

      // Then:
      verify(visitor).visitFloat(same(schema));
    });
  }

  @Test
  public void shouldVisitNumbers() {
    numberSchemas().forEach(schema -> {

      // Given:
      clearInvocations(visitor);

      // When:
      SchemaWalker.visit(schema, visitor);

      // Then:
      verify(visitor).visitNumber(same(schema));
    });
  }

  public static Stream<Schema> integerSchemas() {
    return Stream.of(
        Schema.OPTIONAL_INT8_SCHEMA,
        Schema.OPTIONAL_INT16_SCHEMA,
        Schema.OPTIONAL_INT32_SCHEMA,
        Schema.OPTIONAL_INT64_SCHEMA
    );
  }

  public static Stream<Schema> floatSchemas() {
    return Stream.of(
        Schema.OPTIONAL_FLOAT32_SCHEMA,
        Schema.OPTIONAL_FLOAT64_SCHEMA
    );
  }

  public static Stream<Schema> numberSchemas() {
    return Streams.concat(
        integerSchemas(),
        floatSchemas()
    );
  }
}