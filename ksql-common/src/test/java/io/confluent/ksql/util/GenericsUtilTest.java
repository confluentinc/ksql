/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class GenericsUtilTest {

  @Test
  public void shouldConstructValidGenericTypeName() {
    // Given:
    final String type = "T";

    // When:
    final Schema schema = GenericsUtil.generic(type);

    // Then:
    assertThat(schema.name(), is("<T>"));
  }

  @Test
  public void shouldConstructValidGenericArray() {
    // Given:
    final String type = "T";

    // When:
    final Schema schema = GenericsUtil.array(type);

    // Then:
    assertThat(schema.type(), is(Type.ARRAY));
    assertThat(schema.valueSchema().name(), is("<T>"));
  }

  @Test
  public void shouldConstructValidGenericMap() {
    // Given:
    final String type = "T";

    // When:
    final Schema schema = GenericsUtil.map(Schema.OPTIONAL_STRING_SCHEMA, type);

    // Then:
    assertThat(schema.type(), is(Type.MAP));
    assertThat(schema.keySchema(), is(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(schema.valueSchema().name(), is("<T>"));
  }

  @Test
  public void shouldIdentifyGenericCorrectly() {
    // Given:
    final Schema generic = GenericsUtil.generic("T");

    // Then:
    assertThat("should be a generic", GenericsUtil.isGeneric(generic));
  }

  @Test
  public void shouldNotIdentifyGenericIncorrectly() {
    // Given:
    final Schema array = GenericsUtil.array("T");

    // Then:
    assertThat("should not be a generic", !GenericsUtil.isGeneric(array));
  }

  @Test
  public void shouldFindAllConstituentGenerics() {
    // Given:
    final Schema a = GenericsUtil.generic("A").build();
    final Schema b = GenericsUtil.array("B").build();
    final Schema c = GenericsUtil.map(Schema.OPTIONAL_STRING_SCHEMA, "C").build();

    final Schema complexSchema = SchemaBuilder.struct()
        .field("a", a)
        .field("b", b)
        .field("c", c)
        .build();

    // When:
    final Set<Schema> generics = GenericsUtil.constituentGenerics(complexSchema);

    // Then:
    assertThat(generics, containsInAnyOrder(
        GenericsUtil.generic("A").build(),
        GenericsUtil.generic("B").build(),
        GenericsUtil.generic("C").build()
    ));
  }

  @Test
  public void shouldResolveSchemaWithMapping() {
    // Given:
    final Schema a = GenericsUtil.generic("A").build();
    final Map<Schema, Schema> mapping = ImmutableMap.of(a, Schema.OPTIONAL_STRING_SCHEMA);

    // When:
    final Schema resolved = GenericsUtil.resolve(a, mapping);

    // Then:
    assertThat(resolved, is(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldResolveArraySchemaWithMapping() {
    // Given:
    final Schema a = GenericsUtil.generic("A").build();
    final Schema array = GenericsUtil.array("A").build();
    final Map<Schema, Schema> mapping = ImmutableMap.of(a, Schema.OPTIONAL_STRING_SCHEMA);

    // When:
    final Schema resolved = GenericsUtil.resolve(array, mapping);

    // Then:
    assertThat(resolved, is(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()));
  }

  @Test
  public void shouldResolveMapSchemaWithMapping() {
    // Given:
    final Schema a = GenericsUtil.generic("A").build();
    final Schema map = GenericsUtil.map(Schema.OPTIONAL_STRING_SCHEMA, "A").build();
    final Map<Schema, Schema> mapping = ImmutableMap.of(a, Schema.OPTIONAL_STRING_SCHEMA);

    // When:
    final Schema resolved = GenericsUtil.resolve(map, mapping);

    // Then:
    assertThat(resolved,
        is(SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()));
  }

  @Test
  public void shouldIdentifyGeneric() {
    // Given:
    final Schema a = GenericsUtil.generic("A").build();
    final Schema instance = Schema.OPTIONAL_STRING_SCHEMA;

    // When:
    final Map<Schema, Schema> mapping = GenericsUtil.identifyGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(a, Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldIdentifyArrayGeneric() {
    // Given:
    final Schema a = GenericsUtil.array("A").build();
    final Schema instance = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();

    // When:
    final Map<Schema, Schema> mapping = GenericsUtil.identifyGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(a.valueSchema(), Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldIdentifyMapGeneric() {
    // Given:
    final Schema a = GenericsUtil.map(Schema.OPTIONAL_STRING_SCHEMA, "A").build();
    final Schema instance = SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build();

    // When:
    final Map<Schema, Schema> mapping = GenericsUtil.identifyGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(a.valueSchema(), Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnNonLettersInTypeName() {
    // When:
    GenericsUtil.generic("!");
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowIfConflictingGeneric() {
    // When:
    GenericsUtil.identifyGenerics(
        GenericsUtil.map(GenericsUtil.generic("A").build(), "A").build(),
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA).build()
    );
  }
}