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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.GenericsUtil;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class GenericsUtilTest {

  @Test
  public void shouldIdentifyGenericCorrectly() {
    // Given:
    final GenericType generic = GenericType.of("T");

    // Then:
    assertThat("should be a generic", GenericsUtil.isGeneric(generic));
  }

  @Test
  public void shouldNotIdentifyGenericIncorrectly() {
    // Given:
    final ParamType array = ArrayType.of(GenericType.of("T"));

    // Then:
    assertThat("should not be a generic", !GenericsUtil.isGeneric(array));
  }

  @Test
  public void shouldFindAllConstituentGenerics() {
    // Given:
    final GenericType a = GenericType.of("A");
    final GenericType b = GenericType.of("B");
    final GenericType c = GenericType.of("C");
    final GenericType d = GenericType.of("D");
    final ParamType map = MapType.of(GenericType.of("C"), GenericType.of("D"));

    final StructType complexSchema = StructType.builder()
        .field("a", a)
        .field("b", b)
        .field("c", map)
        .build();

    // When:
    final Set<ParamType> generics = GenericsUtil.constituentGenerics(complexSchema);

    // Then:
    assertThat(generics, containsInAnyOrder(a, b, c, d));
  }

  @Test
  public void shouldFindNoConstituentGenerics() {
    // Given:
    final StructType struct = StructType.builder()
        .field("a", ParamTypes.LONG)
        .field("b", ParamTypes.DECIMAL)
        .build();

    // When:
    final Set<ParamType> generics = GenericsUtil.constituentGenerics(struct);

    // Then:
    assertThat(generics, empty());
  }

  @Test
  public void shouldResolveSchemaWithMapping() {
    // Given:
    final GenericType a = GenericType.of("A");
    final ImmutableMap<GenericType, SqlType> mapping = ImmutableMap.of(a, SqlTypes.STRING);

    // When:
    final SqlType resolved = GenericsUtil.applyResolved(a, mapping);

    // Then:
    assertThat(resolved, is(SqlTypes.STRING));
  }

  @Test
  public void shouldResolveArraySchemaWithMapping() {
    // Given:
    final GenericType a = GenericType.of("A");
    final ParamType array = ArrayType.of(a);
    final ImmutableMap<GenericType, SqlType> mapping = ImmutableMap.of(a, SqlTypes.STRING);

    // When:
    final SqlType resolved = GenericsUtil.applyResolved(array, mapping);

    // Then:
    assertThat(resolved, is(SqlTypes.array(SqlTypes.STRING)));
  }

  @Test
  public void shouldResolveMapSchemaWithMapping() {
    // Given:
    final GenericType a = GenericType.of("A");
    final GenericType b = GenericType.of("B");
    final ParamType map = MapType.of(a, b);
    final ImmutableMap<GenericType, SqlType> mapping = ImmutableMap.of(
        a, SqlTypes.INTEGER,
        b, SqlTypes.DOUBLE
    );

    // When:
    final SqlType resolved = GenericsUtil.applyResolved(map, mapping);

    // Then:
    assertThat(resolved, is(SqlTypes.map(SqlTypes.INTEGER, SqlTypes.DOUBLE)));
  }

  @Test
  public void shouldIdentifyGeneric() {
    // Given:
    final GenericType a = GenericType.of("A");
    final SqlType instance = SqlTypes.STRING;

    // When:
    final Map<GenericType, SqlType> mapping = GenericsUtil.resolveGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(a, SqlTypes.STRING));
  }

  @Test
  public void shouldIdentifyArrayGeneric() {
    // Given:
    final ArrayType a = ArrayType.of(GenericType.of("A"));
    final SqlType instance = SqlTypes.array(SqlTypes.STRING);

    // When:
    final Map<GenericType, SqlType> mapping = GenericsUtil.resolveGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(a.element(), SqlTypes.STRING));
  }

  @Test
  public void shouldIdentifyMapGeneric() {
    // Given:
    final MapType a = MapType.of(GenericType.of("A"), GenericType.of("B"));
    final SqlType instance = SqlTypes.map(SqlTypes.DOUBLE, SqlTypes.BIGINT);

    // When:
    final Map<GenericType, SqlType> mapping = GenericsUtil.resolveGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(a.key(), SqlTypes.DOUBLE));
    assertThat(mapping, hasEntry(a.value(), SqlTypes.BIGINT));
  }

  @Test
  public void shouldNotIdentifyInstanceOfTypeMismatch() {
    // Given:
    final MapType map = MapType.of(GenericType.of("A"), GenericType.of("B"));
    final SqlType instance = SqlTypes.array(SqlTypes.STRING);

    // When:
    final boolean isInstance = GenericsUtil.instanceOf(map, instance);

    // Then:
    assertThat("expected not instance of", !isInstance);
  }

  @Test(expected = KsqlException.class)
  public void shouldFailResolveSchemaWithIncompleteMapping() {
    // Given:
    final GenericType a = GenericType.of("A");
    final Map<GenericType, SqlType> mapping = ImmutableMap.of();

    // When:
    GenericsUtil.applyResolved(a, mapping);
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIdentifyMismatchStructureGeneric() {
    // Given:
    final MapType a = MapType.of(GenericType.of("A"), GenericType.of("B"));
    final SqlArray instance = SqlTypes.array(SqlTypes.STRING);

    // When:
    GenericsUtil.resolveGenerics(a, instance);
  }
}