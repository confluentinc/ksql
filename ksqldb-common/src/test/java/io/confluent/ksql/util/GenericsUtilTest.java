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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.GenericsUtil;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlLambdaResolved;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.HashMap;
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
  public void shouldFindAllConstituentGenericsInLambdaType() {
    // Given:
    final GenericType a = GenericType.of("A");
    final GenericType b = GenericType.of("B");
    final GenericType c = GenericType.of("C");
    final GenericType d = GenericType.of("D");
    final ParamType lambda = LambdaType.of(
        ImmutableList.of(
            GenericType.of("C"),
            GenericType.of("A"),
            GenericType.of("B")),
            GenericType.of("D"));

    // When:
    final Set<ParamType> generics = GenericsUtil.constituentGenerics(lambda);

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
    final SqlArgument instance = SqlArgument.of(SqlTypes.STRING);

    // When:
    final Map<GenericType, SqlType> mapping = GenericsUtil.reserveGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(a, SqlTypes.STRING));
  }

  @Test
  public void shouldIdentifyArrayGeneric() {
    // Given:
    final ArrayType a = ArrayType.of(GenericType.of("A"));
    final SqlArgument instance = SqlArgument.of(SqlTypes.array(SqlTypes.STRING));

    // When:
    final Map<GenericType, SqlType> mapping = GenericsUtil.reserveGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(a.element(), SqlTypes.STRING));
  }

  @Test
  public void shouldIdentifyMapGeneric() {
    // Given:
    final MapType a = MapType.of(GenericType.of("A"), GenericType.of("B"));
    final SqlArgument instance = SqlArgument.of(SqlTypes.map(SqlTypes.DOUBLE, SqlTypes.BIGINT));

    // When:
    final Map<GenericType, SqlType> mapping = GenericsUtil.reserveGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(a.key(), SqlTypes.DOUBLE));
    assertThat(mapping, hasEntry(a.value(), SqlTypes.BIGINT));
  }

  @Test
  public void shouldIdentifySqlLambdaResolvedGenerics() {
    // Given:
    final GenericType typeA = GenericType.of("A");
    final GenericType typeB = GenericType.of("B");
    final LambdaType a = LambdaType.of(ImmutableList.of(typeA, typeB), typeB);
    final SqlArgument instance = SqlArgument.of(
        SqlLambdaResolved.of(ImmutableList.of(SqlTypes.DOUBLE, SqlTypes.BIGINT), SqlTypes.BIGINT));

    // When:
    final Map<GenericType, SqlType> mapping = GenericsUtil.reserveGenerics(a, instance);

    // Then:
    assertThat(mapping, hasEntry(typeA, SqlTypes.DOUBLE));
    assertThat(mapping, hasEntry(typeB, SqlTypes.BIGINT));
  }

  @Test
  public void shouldFailToIdentifySqlLambdaResolvedWithDifferentSchema() {
    // Given:
    final GenericType typeA = GenericType.of("A");
    final GenericType typeB = GenericType.of("B");
    final GenericType typeC = GenericType.of("C");
    final LambdaType a = LambdaType.of(ImmutableList.of(typeA, typeC), typeB);
    final SqlArgument instance = SqlArgument.of(
        SqlLambdaResolved.of(ImmutableList.of(SqlTypes.DOUBLE), SqlTypes.BIGINT));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> GenericsUtil.reserveGenerics(a, instance)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot infer generics for LAMBDA (A, C) => B from LAMBDA (DOUBLE) => BIGINT " 
            + "because they do not have the same schema structure"));
  }

  @Test
  public void shouldNotMapGenericsForNonSqlLambdaResolved() {
    // Given:
    final GenericType typeA = GenericType.of("A");
    final GenericType typeB = GenericType.of("B");
    final GenericType typeC = GenericType.of("C");
    final LambdaType a = LambdaType.of(ImmutableList.of(typeA, typeC), typeB);
    final SqlArgument instance = SqlArgument.of(
        SqlLambda.of(2));

    // When:
    final Map<GenericType, SqlType> mapping = GenericsUtil.reserveGenerics(a, instance);

    // Then:
    
    // the map should be empty since the instance type was a SqlLambda without any types resolved
    assertThat(mapping.size(), is(0));
  }

  @Test
  public void shouldFailToIdentifyLambdasWithDifferentArgumentList() {
    // Given:
    final GenericType typeA = GenericType.of("A");
    final GenericType typeB = GenericType.of("B");
    final GenericType typeC = GenericType.of("C");
    final LambdaType a = LambdaType.of(ImmutableList.of(typeA, typeC), typeB);
    final SqlArgument instance = SqlArgument.of(
        SqlLambda.of(4));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> GenericsUtil.reserveGenerics(a, instance)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot infer generics for LAMBDA (A, C) => B from LAMBDA " 
            + "because they do not have the same schema structure."));
  }

  @Test
  public void shouldFailToIdentifyMismatchedGenericsInLambda() {
    // Given:
    final GenericType typeA = GenericType.of("A");

    final LambdaType a = LambdaType.of(ImmutableList.of(typeA), typeA);
    final SqlArgument instance = SqlArgument.of(
        SqlLambdaResolved.of(ImmutableList.of(SqlTypes.DOUBLE), SqlTypes.BOOLEAN));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> GenericsUtil.reserveGenerics(a, instance)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
    "Found invalid instance of generic schema when mapping LAMBDA (A) => A to LAMBDA (DOUBLE) => BOOLEAN. "
        + "Cannot map A to both DOUBLE and BOOLEAN"));
  }

  @Test
  public void shouldIdentifyInstanceOfLambda() {
    // Given:
    final LambdaType lambda = LambdaType.of(ImmutableList.of(GenericType.of("A")), GenericType.of("B"));
    final SqlArgument instance = SqlArgument.of(
        SqlLambdaResolved.of(ImmutableList.of(SqlTypes.INTEGER), SqlTypes.BIGINT));

    // When:
    final boolean isInstance = GenericsUtil.reserveGenerics(lambda, instance, new HashMap<>()).getLeft();

    // Then:
    assertThat("expected instance of", isInstance);
  }

  @Test
  public void shouldNotIdentifyInstanceOfTypeMismatchLambda() {
    // Given:
    final MapType map = MapType.of(GenericType.of("A"), GenericType.of("B"));
    final SqlArgument lambdaInstance = SqlArgument.of(
        SqlLambdaResolved.of(ImmutableList.of(SqlTypes.INTEGER), SqlTypes.BIGINT));

    final LambdaType lambda = LambdaType.of(ImmutableList.of(GenericType.of("A")), GenericType.of("B"));
    final SqlArgument mapInstance = SqlArgument.of(SqlTypes.map(SqlTypes.STRING, SqlTypes.BOOLEAN));

    // When:
    final boolean isInstance1 = GenericsUtil.reserveGenerics(map, lambdaInstance, new HashMap<>()).getLeft();
    final boolean isInstance2 = GenericsUtil.reserveGenerics(lambda, mapInstance, new HashMap<>()).getLeft();

    // Then:
    assertThat("expected not instance of", !isInstance1);
    assertThat("expected not instance of", !isInstance2);
  }

  @Test
  public void shouldNotIdentifyInstanceOfTypeMismatch() {
    // Given:
    final MapType map = MapType.of(GenericType.of("A"), GenericType.of("B"));
    final SqlArgument instance = SqlArgument.of(SqlTypes.array(SqlTypes.STRING));

    // When:
    final boolean isInstance = GenericsUtil.reserveGenerics(map, instance, new HashMap<>()).getLeft();

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
    final SqlArgument instance = SqlArgument.of(SqlTypes.array(SqlTypes.STRING));

    // When:
    GenericsUtil.reserveGenerics(a, instance);
  }
}