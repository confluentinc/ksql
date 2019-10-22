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

package io.confluent.ksql.function;

import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FunctionValidatorTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private FunctionValidator validator;

  @Before
  public void setUp() {
    validator = new FunctionValidator();
  }

  @Test
  public void shouldThrowOnArrayElementTypeMismatch() {
    // Given:
    final Type actualType = getReturnType("arrayOfLong");

    final SqlType expectedType = SqlTypes.array(SqlTypes.INTEGER);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Return type does not match supplied schema: ARRAY<INTEGER>. "
            + "Type mismatch on element type. expected: class java.lang.Integer, but got: class java.lang.Long");

    // When:
    validator.validateReturnType(actualType, expectedType);
  }

  @Test
  public void shouldNotThrowIfArrayTypeMatchesSchema() {
    // Given:
    final Type actualType = getReturnType("arrayOfLong");

    final SqlType expectedType = SqlTypes.array(SqlTypes.BIGINT);

    // When:
    validator.validateReturnType(actualType, expectedType);

    // Then:
    // passed validation
  }

  @Test
  public void shouldNotThrowOnRawList() {
    // Given:
    final Type actualType = getReturnType("rawArray");

    final SqlType expectedType = SqlTypes.array(SqlTypes.INTEGER);

    // When:
    validator.validateReturnType(actualType, expectedType);

    // Then:
    // passed validation
  }

  @Test
  public void shouldThrowOnMapKeyTypeMismatch() {
    // Given:
    final Type actualType = getReturnType("mapWithIntKey");

    final SqlType expectedType = SqlTypes.map(SqlTypes.INTEGER);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Return type does not match supplied schema: MAP<STRING, INTEGER>. "
            + "Type mismatch on map key type. "
            + "expected: class java.lang.String, but got: class java.lang.Integer");

    // When:
    validator.validateReturnType(actualType, expectedType);
  }

  @Test
  public void shouldThrowOnMapValueTypeMismatch() {
    // Given:
    final Type actualType = getReturnType("mapWithIntValue");

    final SqlType expectedType = SqlTypes.map(SqlTypes.BIGINT);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Return type does not match supplied schema: MAP<STRING, BIGINT>. "
            + "Type mismatch on map value type. "
            + "expected: class java.lang.Long, but got: class java.lang.Integer");

    // When:
    validator.validateReturnType(actualType, expectedType);
  }

  @Test
  public void shouldNotThrowIfMapTypeMatchesSchema() {
    // Given:
    final Type actualType = getReturnType("mapWithIntValue");

    final SqlType expectedType = SqlTypes.map(SqlTypes.INTEGER);

    // When:
    validator.validateReturnType(actualType, expectedType);

    // Then:
    // passed validation
  }

  @Test
  public void shouldNotThrowOnRawMap() {
    // Given:
    final Type actualType = getReturnType("rawMap");

    final SqlType expectedType = SqlTypes.map(SqlTypes.INTEGER);

    // When:
    validator.validateReturnType(actualType, expectedType);

    // Then:
    // passed validation
  }

  @Test
  public void shouldThrowOnWrongRawType() {
    // Given:
    final Type actualType = getReturnType("rawMap");

    final SqlType expectedType = SqlTypes.array(SqlTypes.INTEGER);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Return type does not match supplied schema: ARRAY<INTEGER>. "
            + "expected: interface java.util.List, but got: interface java.util.Map");

    // When:
    validator.validateReturnType(actualType, expectedType);
  }

  @Test
  public void shouldThrowOnWildcard() {
    // Given:
    final Type actualType = getReturnType("withWildcard");

    final SqlType expectedType = SqlTypes.array(SqlTypes.INTEGER);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Wildcards, i.e. '?', are not supported. Please use specific types.");

    // When:
    validator.validateReturnType(actualType, expectedType);
  }

  @Test
  public void shouldThrowOnTypeVar() {
    // Given:
    final Type actualType = getReturnType("typeVariable");

    final SqlType expectedType = SqlTypes.array(SqlTypes.INTEGER);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Type variables, e.g. V, are not supported. Please use specific types.");

    // When:
    validator.validateReturnType(actualType, expectedType);
  }

  @Test
  public void shouldThrowOnGenericArray() {
    // Given:
    final Type actualType = getReturnType("genericArray");

    final SqlType expectedType = SqlTypes.array(SqlTypes.INTEGER);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Generic array types, e.g. V[], are not supported. "
            + "Please List<> for ARRAY types.");

    // When:
    validator.validateReturnType(actualType, expectedType);
  }

  @Test
  public void shouldSupportImplementations() {
    // Given:
    final Type actualType = getReturnType("mapImpl");

    final SqlType expectedType = SqlTypes.map(SqlTypes.INTEGER);

    // When:
    validator.validateReturnType(actualType, expectedType);

    // Then:
    // passed validation
  }

  @Test
  public void shouldNotSupportSubclassed() {
    // Given:
    final Type actualType = getReturnType("mapOfIntSubclass");

    final SqlType expectedType = SqlTypes.map(SqlTypes.INTEGER);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "expected: interface java.util.Map, "
            + "but got: class io.confluent.ksql.function.FunctionValidatorTest$TestTypes$MapOfInt");

    // When:
    validator.validateReturnType(actualType, expectedType);
  }

  @Test
  public void shouldSupportNestedTypes() {
    // Given:
    final Type actualType = getReturnType("arrayOfMapOfStrings");

    final SqlType expectedType = SqlTypes.array(SqlTypes.map(SqlTypes.STRING));

    // When:
    validator.validateReturnType(actualType, expectedType);

    // Then:
    // passed validation
  }

  @Test
  public void shouldThrowIfNestedTypeIsWrong() {
    // Given:
    final Type actualType = getReturnType("arrayOfMapOfStrings");

    final SqlType expectedType = SqlTypes.array(SqlTypes.map(SqlTypes.DOUBLE));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Return type does not match supplied schema: ARRAY<MAP<STRING, DOUBLE>>. "
            + "Type mismatch on map value type. "
            + "expected: class java.lang.Double, but got: class java.lang.String");

    // When:
    validator.validateReturnType(actualType, expectedType);
  }

  private static Type getReturnType(final String funcName) {
    try {
      return TestTypes.class.getDeclaredMethod(funcName).getGenericReturnType();
    } catch (final Exception e) {
      throw new RuntimeException("Invalid test");
    }
  }

  @SuppressWarnings("unused") // Used via reflection
  private static final class TestTypes {

    private static List<Long> arrayOfLong() {
      return null;
    }

    private static List rawArray() {
      return null;
    }

    private static Map<Integer, Integer> mapWithIntKey() {
      return null;
    }

    private static Map<String, Integer> mapWithIntValue() {
      return null;
    }

    private static Map rawMap() {
      return null;
    }

    private static List<?> withWildcard() {
      return null;
    }

    private static <V> List<V> typeVariable() {
      return null;
    }

    private static <V> List<V[]> genericArray() {
      return null;
    }

    public static List<Map<String, String>> arrayOfMapOfStrings() {
      return null;
    }

    private static HashMap<String, Integer> mapImpl() {
      return null;
    }

    private static MapOfInt mapOfIntSubclass() {
      return null;
    }

    private static final class MapOfInt extends HashMap<String, Integer> {

    }
  }
}