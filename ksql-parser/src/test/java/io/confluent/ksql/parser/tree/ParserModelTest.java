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

package io.confluent.ksql.parser.tree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.test.util.ClassFinder;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.kstream.Windows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Meta test to ensure all model classes meet certain requirements
 */
@RunWith(Parameterized.class)
public class ParserModelTest {

  private static final Predicate<Class<?>> KNOWN_IMMUTABLE_TYPES = Stream
      .<Predicate<Class<?>>>of(
          Class::isPrimitive,
          Class::isEnum,
          String.class::isAssignableFrom,
          Windows.class::isAssignableFrom,
          OptionalInt.class::isAssignableFrom,
          OptionalLong.class::isAssignableFrom,
          OptionalDouble.class::isAssignableFrom
      )
      .reduce(type -> false, Predicate::or);

  private final Class<?> modelClass;
  private final String name;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Class<?>> data() {
    return ClassFinder.getClasses(FunctionCall.class.getPackage().getName()).stream()
        .filter(Node.class::isAssignableFrom)
        .collect(Collectors.toList());
  }

  public ParserModelTest(final Class<?> modelClass) {
    this.modelClass = modelClass;
    this.name = modelClass.getSimpleName();
  }

  @Test
  public void shouldBeImmutable() {
    final Immutable annotation = modelClass.getAnnotation(Immutable.class);
    assertThat(name + ": @Immutable annotation missing",
        annotation,
        is(notNullValue()));
  }

  @Test
  public void shouldHaveOnlyFinalFields() {
    getFields().forEach(field ->
        assertThat(name + ": field not final: " + field.getName(),
            Modifier.isFinal(field.getModifiers()),
            is(true))
    );
  }

  @Test
  public void shouldHaveOnlyImmutableFieldTypes() {
    getFields().forEach(this::assertImmutableFieldType);
  }

  private Stream<Field> getFields() {
    return Arrays.stream(modelClass.getDeclaredFields());
  }

  private void assertImmutableFieldType(final Field field) {
    try {
      checkImmutableType(field.getGenericType());
    } catch (final AssertionError e) {
      throw new AssertionError(
          name + "." + field.getName() + " " + field.getGenericType() + ": " + e.getMessage(),
          e);
    }
  }

  private static void checkImmutableType(final Type type) {
    if (type instanceof Class) {
      final Class<?> clazz = (Class<?>) type;
      if (KNOWN_IMMUTABLE_TYPES.test(clazz)) {
        return;
      }

      final Class<?> superclass = clazz.getSuperclass();
      if (!Node.class.isAssignableFrom(superclass) && superclass != Object.class) {
        checkImmutableType(clazz.getGenericSuperclass());
      }

      if (clazz.isAnnotationPresent(Immutable.class)) {
        return;
      }
    }

    if (type instanceof ParameterizedType) {
      final ParameterizedType paramType = (ParameterizedType) type;
      final Class rawType = (Class) paramType.getRawType();

      if (Collection.class.isAssignableFrom(rawType)) {
        checkImmutableCollectionType(paramType);
        return;
      }

      if (java.util.Map.class.isAssignableFrom(rawType)) {
        checkImmutableMapType(paramType);
        return;
      }

      if (Optional.class.isAssignableFrom(rawType)) {
        checkTypeParameters(paramType);
        return;
      }
    }

    throw new AssertionError("Unknown type: " + type);
  }

  private static void checkImmutableCollectionType(final ParameterizedType type) {
    final Class rawType = (Class) type.getRawType();
    if (!ImmutableCollection.class.isAssignableFrom(rawType)) {
      throw new AssertionError("Not ImmutableCollection type: " + rawType);
    }

    checkTypeParameters(type);
  }

  private static void checkImmutableMapType(final ParameterizedType type) {
    final Class rawType = (Class) type.getRawType();
    if (!ImmutableMap.class.isAssignableFrom(rawType)) {
      throw new AssertionError("Not ImmutableMap type: " + rawType);
    }

    checkTypeParameters(type);
  }

  private static void checkTypeParameters(final ParameterizedType type) {
    Arrays.stream(type.getActualTypeArguments())
        .forEach(ParserModelTest::checkImmutableType);
  }
}
