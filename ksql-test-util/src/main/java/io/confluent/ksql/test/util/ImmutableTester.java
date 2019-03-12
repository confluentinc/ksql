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

package io.confluent.ksql.test.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;

public final class ImmutableTester {

  private static final List<Predicate<Class<?>>> STD_IMMUTABLE_TYPES = ImmutableList
      .<Predicate<Class<?>>>builder()
      .add(Class::isPrimitive)
      .add(Class::isEnum)
      .add(String.class::isAssignableFrom)
      .add(Optional.class::isAssignableFrom)
      .add(OptionalInt.class::isAssignableFrom)
      .add(OptionalLong.class::isAssignableFrom)
      .add(OptionalDouble.class::isAssignableFrom)
      .build();

  private final List<Predicate<Class<?>>> knownImmutables = new ArrayList<>(STD_IMMUTABLE_TYPES);

  public ImmutableTester withKnownImmutableTypes(final Set<Class<?>> immutable) {
    immutable.forEach(type -> knownImmutables.add(t -> t.equals(type)));
    return this;
  }

  public ImmutableTester withKnownImmutableTypes(
      final Collection<Predicate<Class<?>>> immutable
  ) {
    knownImmutables.addAll(immutable);
    return this;
  }

  public ImmutableTester withKnownImmutableType(final Class<?> immutable) {
    knownImmutables.add(t -> t.equals(immutable));
    return this;
  }

  public ImmutableTester withKnownImmutableType(
      final Predicate<Class<?>> immutable
  ) {
    knownImmutables.add(immutable);
    return this;
  }

  public void test(final Class<?> typeUnderTest) {
    final String className = typeUnderTest.getSimpleName();

    final Predicate<Class<?>> knownImmutable = knownImmutables
        .stream()
        .reduce(type -> false, Predicate::or);

    assertThat("@Immutable annotation missing",
        typeUnderTest.isAnnotationPresent(Immutable.class),
        is(true));

    Arrays.stream(typeUnderTest.getDeclaredFields())
        .filter(excludeJaCoCoInjectedFields())
        .forEach(field -> {
          assertThat(className + ": field not final: " + field.getName(),
              Modifier.isFinal(field.getModifiers()),
              is(true));

          try {
            checkImmutableType(field.getGenericType(), knownImmutable);
          } catch (final AssertionError e) {
            throw new AssertionError(
                className + "." + field.getName() + " " + field.getGenericType()
                    + ": " + e.getMessage(), e);
          }
        });
  }

  private static void checkImmutableType(
      final Type type,
      final Predicate<Class<?>> knownImmutable
  ) {
    if (type instanceof TypeVariable) {
      return;
    }

    if (type instanceof ParameterizedType) {
      checkParameterizedType((ParameterizedType) type, knownImmutable);
      return;
    }

    if (type instanceof Class) {
      final Class<?> clazz = (Class<?>) type;
      if (knownImmutable.test(clazz)) {
        return;
      }

      final Class<?> superclass = clazz.getSuperclass();
      if (superclass != null && superclass != Object.class) {
        checkImmutableType(clazz.getGenericSuperclass(), knownImmutable);
      }

      if (clazz.isAnnotationPresent(Immutable.class)) {
        return;
      }
    }

    throw new AssertionError("Can not determine if type is immutable: " + type);
  }

  private static void checkParameterizedType(
      final ParameterizedType type,
      final Predicate<Class<?>> knownImmutable
  ) {
    final Class<?> rawType = (Class) type.getRawType();

    if (Collection.class.isAssignableFrom(rawType)) {
      if (!ImmutableCollection.class.isAssignableFrom(rawType)) {
        throw new AssertionError("Not ImmutableCollection type: " + rawType);
      }
    } else if (java.util.Map.class.isAssignableFrom(rawType)) {
      if (!ImmutableMap.class.isAssignableFrom(rawType)) {
        throw new AssertionError("Not ImmutableMap type: " + rawType);
      }
    } else {
      checkImmutableType(rawType, knownImmutable);
    }

    checkTypeParameters(type, knownImmutable);
  }

  private static void checkTypeParameters(
      final ParameterizedType type,
      final Predicate<Class<?>> knownImmutable
  ) {
    Arrays.stream(type.getActualTypeArguments())
        .forEach(type1 -> checkImmutableType(type1, knownImmutable));
  }

  private static Predicate<Field> excludeJaCoCoInjectedFields() {
    // JaCoCo, which runs as part of the Jenkins build, injects a non-field:
    return field -> !field.getName().equals("$jacocoData");
  }
}
