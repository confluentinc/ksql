/*
 * Copyright 2021 Confluent Inc.
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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.errorprone.annotations.Immutable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Tests that a type is immutable.
 *
 * <p>To be classed as immutable it must:
 * <ul>
 *   <li>Annotated {@code @Immutable}</li>
 *   <li>Each field, including static fields, must:</li>
 *   <ul>
 *    <li>be {@code final}</li>
 *    <li>be of a immutable type</li>
 *   </ul>
 * </ul>
 *
 * <p>The tester knows about (most) JDK immutable types.
 *
 * <p>Types from other libraries that are known to be immutable, but which are not annotated as
 * such, can be registered with the tester as immutable, e.g. via
 * {@link ImmutableTester#withKnownImmutableType}.
 *
 * <p>Where a field is not of an immutable type, but is known to be immutable by its usage patterns,
 * it can be annotated with {@code @EffectivelyImmutable}.
 */
public final class ImmutableTester {

  private static final List<Predicate<Class<?>>> STD_IMMUTABLE_TYPES = ImmutableList
      .<Predicate<Class<?>>>builder()
      .add(Class::isPrimitive)
      .add(Class::isEnum)
      .add(Class.class::isAssignableFrom)
      .add(Duration.class::isAssignableFrom)
      .add(Character.class::isAssignableFrom)
      .add(Void.class::isAssignableFrom)
      .add(Byte.class::isAssignableFrom)
      .add(Boolean.class::isAssignableFrom)
      .add(Integer.class::isAssignableFrom)
      .add(Long.class::isAssignableFrom)
      .add(Float.class::isAssignableFrom)
      .add(Double.class::isAssignableFrom)
      .add(String.class::isAssignableFrom)
      .add(Optional.class::isAssignableFrom)
      .add(OptionalInt.class::isAssignableFrom)
      .add(OptionalLong.class::isAssignableFrom)
      .add(OptionalDouble.class::isAssignableFrom)
      .add(ThreadLocal.class::isAssignableFrom)
      .add(URI.class::isAssignableFrom)
      .add(URL.class::isAssignableFrom)
      .add(Pattern.class::isAssignableFrom)
      .add(Range.class::isAssignableFrom)
      .add(BigDecimal.class::isAssignableFrom)
      .build();

  private final List<Predicate<Class<?>>> knownImmutables = new ArrayList<>(STD_IMMUTABLE_TYPES);

  public static List<Class<?>> classesMarkedImmutable(final String packageName) {
    return ClassFinder.getClasses(packageName).stream()
        .filter(c -> c.isAnnotationPresent(Immutable.class))
        .collect(Collectors.toList());
  }

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
    if (typeUnderTest.isEnum() || typeUnderTest.isInterface()) {
      // Implicitly immutable
      return;
    }

    final Predicate<Class<?>> knownImmutable = knownImmutables
        .stream()
        .reduce(type -> false, Predicate::or);

    if (knownImmutable.test(typeUnderTest)) {
      // Known to be immutable
      return;
    }

    assertThat("@Immutable annotation missing",
        typeUnderTest.isAnnotationPresent(Immutable.class),
        is(true));

    final String className = typeUnderTest.getSimpleName();

    Arrays.stream(typeUnderTest.getDeclaredFields())
        .filter(excludeJaCoCoInjectedFields())
        .filter(excludeEffectivelyImmutableFields())
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

    if (type instanceof WildcardType) {
      checkWildcardType((WildcardType) type, knownImmutable);
      return;
    }

    if (type instanceof Class) {
      final Class<?> clazz = (Class<?>) type;
      if (isEffectivelyImmutable(clazz) || knownImmutable.test(clazz)) {
        return;
      }

      final Class<?> superclass = clazz.getSuperclass();
      if (superclass != null && superclass != Object.class) {
        checkImmutableType(
            clazz.getGenericSuperclass(),
            // if we see this type again, we can assume it is immutable
            knownImmutable.or(type::equals)
        );
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
    final Class<?> rawType = (Class<?>) type.getRawType();

    if (Collection.class.isAssignableFrom(rawType)) {
      if (!ImmutableCollection.class.isAssignableFrom(rawType)) {
        throw new AssertionError("Not ImmutableCollection type: " + rawType);
      }
      checkTypeParameters(type, knownImmutable);
    } else if (java.util.Map.class.isAssignableFrom(rawType)) {
      if (!ImmutableMap.class.isAssignableFrom(rawType)) {
        throw new AssertionError("Not ImmutableMap type: " + rawType);
      }
      checkTypeParameters(type, knownImmutable);
    } else if (Multimap.class.isAssignableFrom(rawType)) {
      if (!ImmutableMultimap.class.isAssignableFrom(rawType)) {
        throw new AssertionError("Not ImmutableMultimap type: " + rawType);
      }
      checkTypeParameters(type, knownImmutable);
    } else if (rawType.equals(Optional.class)) {
      checkTypeParameters(type, knownImmutable);
    } else {
      checkImmutableType(rawType, knownImmutable);
    }
  }

  private static void checkWildcardType(
      final WildcardType type,
      final Predicate<Class<?>> knownImmutable
  ) {
    // Type is immutable if extends an immutable type.
    for (final Type upperBound : type.getUpperBounds()) {
      try {
        checkImmutableType(upperBound, knownImmutable);
        return;
      } catch (final AssertionError e) {
        // Not immutable
      }
    }

    throw new AssertionError("Wildcard type does not extend immutable type: " + type);
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

  private static Predicate<Field> excludeEffectivelyImmutableFields() {
    return field -> !containsEffectivelyImmutable(field.getAnnotations());
  }

  private static boolean isEffectivelyImmutable(final Class<?> clazz) {
    return containsEffectivelyImmutable(clazz.getAnnotations());
  }

  private static boolean containsEffectivelyImmutable(final Annotation[] annotations) {
    // @EffectivelyImmutable is an annotation within the common module that can be used to mark
    // a field or type, that is of a type this test can't confirm is immutable, as being effectively
    // immutable.
    //
    // The annotation itself is not accessible from this package, so the check is by name
    return Arrays.stream(annotations)
        .anyMatch(anno -> anno.annotationType().getSimpleName().equals("EffectivelyImmutable"));
  }
}
