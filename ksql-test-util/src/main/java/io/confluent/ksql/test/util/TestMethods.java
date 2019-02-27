/*
 * Copyright 2018 Confluent Inc.
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

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Find methods within a class on which to run tests.
 */
public final class TestMethods {

  private static final Map<Type, Object> BUILT_IN_DEFAULTS = ImmutableMap.<Type, Object>builder()
      .put(int.class, 0)
      .put(long.class, 0L)
      .put(float.class, 0.0f)
      .put(double.class, 0.0)
      .put(char.class, 'c')
      .put(byte.class, (byte) 0)
      .put(short.class, (short) 0)
      .put(String.class, "")
      .put(Duration.class, Duration.ofMillis(1))
      .put(Pattern.class, Pattern.compile(".*"))
      .build();

  private TestMethods() {
  }

  public static <T> Builder<T> builder(final Class<T> typeUnderTest) {
    return new Builder<>(typeUnderTest);
  }

  public static final class Builder<T> {

    private final Class<T> typeUnderTest;
    private final Set<Method> blackList = new HashSet<>();
    private final Map<Class<?>, Object> defaults = new HashMap<>();

    public Builder(final Class<T> typeUnderTest) {
      this.typeUnderTest = Objects.requireNonNull(typeUnderTest, "typeUnderTest");
    }

    /**
     * Exclude a certain method from the test cases.
     *
     * @param methodName the name of the method
     * @param paramTypes the types of the parameters to the method.
     * @return the builder.
     */
    public Builder<T> ignore(final String methodName, final Class<?>... paramTypes) {
      blackList.addAll(getDeclaredPublicMethods(methodName, paramTypes));
      return this;
    }

    /**
     * Set the default instance for a specific parameter type.
     *
     * @param parameterType the type of the parameter.
     * @param defaultInstance the default instance to use where this parameter is seen.
     * @param <PT> the type of the parameter.
     * @return the builder.
     */
    public <PT> Builder<T> setDefault(
        final Class<PT> parameterType,
        final PT defaultInstance
    ) {
      final Object oldValue = defaults.put(parameterType, defaultInstance);
      if (oldValue != null) {
        throw new IllegalArgumentException("Setting default multiple times for " + parameterType);
      }
      return this;
    }

    /**
     * Build the test cases.
     *
     * @return the test cases.
     */
    public Collection<TestCase> build() {
      return Arrays.stream(typeUnderTest.getDeclaredMethods())
          .filter(method -> !Modifier.isStatic(method.getModifiers()))
          .filter(method -> Modifier.isPublic(method.getModifiers()))
          .filter(method -> !blackList.contains(method))
          .map(this::buildTestCase)
          .collect(Collectors.toList());
    }

    private Collection<? extends Method> getDeclaredPublicMethods(
        final String methodName,
        final Class<?>[] paramTypes
    ) {
      final List<Method> matching = Arrays.stream(typeUnderTest.getDeclaredMethods())
          .filter(m -> m.getName().equals(methodName))
          .filter(m -> Arrays.equals(m.getParameterTypes(), paramTypes))
          .collect(Collectors.toList());

      if (matching.isEmpty()) {
        throw new AssertionError("invalid test: unknown excluded method: "
            + methodName + "(" + Arrays.toString(paramTypes) + ")");
      }

      return matching;
    }

    private TestCase<T> buildTestCase(final Method method) {
      final Object[] args = Arrays.stream(method.getParameterTypes())
          .map(this::findArg)
          .toArray();

      method.setAccessible(true);
      return new TestCase<>(method, args);
    }

    private Object findArg(final Class<?> type) {
      if (defaults.containsKey(type)) {
        return defaults.get(type);
      }

      final Object arg = BUILT_IN_DEFAULTS.get(type);
      if (arg != null) {
        return arg;
      }

      if (type.isArray()) {
        return Array.newInstance(type.getComponentType(), 0);
      }

      if (type.isEnum()) {
        return type.getEnumConstants()[0];
      }

      if (Modifier.isFinal(type.getModifiers())) {
        throw new AssertionError("invalid test: please call 'setDefault' for type " + type);
      }

      return mock(type);
    }
  }

  public static final class TestCase<T> {

    private final Method method;
    private final Object[] args;

    private TestCase(final Method method, final Object[] args) {
      this.method = Objects.requireNonNull(method, "method");
      this.args = Objects.requireNonNull(args, "args");
    }

    public void invokeMethod(final T instanceUnderTest) throws Throwable {
      try {
        method.invoke(instanceUnderTest, args);
      } catch (IllegalAccessException e) {
        throw new AssertionError("Invoke failed", e);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }

    @Override
    public String toString() {
      final String params = Arrays.stream(method.getParameterTypes())
          .map(Class::getSimpleName)
          .collect(Collectors.joining(", "));

      return method.getReturnType().getSimpleName() + " " + method.getName() + "(" + params + ")";
    }
  }
}
