/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function.udf;

import static junit.framework.TestCase.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Helper for testing KUDFs.
 */
public class KudfTester {

  private static final int NO_MAX_ARGS = Integer.MAX_VALUE;

  private static final Map<Class<?>, Object>
      DEFAULT_INSTANCES =
      ImmutableMap.<Class<?>, Object>builder()
          .put(String.class, "Hello")
          .put(Boolean.class, true)
          .put(Short.class, (short) 22)
          .put(Integer.class, 66)
          .put(Long.class, 42L)
          .put(Double.class, 12.345)
          .put(Float.class, 567.89f)
          .build();

  private final Supplier<Kudf> udfSupplier;
  private final Set<Integer> nonNullableArgs = new HashSet<>();

  private List<Class<?>> argTypes = Collections.emptyList();
  private List<Object> args = Collections.emptyList();
  private int minArgs = 0;
  private int maxArgs = 0;

  public KudfTester(final Supplier<Kudf> udfSupplier) {
    this.udfSupplier = Objects.requireNonNull(udfSupplier, "udfSupplier");
  }

  /**
   * Specify the list of argument types the UDF requires.
   *
   * Can be called in conjunction with {@link #withArguments}, to supply the list of, potentially
   * super, argument types, or by itself, where types will be auto-instantiated where possible.
   *
   * This implicitly sets min and max argument counts to the length of the supplied arguments.
   *
   * If the UDF has optional arguments, call {@link #withMinArgCount}.
   *
   * If the UDF can accept an unbounded list of arguments, call {@link #withUnboundedMaxArgCount}.
   * The last argument type in the supplied {@code argTypes} will be treated as a varargs.
   *
   * @param argTypes the list of arguments the UDK requires.
   * @return self.
   */
  public KudfTester withArgumentTypes(final Class<?>... argTypes) {
    final List<Class<?>> types = Arrays.asList(argTypes);
    if (types.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("argTypes can not contain nulls");
    }

    this.argTypes = ImmutableList.copyOf(types);

    if (minArgs == 0) {
      minArgs = this.argTypes.size();
    }

    if (maxArgs == 0) {
      maxArgs = this.argTypes.size();
    }
    return this;
  }

  /**
   * Supplies the list of arguments to pass to the UDF.
   *
   * Can be used to pass concrete instances to be used as arguments.
   *
   * Can be used in conjunction with {@link #withArgumentTypes} to provide suitable instances for
   * the, potentially super types, supplied to {@link #withArgumentTypes(Class[])}, or by itself.
   * In which case the argument types will be implied from the list args themselves.
   *
   * This implicitly sets min and max argument counts to the length of the supplied arguments.
   *
   * If the UDF has optional arguments, call {@link #withMinArgCount}.
   *
   * If the UDF can accept an unbounded list of arguments, call {@link #withUnboundedMaxArgCount}.
   * The last argument type in the supplied {@code args} will be treated as a varargs.
   *
   * @param args the list of arguments the UDK requires.
   * @return self.
   */
  public KudfTester withArguments(final Object... args) {
    final List<Object> argList = Arrays.asList(args);
    if (argList.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("args can not contain nulls");
    }

    this.args = ImmutableList.copyOf(argList);

    if (minArgs == 0) {
      minArgs = this.args.size();
    }

    if (maxArgs == 0) {
      maxArgs = this.args.size();
    }

    return this;
  }

  /**
   * Marks an argument so that it is expected to throw an exception when null.
   *
   * It is common for UDFs to return null for null parameters, e.g. {@code Abs(null)} will return
   * null. However, for some UDFs this is not the case. The presence of a null value for some or
   * all arguments may be an error, e.g. {@code EXTRACTJSONFIELD(null, "$some.path")} is valid,
   * (and returns null), where as {@code EXTRACTJSONFIELD(something, null)} is an error and should
   * return in a {@link KsqlException} being thrown.
   *
   * @param argIdx the argument index that should error when null.
   * @return self.
   */
  public KudfTester withNonNullableArgument(final int argIdx) {
    if (argIdx < 0) {
      throw new IllegalArgumentException("argIdx can not be negative: " + argIdx);
    }

    nonNullableArgs.add(argIdx);
    return this;
  }

  /**
   * Set the minimum argument count the UDF should accept.
   *
   * This can be useful if the UDF has optional arguments, meaning the min arg count is less
   * that the size of the list of arguments passed to @link #withArgumentTypes}.
   *
   * @param count the min arg count
   * @return self
   */
  public KudfTester withMinArgCount(final int count) {
    if (count < 0) {
      throw new IllegalArgumentException("count can not be negative:" + count);
    }

    minArgs = count;
    return this;
  }

  /**
   * Call if the UDF can handle an unbounded number of arguments.
   *
   * The last argument type in the list supplied to {@link #withArgumentTypes} will be treated as
   * a vararg.
   *
   * For example, if the UDF takes two or more strings, call:
   *
   * <pre>
   *   {@code kudfTester
   *       .withArgumentTypes(String.class, String.class)
   *       .withUnboundedMaxArgCount()
   *       .test();
   *    }
   * </pre>
   *
   * @return self.
   */
  public KudfTester withUnboundedMaxArgCount() {
    maxArgs = NO_MAX_ARGS;
    return this;
  }

  /**
   * Run the tests!
   */
  public void test() {
    validateAndBuild();
    testTooFewArguments();
    testTooManyArguments();
    testNullHandling();
    testTypeChecking();
  }

  private void validateAndBuild() {
    if (maxArgs == 0) {
      throw new IllegalStateException(
          "UDF must take arguments. Please supply via withArgumentTypes()");
    }

    if (!argTypes.isEmpty() && !args.isEmpty() && argTypes.size() != args.size()) {
      throw new IllegalStateException(
          "length of arguments passed to withArgumentTypes and withArguments must match");
    }

    if (argTypes.isEmpty()) {
      argTypes = ImmutableList.copyOf(
          args.stream()
              .map(Object::getClass)
              .collect(Collectors.toList()));
    }

    if (args.isEmpty()) {
      args = ImmutableList.copyOf(
          argTypes.stream()
              .map(KudfTester::instantiate)
              .collect(Collectors.toList()));
    }

    if (minArgs > argTypes.size()) {
      throw new IllegalStateException("Can not set min arg count higher that the size of the list"
                                      + " of argument types passed to withArgumentTypes()");
    }

    if (maxArgs < argTypes.size()) {
      throw new IllegalStateException("Can not set max arg count lower that the size of the list"
                                      + " of argument types passed to withArgumentTypes()");
    }

    try {
      evaluate(args);
    } catch (final Exception e) {
      throw new AssertionError("UDF threw unexpected exception with the supplied argument(types)",
                               e);
    }
  }

  private void testTooFewArguments() {
    if (minArgs == 0) {
      return;
    }

    final List<Object> theArgs = getArgs();
    theArgs.remove(theArgs.size() - 1);

    try {
      evaluate(theArgs);
      fail("UDF did not throw on two few arguments. minArgs: " + minArgs);
    } catch (final KsqlException e) {
      // expected
    }
  }

  private void testTooManyArguments() {
    if (maxArgs == NO_MAX_ARGS) {
      return;
    }

    final List<Object> theArgs = getArgs();
    theArgs.add(theArgs.get(theArgs.size() - 1));

    try {
      evaluate(theArgs);
      fail("UDF did not throw on two many arguments. maxArgs: " + maxArgs);
    } catch (final KsqlException e) {
      // expected
    }
  }

  private void testNullHandling() {
    final List<Object> theArgs = getArgs();

    for (int idx = 0; idx != theArgs.size(); ++idx) {
      final boolean exceptionExpected = nonNullableArgs.contains(idx);

      final Object old = theArgs.set(idx, null);

      try {
        evaluate(theArgs);

        if (exceptionExpected) {
          fail("Expected UDF to throw KsqlException when arg " + idx + " is null");
        }
      } catch (final KsqlException e) {
        if (!exceptionExpected) {
          throw new AssertionError("Unexpected exception thrown by UDF when arg "
                                   + idx + " is null.", e);
        }
      } catch (final Exception e) {
        if (exceptionExpected) {
          throw new AssertionError("Unexpected exception thrown by UDF when arg "
                                   + idx + " is null. Expected KsqlException", e);
        }
        throw new AssertionError("Unexpected exception thrown by UDF when arg "
                                 + idx + " is null.", e);
      }

      theArgs.set(idx, old);
    }
  }

  private void testTypeChecking() {
    final List<Object> theArgs = getArgs();

    for (int idx = 0; idx != theArgs.size(); ++idx) {
      final Class<?> argType = argTypes.get(idx);
      if (argType.equals(Object.class)) {
        continue;
      }

      final Object differentType = argType.equals(String.class) ? 1234L : "diff";
      final Object old = theArgs.set(idx, differentType);

      try {
        evaluate(theArgs);
        fail("UDF did not throw when arg " + idx + " was the wrong type");

      } catch (final KsqlException e) {
        // Expected
      } catch (final Exception e) {
        throw new AssertionError("UDF threw unexpected exception type when arg " + idx
                                 + " was the wrong type. Expected: KSQLException", e);
      }

      theArgs.set(idx, old);
    }
  }

  private List<Object> getArgs() {
    return new ArrayList<>(args);
  }

  private void evaluate(final List<Object> theArgs) {
    udfSupplier.get().evaluate(theArgs.toArray());
  }

  private static Object instantiate(final Class<?> argType) {
    final Object exact = DEFAULT_INSTANCES.get(argType);
    if (exact != null) {
      return exact;
    }

    return DEFAULT_INSTANCES.entrySet().stream()
        .filter(e -> argType.isAssignableFrom(e.getKey()))
        .map(Map.Entry::getValue)
        .findAny()
        .orElseThrow(() -> new AssertionError(
            "Unknown argType - consider calling setDefault. type:" + argType));
  }
}
