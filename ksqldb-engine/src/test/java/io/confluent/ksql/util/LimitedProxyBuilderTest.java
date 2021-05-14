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

package io.confluent.ksql.util;

import static io.confluent.ksql.util.LimitedProxyBuilder.anyParams;
import static io.confluent.ksql.util.LimitedProxyBuilder.forClass;
import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;
import static io.confluent.ksql.util.LimitedProxyBuilder.noParams;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public final class LimitedProxyBuilderTest {

  private LimitedProxyBuilderTest() {
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class CommonTests {

    @Test
    public void shouldThrowOnNoneInterface() {
      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> forClass(String.class)
      );

      // Then:
      assertThat(e.getMessage(), containsString("Type not an interface: " + String.class));
    }

    @Test
    public void shouldThrowOnDuplicateRegistration() {
      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> LimitedProxyBuilder.forClass(TestInterface.class)
              .swallow("noReturnValue", methodParams(String.class, long.class))
              .forward("noReturnValue", methodParams(String.class, long.class), null)
      );

      // Then:
      assertThat(e.getMessage(), containsString(
          "method(s) already registered:"
      ));
      assertThat(e.getMessage(), containsString(
          "void noReturnValue(String,long)"
      ));
    }

    @Test
    public void shouldThrowUnsupportedOnOtherMethods() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class).build();

      // When:
      final Exception e = assertThrows(
          UnsupportedOperationException.class,
          () -> proxy.noReturnValue("", 1)
      );

      // Then:
      assertThat(e.getMessage(), containsString("noReturnValue(String,long)"));
    }

    @Test
    public void shouldSwallowSpecificMethod() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .swallow("noReturnValue", methodParams(String.class, long.class))
          .build();

      // When:
      proxy.noReturnValue("bob", 42L);

      // Then: call was swallowed
    }

    @Test
    public void shouldSwallowMethodWithNoParams() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .swallow("noReturnValue", noParams())
          .build();

      // When:
      proxy.noReturnValue();

      // Then: call was swallowed
    }

    @Test
    public void shouldSwallowAllVariantsOfMethods() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .swallow("noReturnValue", anyParams())
          .build();

      // When:
      proxy.noReturnValue();
      proxy.noReturnValue("bob");
      proxy.noReturnValue("bob", 42L);

      // Then: call was swallowed
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedOnDefaultMethodIfNotSwallowed() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .swallow("defaultMethods", noParams())
          .build();

      // When:
      proxy.defaultMethods(1);
    }

    @Test
    public void shouldSwallowDefaultMethods() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .swallow("defaultMethods", methodParams(int.class))
          .build();

      // When:
      proxy.defaultMethods(1);

      // Then: invocation swallowed.
    }

    @Test
    public void shouldSwallowDefaultMethodsWhenUsingAnyParams() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .swallow("defaultMethods", anyParams())
          .build();

      // When:
      proxy.defaultMethods();
      proxy.defaultMethods(1);

      // Then: invocation swallowed.
    }

    @Test
    public void shouldThrowIfUnknownMethodName() {
      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> forClass(TestInterface.class)
              .swallow("unknown", anyParams())
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString("Interface 'TestInterface' does not have method: unknown(*)"));
    }

    @Test
    public void shouldThrowIfIUnknownMethodParams() {
      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> forClass(TestInterface.class)
              .swallow("noReturnValue", methodParams(TimeUnit.class))
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString("Interface 'TestInterface' does not have method: noReturnValue(TimeUnit)"));
    }

    @Test
    public void shouldThrowIfMethodHasNonVoidReturnTypeAndNoReturnValueSupplied() {
      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> LimitedProxyBuilder.forClass(TestInterface.class)
              .swallow("someFunc", noParams())
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString(
          "Can only swallow void methods. None void methods:"
      ));
      assertThat(e.getMessage(), containsString(
          "int someFunc()"
      ));
    }

    @Test
    public void shouldThrowIfAnyMethodHasNonVoidReturnType() {
      // When:
      final IllegalArgumentException e = assertThrows(
          IllegalArgumentException.class,
          () -> LimitedProxyBuilder.forClass(TestInterface.class)
              .swallow("someFunc", anyParams())
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString("Can only swallow void methods. None void methods:"));
      assertThat(e.getMessage(), containsString("int someFunc()"));
      assertThat(e.getMessage(), containsString("String someFunc(String)"));
    }

    @Test
    public void shouldThrowIfMethodHasVoidReturnTypeAndReturnValueSupplied() {
      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> LimitedProxyBuilder.forClass(TestInterface.class)
              .swallow("noReturnValue", noParams(), 10)
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString(
          "Can not provide a default value for void method:"
      ));
      assertThat(e.getMessage(), containsString(
          "void noReturnValue()"
      ));
    }

    @Test
    public void shouldSwallowNonVoidFunctionWithNonNullReturnValue() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .swallow("someFunc", noParams(), 10)
          .build();

      // When:
      final int result = proxy.someFunc();

      // Then:
      assertThat(result, is(10));
    }

    @Test
    public void shouldSwallowNonVoidFunctionWithNullReturnValue() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .swallow("returnsList", noParams(), null)
          .build();

      // When:
      final List<String> result = proxy.returnsList();

      // Then:
      assertThat(result, is(nullValue()));
    }

    @Test
    public void shouldSwallowNonVoidFunctionWithReturnValue() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .swallow("returnsList", noParams(), Collections.emptyList())
          .build();

      // When:
      final List<String> result = proxy.returnsList();

      // Then:
      assertThat(result, is(empty()));
    }
  }

  @RunWith(Parameterized.class)
  public static class ForwardTests {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String> getMethodsToTest() {
      return ImmutableList.of("forward to type T", "forward to unrelated type");
    }

    private final TestInterface mock;
    private final Object delegate;

    public ForwardTests(final String testCase) {
      this.mock = mock(TestInterface.class);
      this.delegate = testCase.equals("forward to type T") ? mock : new AnotherType();
    }

    @Test
    public void shouldReturnDelegateInvocationResult() {
      // Given:
      when(mock.someFunc()).thenReturn(12345);

      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .forward("someFunc", noParams(), delegate)
          .build();

      // When:
      final int result = proxy.someFunc();

      // Then:
      assertThat(result, is(12345));
    }

    @Test
    public void shouldForwardToSpecificMethod() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .forward("someFunc", noParams(), delegate)
          .build();

      // When:
      proxy.someFunc();

      // Then:
      verify(mock).someFunc();
    }

    @Test
    public void shouldForwardToNoParamsMethod() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .forward("someFunc", noParams(), delegate)
          .build();

      // When:
      proxy.someFunc();

      // Then:
      verify(mock).someFunc();
    }

    @Test
    public void shouldForwardToAnyParamsMethod() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .forward("someFunc", anyParams(), delegate)
          .build();

      // When:
      proxy.someFunc();
      proxy.someFunc("bob");
      proxy.someFunc(42L, 11.4);

      // Then:
      verify(mock).someFunc();
      verify(mock).someFunc("bob");
      verify(mock).someFunc(42L, 11.4);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedOnDefaultMethodIfNotForwarded() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .forward("defaultMethods", noParams(), delegate)
          .build();

      // When:
      proxy.defaultMethods(1);
    }

    @Test
    public void shouldForwardDefaultMethods() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .forward("defaultMethods", methodParams(int.class), delegate)
          .build();

      // When:
      proxy.defaultMethods(1);

      // Then:
      verify(mock).defaultMethods(1);
    }

    @Test
    public void shouldForwardDefaultMethodsWhenUsingAnyParams() {
      // Given:
      final TestInterface proxy = LimitedProxyBuilder.forClass(TestInterface.class)
          .forward("defaultMethods", anyParams(), delegate)
          .build();

      // When:
      proxy.defaultMethods();
      proxy.defaultMethods(1);

      // Then:
      verify(mock).defaultMethods();
      verify(mock).defaultMethods(1);
    }

    @Test
    public void shouldThrowIfUnknownMethodName() {
      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> forClass(TestInterface.class)
              .forward("unknown", methodParams(String.class), delegate)
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString("Interface 'TestInterface' does not have method: unknown(String)"));
    }

    @Test
    public void shouldThrowIfIUnknownMethodParams() {
      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> forClass(TestInterface.class)
              .forward("noReturnValue", methodParams(TimeUnit.class), delegate)
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString("Interface 'TestInterface' does not have method: noReturnValue(TimeUnit)"));
    }

    @Test
    public void shouldThrowIfDelegateDoesMethodWithSameName() {
      assumeThat("ignore if delegate of type T", delegate, instanceOf(AnotherType.class));

      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> LimitedProxyBuilder.forClass(TestInterface.class)
              .forward("noReturnValue", noParams(), delegate)
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString("Delegate does not have method: void noReturnValue()"));
    }

    @Test
    public void shouldThrowIfDelegatesMethodHasDifferentReturnValue() {
      assumeThat("ignore if delegate of type T", delegate, instanceOf(AnotherType.class));

      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> LimitedProxyBuilder.forClass(TestInterface.class)
              .forward("differentReturnType", anyParams(), delegate)
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString("Delegate's method has different return type. wanted:int, got:long"));
    }

    @Test
    public void shouldThrowIfDelegatesMethodHasDifferentParameters() {
      assumeThat("ignore if delegate of type T", delegate, instanceOf(AnotherType.class));

      // When:
      final Exception e = assertThrows(
          IllegalArgumentException.class,
          () -> LimitedProxyBuilder.forClass(TestInterface.class)
              .forward("differentParams", methodParams(double.class, String.class), delegate)
              .build()
      );

      // Then:
      assertThat(e.getMessage(), containsString("Delegate does not have method: int differentParams(double,String)"));
    }

    @SuppressWarnings("unused") // Invoked via reflection.
    private class AnotherType {

      int someFunc() {
        return mock.someFunc();
      }

      String someFunc(final String a) {
        return mock.someFunc(a);
      }

      void someFunc(final long a0, final double a1) {
        mock.someFunc(a0, a1);
      }

      int differentParams(final Double a0, final String a1) {
        return 0;
      }

      long differentReturnType() {
        return 0L;
      }

      void defaultMethods() {
        mock.defaultMethods();
      }

      void defaultMethods(final int i) {
        mock.defaultMethods(i);
      }
    }
  }

  @SuppressWarnings("unused")
      // Invoked via reflection
  interface TestInterface {

    void noReturnValue();

    void noReturnValue(String arg);

    void noReturnValue(String arg0, long arg1);

    int someFunc();

    String someFunc(String arg);

    void someFunc(long arg0, double arg1);

    int differentParams(double arg0, String arg1);

    int differentReturnType();

    void defaultMethods();

    default void defaultMethods(final int i) {
      throw new AssertionError("should never be called");
    }

    List<String> returnsList();
  }
}