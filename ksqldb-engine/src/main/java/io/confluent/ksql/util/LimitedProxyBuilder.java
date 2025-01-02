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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.google.errorprone.annotations.Immutable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Creates a proxy around an instance of a class, only allowing white listed function calls.
 *
 * <p>None white listed function calls result in an {@link UnsupportedOperationException}.
 *
 * <p>Proxies are used to minimise the impact of new methods being added to interfaces from
 * products outside the control of KSQL, e.g. avoiding compilation errors if a new method
 * is added to an interface.
 *
 * @param <T> the interface type that is to be proxied.
 */
public final class LimitedProxyBuilder<T> {

  /**
   * Special marked that can be passed as the only type in any {@code paramTypes} parameter to
   * indicate all variants of the given {@code methodName} should be included.
   */
  private static final MethodParams ANY_PARAMS = methodParams(AllVariants.class);

  private static final MethodParams NO_PARAMS = methodParams();

  private static final InvocationHandler SWALLOW = (proxy, method, args) -> null;

  private final Class<T> type;

  private final Map<Method, InvocationHandler> handledMethods = new HashMap<>();

  /**
   * Get a build for an interface {@code T}.
   *
   * @param type the type of the interface to proxy.
   * @param <T> the type of the interface to proxy.
   * @return the proxy builder.
   */
  public  static <T> LimitedProxyBuilder<T> forClass(final Class<T> type) {
    return new LimitedProxyBuilder<>(type);
  }

  /**
   * Add method(s) whose invocation should be swallowed, i.e. result in a no-op.
   *
   * <p>Method must have a {@code void} return type.
   *
   * @param methodName the name of the method.
   * @param methodParams the type of the method.
   * @return the builder.
   */
  public LimitedProxyBuilder<T> swallow(final String methodName, final MethodParams methodParams) {
    final Collection<Method> methods = getDeclaredPublicMethods(methodName, methodParams);

    throwOnNoneVoidReturnType(methods);

    methods.forEach(method -> handledMethods.put(method, buildSwallower(method, null)));
    return this;
  }

  /**
   * Add method(s) whose invocation should be swallowed, i.e. result in a no-op.
   *
   * @param methodName the name of the method.
   * @param methodParams the type of the method.
   * @param returnValue the return value from the method.
   * @return the builder.
   */
  public LimitedProxyBuilder<T> swallow(
      final String methodName,
      final MethodParams methodParams,
      final Object returnValue
  ) {
    final Collection<Method> methods = getDeclaredPublicMethods(methodName, methodParams);

    methods.forEach(method -> handledMethods.put(method, buildSwallower(method, returnValue)));
    return this;
  }

  /**
   * Add method(s) whose invocations should be forwarded to a {@code delegate}.
   *
   * <p>If the {@code delegate} implements {@code T}, then invocations will be forwarded to the
   * same
   * method on the {@code delegate}.
   *
   * <p>If the {@code delegate} does not implement {@code T}, then invocations will be forwarded to
   * a method with the same name, return type and parameter types on the {@code delegate}.
   *
   * @param methodName the name of the method.
   * @param methodParams the type of the method.
   * @param delegate the delegate to forward to.
   * @return the builder
   */
  public LimitedProxyBuilder<T> forward(
      final String methodName,
      final MethodParams methodParams,
      final Object delegate
  ) {
    final Collection<Method> methods = getDeclaredPublicMethods(methodName, methodParams);

    methods.forEach(method -> handledMethods.put(method, buildForwader(delegate, method)));
    return this;
  }

  /**
   * Build the proxy.
   *
   * @param <TT> Actual type, (to allow generic interface types).
   * @return the proxy.
   */
  @SuppressWarnings("unchecked")
  public <TT extends T> TT build() {
    return (TT) Proxy.newProxyInstance(
        LimitedProxyBuilder.class.getClassLoader(),
        new Class[]{type},
        new SandboxProxy(handledMethods));
  }

  /**
   * Build a {@link MethodParams} instance.
   *
   * @param params the types of the methods params.
   * @return the method params instance.
   */
  public static MethodParams methodParams(final Class<?>... params) {
    return new MethodParams(params);
  }

  /**
   * An empty {@link MethodParams} instance, indicating the method takes no parameters.
   *
   * @return the empty method params instance.
   */
  public static MethodParams noParams() {
    return NO_PARAMS;
  }

  /**
   * A special marker {@link MethodParams} instance, that matches methods with any params.
   *
   * <p>i.e. this instance can be used to include all flavours of a method.
   *
   * @return the special instance.
   */
  public static MethodParams anyParams() {
    return ANY_PARAMS;
  }

  private LimitedProxyBuilder(final Class<T> type) {
    this.type = Objects.requireNonNull(type, "type");

    if (!type.isInterface()) {
      throw new IllegalArgumentException("Type not an interface: " + type);
    }
  }

  private Collection<Method> getDeclaredPublicMethods(
      final String methodName,
      final MethodParams methodParams
  ) {
    final boolean allVariants = methodParams.equals(ANY_PARAMS);

    final List<Method> matching = Arrays.stream(type.getDeclaredMethods())
        .filter(m -> m.getName().equals(methodName))
        .filter(m -> allVariants || methodParams.matches(m.getParameterTypes()))
        .collect(Collectors.toList());

    if (matching.isEmpty()) {
      throw new IllegalArgumentException("Interface '" + type.getSimpleName()
          + "' does not have method: " + methodName + "(" + methodParams + ")");
    }

    throwIfAlreadyRegistered(matching);

    return matching;
  }

  private static InvocationHandler buildSwallower(final Method method, final Object returnValue) {
    final Class<?> returnType = method.getReturnType();
    if (returnType.equals(void.class)) {
      if (returnValue != null) {
        throw new IllegalArgumentException("Can not provide a default value for void method: "
            + formatMethod(method));
      }
      return SWALLOW;
    }

    if (returnValue != null
        && !Primitives.unwrap(returnValue.getClass()).equals(Primitives.unwrap(returnType))
        && !returnType.isAssignableFrom(returnValue.getClass())) {
      throw new IllegalArgumentException("Supplied return value is not of type "
          + returnType.getSimpleName());
    }

    return (proxy, m, args) -> returnValue;
  }

  private InvocationHandler buildForwader(final Object delegate, final Method proxyMethod) {
    if (type.isAssignableFrom(delegate.getClass())) {
      return (proxy, m, args) -> m.invoke(delegate, args);
    }

    final Method declaredMethod = findMatchingMethod(delegate, proxyMethod);
    return (proxy, m, args) -> declaredMethod.invoke(delegate, args);
  }

  private void throwIfAlreadyRegistered(final Collection<Method> methods) {
    final String duplicates = methods.stream()
        .filter(handledMethods::containsKey)
        .map(LimitedProxyBuilder::formatMethod)
        .collect(Collectors.joining(System.lineSeparator()));

    if (!duplicates.isEmpty()) {
      throw new IllegalArgumentException("method(s) already registered: "
          + System.lineSeparator() + duplicates);
    }
  }

  private static void throwOnNoneVoidReturnType(final Collection<Method> methods) {
    final String noneVoid = methods.stream()
        .filter(method -> !method.getReturnType().equals(void.class))
        .map(LimitedProxyBuilder::formatMethod)
        .collect(Collectors.joining(System.lineSeparator()));

    if (!noneVoid.isEmpty()) {
      throw new IllegalArgumentException("Can only swallow void methods. "
          + "None void methods: " + System.lineSeparator() + noneVoid);
    }
  }

  @SuppressWarnings("deprecation") // No alternative until Java 9.
  private static Method findMatchingMethod(final Object delegate, final Method proxyMethod) {
    try {
      final Method declaredMethod = delegate.getClass()
          .getDeclaredMethod(proxyMethod.getName(), proxyMethod.getParameterTypes());

      if (!declaredMethod.getReturnType().equals(proxyMethod.getReturnType())) {
        throw new IllegalArgumentException(
            "Delegate's method has different return type. wanted:"
                + proxyMethod.getReturnType() + ", got:" + declaredMethod.getReturnType());
      }

      if (!declaredMethod.isAccessible()) {
        declaredMethod.setAccessible(true);
      }

      return declaredMethod;
    } catch (final NoSuchMethodException e) {
      throw new IllegalArgumentException("Delegate does not have method: "
          + formatMethod(proxyMethod), e);
    }
  }

  private static String formatMethod(final Method method) {
    final String params = Arrays.stream(method.getParameterTypes())
        .map(Class::getSimpleName)
        .collect(Collectors.joining(","));

    return method.getReturnType().getSimpleName() + " " + method.getName() + "(" + params + ")";
  }

  private static final class SandboxProxy implements InvocationHandler {

    private final Map<Method, InvocationHandler> handledMethods;

    private SandboxProxy(final Map<Method, InvocationHandler> handledMethods) {
      this.handledMethods = ImmutableMap.copyOf(handledMethods);
    }

    @Override
    public Object invoke(
        final Object proxy,
        final Method method,
        final Object[] args
    ) throws Throwable {
      final InvocationHandler handler = handledMethods.get(method);
      if (handler == null) {
        throw new UnsupportedOperationException(formatMethod(method));
      }

      try {
        return handler.invoke(proxy, method, args);
      } catch (final InvocationTargetException e) {
        throw e.getCause() == null ? e : e.getCause();
      }
    }
  }

  private static final class AllVariants {

  }

  @Immutable
  static final class MethodParams {

    private final ImmutableList<Class<?>> paramTypes;

    MethodParams(final Class<?>... params) {
      this.paramTypes = asList(params);
    }

    public boolean matches(final Class<?>[] argTypes) {
      return asList(argTypes).equals(paramTypes);
    }

    @Override
    public String toString() {
      if (this == ANY_PARAMS) {
        return "*";
      }
      return paramTypes.stream()
          .map(Class::getSimpleName)
          .collect(Collectors.joining(","));
    }

    private static ImmutableList<Class<?>> asList(final Class<?>[] params) {
      return ImmutableList.<Class<?>>builder().add(params).build();
    }
  }
}
