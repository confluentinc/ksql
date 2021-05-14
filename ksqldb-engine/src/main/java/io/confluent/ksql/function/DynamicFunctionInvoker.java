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

package io.confluent.ksql.function;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.TypeVariable;

/**
 * An implementation of UdfInvoker which invokes the UDF using reflection
 */
public class DynamicFunctionInvoker implements FunctionInvoker {

  private final Method method;

  DynamicFunctionInvoker(final Method method) {
    final Class<?>[] types = method.getParameterTypes();
    for (int i = 0; i < types.length; i++) {
      if (method.getParameterTypes()[i].isArray()
          && (!method.isVarArgs() || i != method.getParameterCount() - 1)) {
        throw new KsqlFunctionException(
            "Invalid function method signature (contains non var-arg array): " + method);
      }
      if (method.getGenericParameterTypes()[i] instanceof TypeVariable
          || method.getGenericParameterTypes()[i] instanceof GenericArrayType) {
        // this is the case where the type parameter is generic
        continue;
      }
      final Class<?> type = types[i];
      UdafTypes.checkSupportedType(method, type);
    }
    this.method = method;
  }

  @Override
  public Object eval(final Object udf, final Object... args) {
    try {
      final Object[] extractedArgs = extractArgs(args);
      return method.invoke(udf, extractedArgs);
    } catch (final Exception e) {
      throw new KsqlFunctionException("Failed to invoke function " + method, e);
    }
  }

  /*
  Method.invoke() is a pain and expects any varargs to be packaged up in a further Object[]
   */
  private Object[] extractArgs(final Object... source) {
    if (!method.isVarArgs()) {
      return source;
    }

    final Object[] args = new Object[method.getParameterCount()];
    System.arraycopy(source, 0, args, 0, method.getParameterCount() - 1);

    final int start = method.getParameterCount() - 1;
    final Class<?> componentType = method.getParameterTypes()[start].getComponentType();

    // Need to convert to array of component type - Method.invoke requires this
    final Object val = Array.newInstance(componentType, source.length - start);
    for (int i = start; i < source.length; i++) {
      Array.set(val, i - start, source[i]);
    }
    args[start] = val;

    return args;
  }
}
