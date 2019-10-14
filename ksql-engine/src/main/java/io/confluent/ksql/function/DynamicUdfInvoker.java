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

package io.confluent.ksql.function;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;

/**
 * An implementation of UdfInvoker which invokes the UDF dynamically using reflection
 */
public class DynamicUdfInvoker implements UdfInvoker {

  private final Method method;

  DynamicUdfInvoker(final Method method) {
    final Class<?>[] types = method.getParameterTypes();
    for (int i = 0; i < types.length; i++) {
      if (method.getParameterTypes()[i].isArray()
          && (!method.isVarArgs() || i != method.getParameterCount() - 1)) {
        throw new KsqlFunctionException(
            "Invalid UDF method signature (contains non var-arg array): " + method);
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
      for (int i = 0; i < extractedArgs.length; i++) {
        extractedArgs[i] = UdfArgCoercer.coerceUdfArgs(args[i], method.getParameterTypes()[i], i);
      }
      return method.invoke(udf, extractedArgs);
    } catch (Exception e) {
      throw new KsqlFunctionException("Failed to invoke udf " + method, e);
    }
  }

  // Method.invoke() is a pain and expects any varargs to be packaged up in a further Object[]
  private Object[] extractArgs(final Object... source) {
    if (!method.isVarArgs()) {
      return source;
    }

    final Object[] args = new Object[method.getParameterCount()];
    System.arraycopy(source, 0, args, 0, method.getParameterCount() - 1);

    final int start = method.getParameterCount() - 1;
    final Object[] varargs = Arrays.copyOfRange(source, start, source.length);
    args[start] = varargs;

    return args;
  }
}
