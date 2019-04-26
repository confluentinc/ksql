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

import com.google.common.primitives.Primitives;
import com.squareup.javapoet.CodeBlock;
import java.lang.reflect.Method;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public final class UdfTemplate {

  private static final Logger LOG = LoggerFactory.getLogger(UdfTemplate.class);

  private UdfTemplate() { }

  static String generateCode(final Method method, final String obj) {
    final Class<?>[] params = method.getParameterTypes();

    final CodeBlock.Builder code = CodeBlock.builder();
    for (int idx = 0; idx < params.length; idx++) {
      final Class<?> param = params[idx];
      code.addStatement("$T arg$L = ($T) $T.coerce(args, $T.class, $L)",
                        Primitives.wrap(param),
                        idx,
                        Primitives.wrap(param),
                        UdfTemplate.class,
                        param,
                        idx);
    }

    final String args = IntStream.range(0, params.length)
        .mapToObj(i -> "arg" + i)
        .collect(Collectors.joining(", "));

    // use the canonical name instead of relying on $T because there is a bug
    // in javapoet when trying to compile using the default package
    code.addStatement("return (($L) $L).$L($L)",
        method.getDeclaringClass().getCanonicalName(), obj, method.getName(), args);

    final String codeString = code.build().toString();
    LOG.trace("Generated code:\n" + codeString);
    return codeString;
  }

  public static <T> T coerce(
      final Object[] args,
      final Class<? extends T> clazz,
      final int index) {
    final Object arg = args[index];
    if (arg == null) {
      if (clazz.isPrimitive()) {
        throw new KsqlFunctionException(
            String.format(
                "Can't coerce argument at index %d from null to a primitive type", index));
      }
      return null;
    }

    // using boxed type is safe: long.class and Long.class are both of type Class<Long>
    // and this is a no-op for non-primitives
    final Class<? extends T> boxedType = Primitives.wrap(clazz);
    if (boxedType.isAssignableFrom(arg.getClass())) {
      return boxedType.cast(arg);
    }

    if (arg instanceof String) {
      try {
        return fromString((String) arg, clazz);
      } catch (Exception e) {
        throw new KsqlFunctionException(
            String.format("Couldn't coerce string argument '\"args[%d]\"' to type %s",
                          index, clazz));
      }
    }

    if (arg instanceof Number) {
      try {
        return (T) fromNumber((Number) arg, boxedType);
      } catch (Exception e) {
        throw new KsqlFunctionException(
            String.format("Couldn't coerce numeric argument '\"args[%d]:(%s) %s\"' to type %s",
                          index, arg.getClass(), arg, clazz));
      }
    }

    throw new KsqlFunctionException(
        String.format("Impossible to coerce (%s) %s into %s", arg.getClass(), arg, clazz));
  }

  private static <T> T fromNumber(final Number arg, final Class<? extends T> boxedType) {
    if (Integer.class.isAssignableFrom(boxedType)) {
      return boxedType.cast(arg.intValue());
    } else if (Long.class.isAssignableFrom(boxedType)) {
      return boxedType.cast(arg.longValue());
    } else if (Double.class.isAssignableFrom(boxedType)) {
      return boxedType.cast(arg.doubleValue());
    } else if (Float.class.isAssignableFrom(boxedType)) {
      return boxedType.cast(arg.floatValue());
    } else if (Byte.class.isAssignableFrom(boxedType)) {
      return boxedType.cast(arg.byteValue());
    } else if (Short.class.isAssignableFrom(boxedType)) {
      return boxedType.cast(arg.shortValue());
    }

    throw new KsqlFunctionException(String.format("Cannot coerce %s into %s", arg, boxedType));
  }

  @SuppressWarnings("unchecked")
  private static <T> T fromString(final String arg, final Class<T> clazz) {
    if (Integer.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Integer.valueOf(arg);
    } else if (Long.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Long.valueOf(arg);
    } else if (Double.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Double.valueOf(arg);
    } else if (Float.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Float.valueOf(arg);
    } else if (Byte.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Byte.valueOf(arg);
    } else if (Short.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Short.valueOf(arg);
    } else if (Boolean.class.isAssignableFrom(Primitives.wrap(clazz))) {
      return (T) Boolean.valueOf(arg);
    }

    throw new KsqlFunctionException(String.format("Cannot coerce %s into %s", arg, clazz));
  }

}
