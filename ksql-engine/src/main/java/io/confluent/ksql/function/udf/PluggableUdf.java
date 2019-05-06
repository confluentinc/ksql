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

package io.confluent.ksql.function.udf;

import io.confluent.ksql.function.UdfInvoker;
import io.confluent.ksql.security.ExtensionSecurityManager;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

/**
 * Class to allow conversion from Kudf to UdfInvoker.
 * This may change if we ever get rid of Kudf. As it stands we need
 * to do a conversion from custom UDF -> Kudf so we can support stong
 * typing etc.
 */
public class PluggableUdf implements Kudf {

  private final UdfInvoker udf;
  private final Object actualUdf;
  private final Method method;

  public PluggableUdf(
      final UdfInvoker udfInvoker,
      final Object actualUdf,
      final Method method
  ) {
    this.udf = Objects.requireNonNull(udfInvoker, "udfInvoker");
    this.actualUdf = Objects.requireNonNull(actualUdf, "actualUdf");
    this.method = Objects.requireNonNull(method, "method");
  }

  @Override
  public Object evaluate(final Object... args) {
    try {
      ExtensionSecurityManager.INSTANCE.pushInUdf();
      return udf.eval(actualUdf, extractArgs(args));
    } finally {
      ExtensionSecurityManager.INSTANCE.popOutUdf();
    }
  }

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
