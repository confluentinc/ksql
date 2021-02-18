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

package io.confluent.ksql.benchmark;

import io.confluent.ksql.function.FunctionInvoker;
import io.confluent.ksql.function.FunctionLoaderUtils;
import io.confluent.ksql.function.udf.PluggableUdf;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 6, time = 10)
@Measurement(iterations = 3, time = 10)
@Threads(4)
@Fork(3)
public class UdfInvokerBenchmark {

  @State(Scope.Thread)
  public static class UdfInvokerState {

    private PluggableUdf simple;
    private PluggableUdf varargs;
    private Method simpleMethod;
    private Method varArgsMethod;

    @Setup(Level.Iteration)
    public void setUp() {
      simpleMethod = createMethod("simpleMethod", int.class);
      varArgsMethod = createMethod("varArgsMethod", int.class, long[].class);
      simple = createPluggableUdf(simpleMethod);
      varargs = createPluggableUdf(varArgsMethod);
    }

    private Method createMethod(final String methodName, final Class<?>... params) {
      try {
        return getClass().getMethod(methodName, params);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    private PluggableUdf createPluggableUdf(final Method method) {
      try {
        final FunctionInvoker invoker = FunctionLoaderUtils.createFunctionInvoker(method);
        return new PluggableUdf(invoker, this);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    public int simpleMethod(final int x) {
      return x + 1;
    }

    public int varArgsMethod(final int x, final long... arr) {
      return x + arr.length;
    }
  }

  @Benchmark
  public int invokeSimpleMethod(final UdfInvokerState state) {
    try {
      return (Integer) state.simpleMethod.invoke(state, 1);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Benchmark
  public int invokeVarargsMethod(final UdfInvokerState state) {
    try {
      return (Integer) state.varArgsMethod.invoke(state, 1, vargs2);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Benchmark
  public int invokeSimple(final UdfInvokerState state) {
    return (Integer) state.simple.evaluate(1);
  }

  @Benchmark
  public int invokeVarargs(final UdfInvokerState state) {
    return (Integer) state.varargs.evaluate(vargs);
  }

  static Object[] vargs = new Object[]{1, 1L, 2L, 3L, 4L, 5L};
  static long[] vargs2 = new long[] {1L, 2L, 3L, 4L, 5L};

  public static void main(final String[] args) throws RunnerException {
    final Options opt = new OptionsBuilder()
        .include(UdfInvokerBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}

