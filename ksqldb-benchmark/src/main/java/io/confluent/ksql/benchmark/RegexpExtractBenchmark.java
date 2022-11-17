/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.ksql.function.udf.string.RegexpExtract;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 3, time = 10)
@Threads(4)
@Fork(3)
public class RegexpExtractBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({ "32", "128", "1024"})
    public int stringSize;

    public final String pattern = "((api[_.]secret|password|sasl[_.]password).?[=:].?([a-zA-Z0-9+/]{64})|(ey[A-Za-z0-9_-]+\\\\.){2}[A-Za-z0-9_-]+)";
  }

  @Benchmark
  public void regexpExtract(final Blackhole bh, final BenchmarkState state) {
    final RegexpExtract udf = new RegexpExtract();
    bh.consume(udf.regexpExtract(state.pattern, RandomStringUtils.random(state.stringSize)));
  }

  public static void main(final String[] args) throws Exception {

    final Options opt = args.length != 0
        ? new CommandLineOptions(args)
        : new OptionsBuilder()
            .include(RegexpExtractBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .build();

    new Runner(opt).run();
  }
}
