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

import io.confluent.ksql.test.tools.CalciteTPCHQueries;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.adapter.tpch.TpchSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
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
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 *  Runs JMH microbenchmarks against KSQL serdes.
 *  See `ksql-benchmark/README.md` for more info, including benchmark results
 *  and how to run the benchmarks.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 3, time = 10)
@Threads(4)
@Fork(3)
public class CalciteBenchmark {
  private static final String TPCHQuery1 = CalciteTPCHQueries.QUERIES.get(0);

  @State(Scope.Thread)
  public static class FrontEndState {

    public Planner planner;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Setup(Level.Iteration)
    public void setUp() throws IOException {
      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

      rootSchema.add(
          "TPCH",
          new TpchSchema(
              1.0,
              1,
              1,
              false
          )
      );

      final SqlParser.Config parserConfig =
          SqlParser
              .config()
              .withCaseSensitive(false)
              .withParserFactory(SqlDdlParserImpl::new);

      final FrameworkConfig config = Frameworks.newConfigBuilder()
          .parserConfig(parserConfig)
          .defaultSchema(rootSchema)
          .build();

      planner = Frameworks.getPlanner(config);
    }

    @Setup(Level.Invocation)
    public void setUpTest() {
      planner.close();
      planner.reset();
    }
  }

  @SuppressWarnings("MethodMayBeStatic") // Tests can not be static
  @Benchmark
  public SqlNode query(final FrontEndState frontEndState) throws SqlParseException {
//    frontEndState.planner.close();
//    frontEndState.planner.reset();
    return frontEndState.planner.parse(TPCHQuery1);
  }

  public static void main(final String[] args) throws Exception {

    final Options opt = args.length != 0
        ? new CommandLineOptions(args)
        : new OptionsBuilder()
            .include(CalciteBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .build();

    new Runner(opt).run();
  }
}
