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

import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.test.tools.CalciteTPCHQueries;
import io.confluent.ksql.test.tools.PrestoPlanner;
import io.confluent.ksql.test.tools.PrestoPlanner.ProtoConnector;
import io.confluent.ksql.test.tools.PrestoPlanner.Table;
import java.io.IOException;
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
public class PrestoBenchmark {
  private static final String TPCHQuery1 = CalciteTPCHQueries.QUERIES.get(0);

  @State(Scope.Thread)
  public static class FrontEndState {
    private PrestoPlanner prestoPlanner;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Setup(Level.Invocation)
    public void setUp() throws IOException {
      final String schemaName = "tpch";
      final Table pantalonesPrestoTable = new Table(
          "lineitem",
          schemaName,
          ImmutableList.of(
              ColumnMetadata
                  .builder()
                  .setName("l_linestatus")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_rowNumber")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_orderKey")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_partKey")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_supplierKey")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_lineNumber")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_quantity")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_extendedPrice")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_discount")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_tax")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_returnFlag")
                  .setType(VarcharType.createUnboundedVarcharType())
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_status")
                  .setType(VarcharType.createUnboundedVarcharType())
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_shipDate")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_commitDate")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_receiptDate")
                  .setType(IntegerType.INTEGER)
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_shipInstructions")
                  .setType(VarcharType.createUnboundedVarcharType())
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_shipMode")
                  .setType(VarcharType.createUnboundedVarcharType())
                  .setNullable(false)
                  .build(),
              ColumnMetadata
                  .builder()
                  .setName("l_comment")
                  .setType(VarcharType.createUnboundedVarcharType())
                  .setNullable(false)
                  .build()
          )
      );
      ProtoConnector connector = new ProtoConnector(
          ImmutableList.of(
              pantalonesPrestoTable
          )
      );

      prestoPlanner = new PrestoPlanner(schemaName, connector);
    }
  }

  @SuppressWarnings("MethodMayBeStatic") // Tests can not be static
  @Benchmark
  public Plan query(final FrontEndState frontEndState) {
    return frontEndState.prestoPlanner.logicalPlan(TPCHQuery1);
  }

  public static void main(final String[] args) throws Exception {

    final Options opt = args.length != 0
        ? new CommandLineOptions(args)
        : new OptionsBuilder()
            .include(PrestoBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .build();

    new Runner(opt).run();
  }
}
