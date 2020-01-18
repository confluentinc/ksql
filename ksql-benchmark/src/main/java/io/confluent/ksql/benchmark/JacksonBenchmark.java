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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
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
@Warmup(iterations = 4, time = 10)
@Measurement(iterations = 4, time = 10)
@Threads(4)
@Fork(3)
public class JacksonBenchmark {

  @State(Scope.Thread)
  public static class JacksonState {

    public ObjectMapper objectMapper;
    private String serializedString =
        "{\"sql\":\"select * from foo\",\"push\":false,\"properties\":{\"prop1\":\"val1\",\"prop2\":23}}";
    public byte[] serializedBytes;

    @Setup(Level.Iteration)
    public void setUp() throws Exception {
      objectMapper = DatabindCodec.mapper();
      serializedBytes = serializedString.getBytes("UTF-8");
    }
  }

  @Benchmark
  public int deserializeWithObjectMapper(final JacksonState state) throws Exception {
    final ObjectMapper mapper = state.objectMapper;
    final QueryStreamArgs args = mapper.readValue(state.serializedBytes, QueryStreamArgs.class);
    return args.push ? 1 : 0;
  }

  @Benchmark
  public int deserializeWithParser(final JacksonState state) throws Exception {
    final JsonObject jsonObject = new JsonObject(Buffer.buffer(state.serializedBytes));
    return jsonObject.size();
  }

  public static void main(final String[] args) throws RunnerException {
    final Options opt = new OptionsBuilder()
        .include(JacksonBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}

