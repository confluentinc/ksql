/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.benchmark;

import static io.confluent.ksql.datagen.DataGenSchemaUtil.getOptionalSchema;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.datagen.RowGenerator;
import io.confluent.ksql.datagen.SessionManager;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public final class SerdeBenchmark {

  private static final String SCHEMA_DIR = "ksql-benchmark/src/resources/schemas";
  private static final String topicName = "serde_benchmark";

  private SerdeBenchmark() {
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 5, time = 10)
  @Measurement(iterations = 5, time = 10)
  @Threads(4)
  public abstract static class AbstractBenchmark {

    @State(Scope.Thread)
    public static class SchemaAndGenericRowState {
      org.apache.kafka.connect.data.Schema schema;
      GenericRow row;

      @Param({"impressions", "metrics"})
      public String schemaName;

      @Setup(Level.Iteration)
      public void setUp() throws Exception {
        // from DataGen
        final Generator generator = new Generator(getSchemaFile(), new Random());

        // from DataGenProducer
        final Schema avroSchema = generator.schema();
        final AvroData avroData = new AvroData(1);
        final org.apache.kafka.connect.data.Schema ksqlSchema =
            getOptionalSchema(avroData.toConnectSchema(avroSchema));

        // choose arbitrary String key
        String key = "";
        for (final Field field : ksqlSchema.fields()) {
          if (field.schema().type().getName().equals("string")) {
            key = field.name();
            break;
          }
        }

        // from DataGenProducer (continued)
        final SessionManager sessionManager = new SessionManager();
        final RowGenerator rowGenerator =
            new RowGenerator(generator, avroData, avroSchema, ksqlSchema, sessionManager, key);
        final Pair<String, GenericRow> genericRowPair = rowGenerator.generateRow();
        row = genericRowPair.getRight();
        schema = ksqlSchema;
      }

      private File getSchemaFile() {
        return Paths.get(SCHEMA_DIR, schemaName + ".avro").toFile();
      }
    }

    @State(Scope.Thread)
    public abstract static class SerdeState {
      Serializer<GenericRow> serializer;
      Deserializer<GenericRow> deserializer;
      GenericRow row;
      byte[] bytes;

      @Setup(Level.Iteration)
      public void setUp(final SchemaAndGenericRowState rowState) {
        final Serde<GenericRow> serde = getSerde(rowState.schema);
        serializer = serde.serializer();
        deserializer = serde.deserializer();
        row = rowState.row;
        bytes = serializer.serialize(topicName, row);
      }

      abstract Serde<GenericRow> getSerde(org.apache.kafka.connect.data.Schema schema);
    }
  }

  public static class JsonBenchmark extends AbstractBenchmark {

    public static class JsonSerdeState extends SerdeState {

      @Override
      Serde<GenericRow> getSerde(final org.apache.kafka.connect.data.Schema schema) {
        final Serializer<GenericRow> serializer = getJsonSerde(schema).serializer();
        // KsqlJsonDeserializer requires schema field names to be uppercase
        final Deserializer<GenericRow> deserializer =
            getJsonSerde(convertFieldNamesToUppercase(schema)).deserializer();
        return Serdes.serdeFrom(serializer, deserializer);
      }
    }

    @Benchmark
    public byte[] serialize(final JsonSerdeState serdeState) {
      return serdeState.serializer.serialize(topicName, serdeState.row);
    }

    @Benchmark
    public GenericRow deserialize(final JsonSerdeState serdeState) {
      return serdeState.deserializer.deserialize(topicName, serdeState.bytes);
    }

    private static org.apache.kafka.connect.data.Schema convertFieldNamesToUppercase(
        final org.apache.kafka.connect.data.Schema schema) {
      SchemaBuilder builder = SchemaBuilder.struct();
      for (final Field field : schema.fields()) {
        builder = builder.field(field.name().toUpperCase(), field.schema());
      }
      return builder.build();
    }

    private static Serde<GenericRow> getJsonSerde(
        final org.apache.kafka.connect.data.Schema schema) {
      return new KsqlJsonTopicSerDe().getGenericRowSerde(
          schema,
          new KsqlConfig(Collections.emptyMap()),
          false,
          () -> null,
          "benchmark");
    }
  }

  public static class AvroBenchmark extends AbstractBenchmark {

    public static class AvroSerdeState extends SerdeState {

      @Override
      Serde<GenericRow> getSerde(final org.apache.kafka.connect.data.Schema schema) {
        final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
        return new KsqlAvroTopicSerDe("benchmarkSchema").getGenericRowSerde(
            schema,
            new KsqlConfig(Collections.emptyMap()),
            false,
            () -> schemaRegistryClient,
            "benchmark");
      }
    }

    @Benchmark
    public byte[] serialize(final AvroSerdeState serdeState) {
      return serdeState.serializer.serialize(topicName, serdeState.row);
    }

    @Benchmark
    public GenericRow deserialize(final AvroSerdeState serdeState) {
      return serdeState.deserializer.deserialize(topicName, serdeState.bytes);
    }
  }

  public static void main(final String[] args) throws RunnerException {
    final Options opt = new OptionsBuilder()
        .include(SerdeBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}
