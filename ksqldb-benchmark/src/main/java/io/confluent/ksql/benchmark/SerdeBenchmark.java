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

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.avro.random.generator.Generator;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.datagen.RowGenerator;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
public class SerdeBenchmark {

  private static final Path SCHEMA_DIR = Paths.get("schemas");
  private static final String SCHEMA_FILE_SUFFIX = ".avro";
  private static final String TOPIC_NAME = "serde_benchmark";

  private static final String IMPRESSIONS_SCHEMA = "impressions";
  private static final String METRICS_SCHEMA = "metrics";
  private static final String SINGLE_KEY_SCHEMA = "single-key";

  private static final String SEPARATOR = "/";

  private static final String JSON_FORMAT = "JSON";
  private static final String AVRO_FORMAT = "Avro";
  private static final String PROTOBUF_FORMAT = "Protobuf";
  private static final String DELIMITED_FORMAT = "Delimited";
  private static final String KAFKA_FORMAT = "Kafka";

  private static final class Params {

    final String schemaName;
    final String formatName;

    static Params parse(final String text) {
      final String[] parts = text.split(SEPARATOR);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Param should be in form "
            + "'<format-name>" + SEPARATOR + "<schema-name>', got: " + text);
      }

      return new Params(parts[0], parts[1]);
    }

    private Params(final String schemaName, final String formatName) {
      this.schemaName = Objects.requireNonNull(schemaName, "schemaName");
      this.formatName = Objects.requireNonNull(formatName, "formatName").toUpperCase();
    }
  }

  @State(Scope.Thread)
  public static class SerdeState {

    Serializer<Object> serializer;
    Deserializer<Object> deserializer;
    Object data;
    byte[] bytes;

    @Param({
        SINGLE_KEY_SCHEMA + SEPARATOR + DELIMITED_FORMAT,
        SINGLE_KEY_SCHEMA + SEPARATOR + KAFKA_FORMAT,
        // SINGLE_KEY + PROTOBUF excluded as PB isn't yet supported for single key schemas
        SINGLE_KEY_SCHEMA + SEPARATOR + JSON_FORMAT,
        SINGLE_KEY_SCHEMA + SEPARATOR + AVRO_FORMAT,

        IMPRESSIONS_SCHEMA + SEPARATOR + DELIMITED_FORMAT,
        // IMPRESSIONS + KAFKA excluded as KAFKA does not support multiple columns
        IMPRESSIONS_SCHEMA + SEPARATOR + PROTOBUF_FORMAT,
        IMPRESSIONS_SCHEMA + SEPARATOR + JSON_FORMAT,
        IMPRESSIONS_SCHEMA + SEPARATOR + AVRO_FORMAT,

        // METRICS + DELIMITED_FORMAT excluded as DELIMITED does not support complex types
        // METRICS + KAFKA excluded as KAFKA does not support multiple columns
        METRICS_SCHEMA + SEPARATOR + PROTOBUF_FORMAT,
        METRICS_SCHEMA + SEPARATOR + JSON_FORMAT,
        METRICS_SCHEMA + SEPARATOR + AVRO_FORMAT
    })
    public String params;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Setup(Level.Iteration)
    public void setUp() throws IOException {
      final Params params = Params.parse(this.params);

      final RowGenerator generator = getRowGenerator(params.schemaName);

      final LogicalSchema schema = generator.schema();
      final Pair<GenericKey, GenericRow> row = generator.generateRow();

      if (params.schemaName.equals(SINGLE_KEY_SCHEMA)) {
        // Benchmark the key serde:
        final Serde<GenericKey> serde = getGenericKeySerde(schema, params.formatName);

        serializer = (Serializer) serde.serializer();
        deserializer = (Deserializer) serde.deserializer();
        data = row.getLeft();
      } else {

        // Benchmark the value serde:
        final Serde<GenericRow> serde = getGenericRowSerde(schema, params.formatName);

        serializer = (Serializer) serde.serializer();
        deserializer = (Deserializer) serde.deserializer();
        data = row.getRight();
      }

      bytes = serializer.serialize(TOPIC_NAME, data);
    }

    private static RowGenerator getRowGenerator(final String schemaName) throws IOException {
      final Generator generator = getGenerator(schemaName);

      // choose arbitrary key
      final String keyField = generator.schema().getFields().get(0).name();

      return new RowGenerator(generator, keyField, Optional.empty());
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    private static Generator getGenerator(final String schemaName) throws IOException {
      final Path schemaPath = SCHEMA_DIR.resolve(schemaName + SCHEMA_FILE_SUFFIX);

      try (InputStream schemaResource = SerdeBenchmark.class.getClassLoader()
          .getResourceAsStream(schemaPath.toString())) {

        if (schemaResource == null) {
          throw new FileNotFoundException("Schema file not found: " + schemaName);
        }

        return new Generator(schemaResource, new Random());
      }
    }

    private static FormatInfo getFormatInfo(final String formatName) {
      if (AvroFormat.NAME.equals(formatName)) {
        return FormatInfo.of(
            FormatFactory.AVRO.name(),
            ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "benchmarkSchema")
        );
      }

      return FormatInfo.of(formatName);
    }

    private static Serde<GenericKey> getGenericKeySerde(
        final LogicalSchema schema,
        final String formatName
    ) {
      final FormatInfo formatInfo = getFormatInfo(formatName);

      final SchemaRegistryClient srClient = new MockSchemaRegistryClient();

      final PersistenceSchema persistenceSchema = PersistenceSchema
          .from(schema.key(), SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

      return new GenericKeySerDe().create(
          formatInfo,
          persistenceSchema,
          new KsqlConfig(Collections.emptyMap()),
          () -> srClient,
          "benchmark",
          ProcessingLogContext.create(),
          Optional.empty()
      );
    }

    private static Serde<GenericRow> getGenericRowSerde(
        final LogicalSchema schema,
        final String formatName
    ) {
      final FormatInfo format = getFormatInfo(formatName);

      final SchemaRegistryClient srClient = new MockSchemaRegistryClient();

      return GenericRowSerDe.from(
          format,
          PersistenceSchema.from(schema.value(), SerdeFeatures.of()),
          new KsqlConfig(Collections.emptyMap()),
          () -> srClient,
          "benchmark",
          ProcessingLogContext.create()
      );
    }
  }

  @SuppressWarnings("MethodMayBeStatic") // Tests can not be static
  @Benchmark
  public byte[] serialize(final SerdeState serdeState) {
    if (serdeState.serializer == null) {
      return null;
    }

    return serdeState.serializer.serialize(TOPIC_NAME, serdeState.data);
  }

  @SuppressWarnings("MethodMayBeStatic") // Tests can not be static
  @Benchmark
  public Object deserialize(final SerdeState serdeState) {
    if (serdeState.deserializer == null) {
      return null;
    }

    return serdeState.deserializer.deserialize(TOPIC_NAME, serdeState.bytes);
  }

  public static void main(final String[] args) throws Exception {

    final Options opt = args.length != 0
        ? new CommandLineOptions(args)
        : new OptionsBuilder()
            .include(SerdeBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .build();

    new Runner(opt).run();
  }
}
