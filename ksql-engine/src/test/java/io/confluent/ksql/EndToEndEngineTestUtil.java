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
package io.confluent.ksql;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.test.commons.Test;
import io.confluent.ksql.test.commons.TestCase;
import io.confluent.ksql.test.commons.TopologyAndConfigs;
import io.confluent.ksql.test.commons.TopologyTestDriverContainer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.test.TestUtils;

final class EndToEndEngineTestUtil {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String CONFIG_END_MARKER = "CONFIGS_END";
  private static final String SCHEMAS_END_MARKER = "SCHEMAS_END";

  // Pass a single test or multiple tests separated by commas to the test framework.
  // Example:
  //     mvn test -pl ksql-engine -Dtest=QueryTranslationTest -Dksql.test.files=test1.json
  //     mvn test -pl ksql-engine -Dtest=QueryTranslationTest -Dksql.test.files=test1.json,test2,json
  private static final String KSQL_TEST_FILES = "ksql.test.files";

  private EndToEndEngineTestUtil(){}

  static void writeExpectedTopologyFiles(
      final String topologyDir,
      final List<TestCase> testCases) {

    final ObjectWriter objectWriter = new ObjectMapper().writerWithDefaultPrettyPrinter();

    testCases.forEach(testCase -> {
      final Map<String, Object> originalConfigs = getConfigs(null);
      final Map<String, Object> updatedConfigs = new HashMap<>(originalConfigs);

      final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.copyOf(updatedConfigs));
      try(final ServiceContext serviceContext = getServiceContext();
          final KsqlEngine ksqlEngine = getKsqlEngine(serviceContext)) {
        final List<PersistentQueryMetadata> queries =
            buildQueries(testCase, serviceContext, ksqlEngine, ksqlConfig);
          final Map<String, String> configsToPersist
              = ksqlConfig.getAllConfigPropsWithSecretsObfuscated();
          writeExpectedTopologyFile(
              testCase.name,
              queries.get(queries.size() -1),
              configsToPersist,
              objectWriter,
              topologyDir);
      }
    });
  }

  private static List<PersistentQueryMetadata> buildQueries(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig
  ) {
    testCase.initializeTopics(
        serviceContext.getTopicClient(),
        serviceContext.getSchemaRegistryClient());

    final String sql = testCase.statements().stream()
        .collect(Collectors.joining(System.lineSeparator()));

    final List<PersistentQueryMetadata> queries = KsqlEngineTestUtil.execute(
        ksqlEngine,
        sql,
        ksqlConfig,
        testCase.properties(),
        Optional.of(serviceContext.getSchemaRegistryClient()))
        .stream()
        .map(queryMetadata -> (PersistentQueryMetadata)queryMetadata)
        .collect(Collectors.toList());

    assertThat("test did not generate any queries.", queries.isEmpty(), is(false));
    return queries;
  }

  private static TopologyTestDriverContainer buildStreamsTopologyTestDriver(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig) {
    final Map<String, String> persistedConfigs = testCase.persistedProperties();
    final KsqlConfig maybeUpdatedConfigs = persistedConfigs.isEmpty() ? ksqlConfig :
        ksqlConfig.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

    final List<PersistentQueryMetadata> queryMetadata =
        buildQueries(testCase, serviceContext, ksqlEngine, maybeUpdatedConfigs);
    final PersistentQueryMetadata persistentQueryMetadata = queryMetadata
        .get(queryMetadata.size() -1);
    testCase.setGeneratedTopologies(ImmutableList.of(persistentQueryMetadata.getTopologyDescription()));
    testCase.setGeneratedSchemas(ImmutableList.of(persistentQueryMetadata.getSchemasDescription()));
    final Properties streamsProperties = new Properties();
    streamsProperties.putAll(persistentQueryMetadata.getStreamsProperties());
    final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(
        persistentQueryMetadata.getTopology(),
        streamsProperties,
        0);
    return TopologyTestDriverContainer.of(
        topologyTestDriver,
        persistentQueryMetadata.getSourceNames()
            .stream()
            .map(s -> ksqlEngine.getMetaStore().getSource(s).getKafkaTopicName())
            .collect(Collectors.toSet()),
        persistentQueryMetadata.getSinkNames()
            .stream()
            .map(s -> ksqlEngine.getMetaStore().getSource(s).getKafkaTopicName())
            .collect(Collectors.toSet())
    );
  }

  private static void writeExpectedTopologyFile(
      final String queryName,
      final PersistentQueryMetadata query,
      final Map<String, String> configs,
      final ObjectWriter objectWriter,
      final String topologyDir
  ) {
      final Path newTopologyDataPath = Paths.get(topologyDir);
      try {
          final String updatedQueryName = formatQueryName(queryName);
          final Path topologyFile = Paths.get(newTopologyDataPath.toString(), updatedQueryName);
          final String configString = objectWriter.writeValueAsString(configs);
        final String topologyString = query.getTopology().describe().toString();
        final String schemasString = query.getSchemasDescription();

        final byte[] topologyBytes =
            (configString + "\n"
                + CONFIG_END_MARKER + "\n"
                + schemasString + "\n"
                + SCHEMAS_END_MARKER + "\n"
                + topologyString
            ).getBytes(StandardCharsets.UTF_8);

          Files.write(topologyFile,
                      topologyBytes,
                      StandardOpenOption.CREATE,
                      StandardOpenOption.WRITE,
                      StandardOpenOption.TRUNCATE_EXISTING);
      } catch (IOException e) {
          throw new RuntimeException(e);
      }
  }

  static String formatQueryName(final String originalQueryName) {
    return originalQueryName.replaceAll(" - (AVRO|JSON)$", "").replaceAll("\\s", "_");
  }

  static Map<String, TopologyAndConfigs> loadExpectedTopologies(final String dir) {
    final HashMap<String, TopologyAndConfigs> expectedTopologyAndConfigs = new HashMap<>();
    final ObjectReader objectReader = new ObjectMapper().readerFor(Map.class);
    final List<String> topologyFiles = findExpectedTopologyFiles(dir);
    topologyFiles.forEach(fileName -> {
      final TopologyAndConfigs topologyAndConfigs = readTopologyFile(dir + "/" + fileName, objectReader);
      expectedTopologyAndConfigs.put(fileName, topologyAndConfigs);
    });
    return expectedTopologyAndConfigs;
  }

  private static TopologyAndConfigs readTopologyFile(final String file, final ObjectReader objectReader) {
    try (final BufferedReader reader =
        new BufferedReader((
            new InputStreamReader(EndToEndEngineTestUtil.class.getClassLoader().
                getResourceAsStream(file), StandardCharsets.UTF_8)))) {
      final StringBuilder topologyFileBuilder = new StringBuilder();

      String schemas = null;
      String topologyAndConfigLine;
      Optional<Map<String, String>> persistedConfigs = Optional.empty();

      while ((topologyAndConfigLine = reader.readLine()) != null) {
        if (topologyAndConfigLine.contains(CONFIG_END_MARKER)) {
          persistedConfigs = Optional
              .of(objectReader.readValue(topologyFileBuilder.toString()));
          topologyFileBuilder.setLength(0);
        } else if (topologyAndConfigLine.contains(SCHEMAS_END_MARKER)) {
          schemas = StringUtils.stripEnd(topologyFileBuilder.toString(), "\n");
          topologyFileBuilder.setLength(0);
        } else {
          topologyFileBuilder.append(topologyAndConfigLine).append("\n");
        }
      }

      return new TopologyAndConfigs(
          topologyFileBuilder.toString(),
          Optional.ofNullable(schemas),
          persistedConfigs
      );

    } catch (IOException e) {
      throw new RuntimeException(String.format("Couldn't read topology file %s %s", file, e));
    }
  }

  static List<String> findExpectedTopologyDirectories(final String dir) {
    try {
      return findContentsOfDirectory(dir);
    } catch (final IOException e) {
      throw new RuntimeException("Could not find expected topology directories.", e);
    }
  }

  private static List<String> findExpectedTopologyFiles(final String dir) {
    try {
      return findContentsOfDirectory(dir);
    } catch (final IOException e) {
      throw new RuntimeException("Could not find expected topology files. dir: " + dir, e);
    }
  }

  private static List<String> findContentsOfDirectory(final String dir) throws IOException {
    final List<String> contents = new ArrayList<>();
    try (final BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(EndToEndEngineTestUtil.class.getClassLoader().
                getResourceAsStream(dir), StandardCharsets.UTF_8))) {

      String file;
      while ((file = reader.readLine()) != null) {
        contents.add(file);
      }
    }
    return contents;
  }

  private static List<Path> findTests(final Path dir) {
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(EndToEndEngineTestUtil.class.getClassLoader().
            getResourceAsStream(dir.toString()), StandardCharsets.UTF_8))) {

      final List<Path> tests = new ArrayList<>();

      String test;
      while ((test = reader.readLine()) != null) {
        if (test.endsWith(".json")) {
          tests.add(dir.resolve(test));
        }
      }
      return tests;
    } catch (IOException e) {
      throw new AssertionError("Invalid test - failed to read dir: " + dir);
    }
  }

  private static List<Path> getTests(final Path dir, final List<String> files) {
    return files.stream().map(name -> dir.resolve(name.trim())).collect(Collectors.toList());
  }

  /**
   * Returns a list of files specified in the system property 'ksql.test.file'.
   * The list may be specified as a comma-separated string. If 'ksql.test.file' is not found,
   * then an empty list is returned.
   */
  static List<String> getTestFilesParam() {
    final String ksqlTestFiles = System.getProperty(KSQL_TEST_FILES, "").trim();
    if (ksqlTestFiles.isEmpty()) {
      return Collections.emptyList();
    }

    return Arrays.asList(ksqlTestFiles.split(","));
  }


  interface TestFile<TestType extends Test> {

    Stream<TestType> buildTests(final Path testPath);
  }

  static <TF extends TestFile<T>, T extends Test> Stream<T> findTestCases(
      final Path dir,
      final List<String> files,
      final Class<TF> testFileType
  ) {
    final List<T> testCases = getTestPaths(dir, files).stream()
        .flatMap(testPath -> buildTests(testPath, testFileType))
        .collect(Collectors.toList());

    throwOnDuplicateNames(testCases);

    return testCases.stream();
  }

  private static void throwOnDuplicateNames(final List<? extends Test> testCases) {
    final String duplicates = testCases.stream()
        .collect(Collectors.groupingBy(Test::getName))
        .entrySet()
        .stream()
        .filter(e -> e.getValue().size() > 1)
        .map(e -> "test name: '" + e.getKey()
            + "' found in files: " + e.getValue().stream().map(Test::getTestFile)
            .collect(Collectors.joining(",")))
        .collect(Collectors.joining(System.lineSeparator()));

    if (!duplicates.isEmpty()) {
      throw new IllegalStateException("There are tests with duplicate names: "
          + System.lineSeparator() + duplicates);
    }
  }

  /**
   * Return a list of test paths found on the given directory. If the files parameter is not empty,
   * then returns only the paths of the given list.
   */
  private static List<Path> getTestPaths(final Path dir, final List<String> files) {
    if (files != null && !files.isEmpty()) {
      return getTests(dir, files);
    } else {
      return findTests(dir);
    }
  }

  private static <TF extends TestFile<T>, T extends Test> Stream<T> buildTests(
      final Path testPath,
      final Class<TF> testFileType
  ) {
    try (InputStream stream = EndToEndEngineTestUtil.class
        .getClassLoader()
        .getResourceAsStream(testPath.toString())
    ) {
      final TF testFile = OBJECT_MAPPER.readValue(stream, testFileType);
      return testFile.buildTests(testPath);
    } catch (Exception e) {
      throw new RuntimeException("Unable to load test at path " + testPath, e);
    }
  }

  private static ServiceContext getServiceContext() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    return TestServiceContext.create(() -> schemaRegistryClient);
  }

  private static KsqlEngine getKsqlEngine(final ServiceContext serviceContext) {
    final MutableMetaStore metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    return KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
  }

  private static Map<String, Object> getConfigs(final Map<String, Object> additionalConfigs) {

    ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0")
        .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0)
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
        .put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "some.ksql.service.id")
        .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "some.ksql.service.id")
        .put(
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_ON)
        .put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");

      if(additionalConfigs != null){
          mapBuilder.putAll(additionalConfigs);
      }
      return mapBuilder.build();

  }

  static void shouldBuildAndExecuteQuery(final TestCase testCase) {

    final Map<String, Object> config = getConfigs(new HashMap<>());
    final KsqlConfig currentConfigs = new KsqlConfig(config);

    final Map<String, String> persistedConfigs = testCase.persistedProperties();

    final KsqlConfig ksqlConfig = persistedConfigs.isEmpty() ? currentConfigs :
        currentConfigs.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

    try (final ServiceContext serviceContext = getServiceContext();
        final KsqlEngine ksqlEngine = getKsqlEngine(serviceContext)) {
      testCase.initializeTopics(
          serviceContext.getTopicClient(),
          serviceContext.getSchemaRegistryClient());
      final TopologyTestDriverContainer topologyTestDriverContainer =
          buildStreamsTopologyTestDriver(
              testCase,
              serviceContext,
              ksqlEngine,
              ksqlConfig);
      testCase.verifyTopology();
      testCase.processInput(
          topologyTestDriverContainer,
          serviceContext.getSchemaRegistryClient());
      testCase.verifyOutput(
          topologyTestDriverContainer,
          serviceContext.getSchemaRegistryClient());
      testCase.verifyMetastore(ksqlEngine.getMetaStore());
    } catch (final RuntimeException e) {
      testCase.handleException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static Object valueSpecToAvro(final Object spec, final org.apache.avro.Schema schema) {
    if (spec == null) {
      return null;
    }
    switch (schema.getType()) {
      case INT:
        return Integer.valueOf(spec.toString());
      case LONG:
        return Long.valueOf(spec.toString());
      case STRING:
        return spec.toString();
      case DOUBLE:
        return Double.valueOf(spec.toString());
      case FLOAT:
        return Float.valueOf(spec.toString());
      case BOOLEAN:
        return spec;
      case ARRAY:
        return ((List)spec).stream()
            .map(o -> valueSpecToAvro(o, schema.getElementType()))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<Object, Object>)spec).entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> valueSpecToAvro(e.getValue(), schema.getValueType())
            )
        );
      case RECORD:
        final GenericRecord record = new GenericData.Record(schema);
        for (final org.apache.avro.Schema.Field field : schema.getFields()) {
          record.put(
              field.name(),
              valueSpecToAvro(((Map<String, ?>)spec).get(field.name()), field.schema())
          );
        }
        return record;
      case UNION:
        for (final org.apache.avro.Schema memberSchema : schema.getTypes()) {
          if (!memberSchema.getType().equals(org.apache.avro.Schema.Type.NULL)) {
            return valueSpecToAvro(spec, memberSchema);
          }
        }
        throw new RuntimeException("Union must have non-null type: " + schema.getType().getName());

      default:
        throw new RuntimeException(
            "This test does not support the data type yet: " + schema.getType().getName());
    }
  }

  @SuppressWarnings("unchecked")
  static Object avroToValueSpec(final Object avro,
                                final org.apache.avro.Schema schema,
                                final boolean toUpper) {
    if (avro == null) {
      return null;
    }
    switch (schema.getType()) {
      case INT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return avro;
      case LONG:
        // Ensure that smaller long values match the value spec from the test file.
        // The json deserializer uses Integer for any number less than Integer.MAX_VALUE.
        if (((Long)avro) < Integer.MAX_VALUE && ((Long)avro) > Integer.MIN_VALUE) {
          return ((Long)avro).intValue();
        }
        return avro;
      case ENUM:
      case STRING:
        return avro.toString();
      case ARRAY:
        if (schema.getElementType().getName().equals(AvroData.MAP_ENTRY_TYPE_NAME) ||
            Objects.equals(
                schema.getElementType().getProp(AvroData.CONNECT_INTERNAL_TYPE_NAME),
                AvroData.MAP_ENTRY_TYPE_NAME)
            ) {
          final org.apache.avro.Schema valueSchema
              = schema.getElementType().getField("value").schema();
          return ((List) avro).stream().collect(
              Collectors.toMap(
                  m -> ((GenericData.Record) m).get("key").toString(),
                  m -> (avroToValueSpec(((GenericData.Record) m).get("value"), valueSchema, toUpper))
              )
          );
        }
        return ((List)avro).stream()
            .map(o -> avroToValueSpec(o, schema.getElementType(), toUpper))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<Object, Object>)avro).entrySet().stream().collect(
                Collectors.toMap(
                    e -> e.getKey().toString(),
                    e -> avroToValueSpec(e.getValue(), schema.getValueType(), toUpper)
                )
            );
      case RECORD:
        final Map<String, Object> recordSpec = new HashMap<>();
        schema.getFields().forEach(
            f -> recordSpec.put(
                toUpper ? f.name().toUpperCase() : f.name(),
                avroToValueSpec(
                    ((GenericData.Record)avro).get(f.name()),
                    f.schema(),
                    toUpper)
            )
        );
        return recordSpec;
      case UNION:
        final int pos = GenericData.get().resolveUnion(schema, avro);
        final boolean hasNull = schema.getTypes().stream()
            .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
        final Object resolved = avroToValueSpec(avro, schema.getTypes().get(pos), toUpper);
        if (schema.getTypes().get(pos).getType().equals(org.apache.avro.Schema.Type.NULL)
          || schema.getTypes().size() == 2 && hasNull) {
          return resolved;
        }
        final Map<String, Object> ret = Maps.newHashMap();
        schema.getTypes()
            .forEach(
              s -> ret.put(s.getName().toUpperCase(), null));
        ret.put(schema.getTypes().get(pos).getName().toUpperCase(), resolved);
        return ret;
      default:
        throw new RuntimeException("Test cannot handle data of type: " + schema.getType());
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  static String buildTestName(
      final Path testPath,
      final String testName,
      final String postfix
  ) {
    final String fileName = com.google.common.io.Files.getNameWithoutExtension(testPath.toString());
    final String pf = postfix.isEmpty() ? "" : " - " + postfix;
    return fileName + " - " + testName + pf;
  }

  static Optional<org.apache.avro.Schema> buildAvroSchema(final JsonNode schema) {
    if (schema instanceof NullNode) {
      return Optional.empty();
    }

    try {
      final String schemaString = OBJECT_MAPPER.writeValueAsString(schema);
      final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
      return Optional.of(parser.parse(schemaString));
    } catch (final Exception e) {
      throw new InvalidFieldException("schema", "failed to parse", e);
    }
  }

  static final class MissingFieldException extends RuntimeException {

    MissingFieldException(final String fieldName) {
      super("test must define '" + fieldName + "' field");
    }
  }

  static final class InvalidFieldException extends RuntimeException {

    InvalidFieldException(
        final String fieldName,
        final String reason
    ) {
      super(fieldName + ": " + reason);
    }

    InvalidFieldException(
        final String fieldName,
        final String reason,
        final Throwable cause
    ) {
      super(fieldName + ": " + reason, cause);
    }
  }
}
