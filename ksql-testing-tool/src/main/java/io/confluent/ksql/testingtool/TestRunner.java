package io.confluent.ksql.testingtool;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.schema.inference.DefaultSchemaInjector;
import io.confluent.ksql.schema.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.test.commons.TestCase;
import io.confluent.ksql.test.commons.TopologyTestDriverContainer;
import io.confluent.ksql.testingtool.services.KsqlEngineTestUtil;
import io.confluent.ksql.testingtool.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;

public final class TestRunner {

  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final static KsqlParser KSQL_PARSER = new DefaultKsqlParser();

  private static final ServiceContext serviceContext = getServiceContext();
  private static final KsqlEngine ksqlEngine = getKsqlEngine(serviceContext);
  private static final Map<String, Object> config = getConfigs(new HashMap<>());

  public static void main(final String[] args) throws IOException {

    try {
      final TestOptions testOptions = TestOptions.parse(args);

      final QttTestFile qttTestFile = OBJECT_MAPPER.readValue(
          new File(testOptions.getTestDataFile()), QttTestFile.class);

      final String statements = new String(java.nio.file.Files.readAllBytes(
          Paths.get(testOptions.getQueriesFile())), StandardCharsets.UTF_8);
      final List<ParsedStatement> parsedStatements = KSQL_PARSER.parse(statements);
      final List<String> statementsList = parsedStatements
          .stream()
          .map(ParsedStatement::getStatementText)
          .collect(Collectors.toList());

      final TestCaseNode testCaseNode = qttTestFile.tests.get(0);

      final TestCaseNode testCaseNodeWithStatements = new TestCaseNode(
          testCaseNode,
          statementsList);
      final List<TestCase> testCases = testCaseNodeWithStatements.buildTests(new File(testOptions.getTestDataFile()).toPath());
//      final List<TestCase> testCases = qttTestFile.tests.get(0).buildTests(new File(testFilePath).toPath());
      shouldBuildAndExecuteQuery(testCases.get(0));
//    shouldBuildAndExecuteQuery(qttTestFile.tests.get(0).buildTests(new File(testFilePath).toPath()).get(0));

      ksqlEngine.close();
      serviceContext.close();

      System.out.println();

    } catch (final Exception e) {
      System.err.println("Failed to start KSQL testing tool: " + e.getMessage());
      System.exit(-1);
    }

  }

  static void shouldBuildAndExecuteQuery(final TestCase testCase) {

//    final Map<String, Object> config = getConfigs(new HashMap<>());
    final KsqlConfig currentConfigs = new KsqlConfig(config);

    final Map<String, String> persistedConfigs = testCase.persistedProperties();

    final KsqlConfig ksqlConfig = persistedConfigs.isEmpty() ? currentConfigs :
        currentConfigs.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

//    try (final ServiceContext serviceContext = getServiceContext();
//        final KsqlEngine ksqlEngine = getKsqlEngine(serviceContext)) {
    try {
      testCase.initializeTopics(
          serviceContext.getTopicClient(),
          serviceContext.getSchemaRegistryClient());
//      final TopologyTestDriver testDriver = buildStreamsTopologyTestDriver(
//          testCase,
//          serviceContext,
//          ksqlEngine,
//          ksqlConfig);
      final List<TopologyTestDriverContainer> topologyTestDrivers = buildStreamsTopologyTestDriver(
          testCase,
          serviceContext,
          ksqlEngine,
          ksqlConfig
      );
      for (final TopologyTestDriverContainer topologyTestDriverContainer: topologyTestDrivers) {
        testCase.verifyTopology();
        testCase.processInput(topologyTestDriverContainer, serviceContext.getSchemaRegistryClient());
        testCase.printOutput(topologyTestDriverContainer, serviceContext.getSchemaRegistryClient());
//        testCase.verifyOutput(topologyTestDriverContainer, serviceContext.getSchemaRegistryClient());
        testCase.verifyMetastore(ksqlEngine.getMetaStore());
      }
//      testCase.verifyTopology();
//      testCase.processInput(testDriver, serviceContext.getSchemaRegistryClient());
//      testCase.verifyOutput(testDriver, serviceContext.getSchemaRegistryClient());
//      testCase.verifyMetastore(ksqlEngine.getMetaStore());
    } catch (final RuntimeException e) {
      testCase.handleException(e);
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

  private static List<TopologyTestDriverContainer> buildStreamsTopologyTestDriver(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig) {
    final Map<String, String> persistedConfigs = testCase.persistedProperties();
    final KsqlConfig maybeUpdatedConfigs = persistedConfigs.isEmpty() ? ksqlConfig :
        ksqlConfig.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

//    final PersistentQueryMetadata queryMetadata =
//        buildQuery(testCase, serviceContext, ksqlEngine, maybeUpdatedConfigs);
    final List<PersistentQueryMetadata> queryMetadataList = buildQueries(
        testCase, serviceContext, ksqlEngine, maybeUpdatedConfigs);
    final List<String> generatedTopologies = new ArrayList<>();
    final List<String> generatedSchemas = new ArrayList<>();
    final List<TopologyTestDriverContainer> topologyTestDrivers = new ArrayList<>();
    for (final PersistentQueryMetadata persistentQueryMetadata: queryMetadataList) {
      generatedTopologies.add(persistentQueryMetadata.getTopologyDescription());
      generatedSchemas.add(persistentQueryMetadata.getSchemasDescription());
      final Properties streamsProperties = new Properties();
      streamsProperties.putAll(persistentQueryMetadata.getStreamsProperties());
      final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(
          persistentQueryMetadata.getTopology(),
          streamsProperties,
          0);
      topologyTestDrivers.add(TopologyTestDriverContainer.of(
          topologyTestDriver,
          persistentQueryMetadata.getSourceNames().stream().map(s -> ksqlEngine.getMetaStore().getSource(s).getKafkaTopicName()).collect(
              Collectors.toSet()),
          persistentQueryMetadata.getSinkNames().stream().map(s -> ksqlEngine.getMetaStore().getSource(s).getKafkaTopicName()).collect(
              Collectors.toSet())
      ));
    }
    return topologyTestDrivers;
//    testCase.setGeneratedTopology(generatedTopologies);
//    testCase.setGeneratedSchemas(generatedSchemas);
//    final Properties streamsProperties = new Properties();
//    streamsProperties.putAll(queryMetadata.getStreamsProperties());
//    return new TopologyTestDriver(
//        queryMetadata.getTopology(),
//        streamsProperties,
//        0);
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

    final List<PersistentQueryMetadata> queries = execute(
        ksqlEngine,
        sql,
        ksqlConfig,
        testCase.properties(),
        Optional.of(serviceContext.getSchemaRegistryClient())
    );

    assertThat("test did not generate any queries.", queries.isEmpty(), is(false));
//    return (PersistentQueryMetadata) queries.get(queries.size() - 1);
    return queries;
  }

  /**
   * @param srClient if supplied, then schemas can be inferred from the schema registry.
   */
  public static List<PersistentQueryMetadata> execute(
      final KsqlEngine engine,
      final String sql,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<SchemaRegistryClient> srClient
  ) {
    final List<ParsedStatement> statements = engine.parse(sql);

    final Optional<DefaultSchemaInjector> schemaInjector = srClient
        .map(SchemaRegistryTopicSchemaSupplier::new)
        .map(DefaultSchemaInjector::new);

    final KsqlExecutionContext sandbox = engine.createSandbox();
    statements
        .forEach(stmt -> execute(sandbox, stmt, ksqlConfig, overriddenProperties, schemaInjector));

    return statements.stream()
        .map(stmt -> execute(engine, stmt, ksqlConfig, overriddenProperties, schemaInjector))
        .map(ExecuteResult::getQuery)
        .filter(Optional::isPresent)
        .map(q -> (PersistentQueryMetadata) q.get())
        .collect(Collectors.toList());
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  private static ExecuteResult execute(
      final KsqlExecutionContext executionContext,
      final ParsedStatement stmt,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<DefaultSchemaInjector> schemaInjector
  ) {
    final PreparedStatement<?> prepared = executionContext.prepare(stmt);
    final ConfiguredStatement<?> configured = ConfiguredStatement.of(
        prepared, overriddenProperties, ksqlConfig);
    final ConfiguredStatement<?> withSchema =
        schemaInjector
            .map(injector -> injector.inject(configured))
            .orElse((ConfiguredStatement) configured);
    return executionContext.execute(withSchema);
  }
}
