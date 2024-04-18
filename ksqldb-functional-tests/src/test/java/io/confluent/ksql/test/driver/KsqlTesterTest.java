/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.driver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.engine.generic.GenericRecordFactory;
import io.confluent.ksql.engine.generic.KsqlGenericRecord;
import io.confluent.ksql.format.DefaultFormatInjector;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.parser.AssertTable;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AssertStatement;
import io.confluent.ksql.parser.tree.AssertStream;
import io.confluent.ksql.parser.tree.AssertTombstone;
import io.confluent.ksql.parser.tree.AssertValues;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.properties.PropertyOverrider;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.test.KsqlTestException;
import io.confluent.ksql.test.driver.TestDriverPipeline.TopicInfo;
import io.confluent.ksql.test.parser.SqlTestLoader;
import io.confluent.ksql.test.parser.TestDirective;
import io.confluent.ksql.test.parser.TestStatement;
import io.confluent.ksql.test.tools.TestFunctionRegistry;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class KsqlTesterTest {
  private static final Logger LOG = LoggerFactory.getLogger(KsqlTesterTest.class);

  private static final String TEST_DIR = "/sql-tests";

  private static final ImmutableMap<String, Object> BASE_CONFIG = ImmutableMap
      .<String, Object>builder()
      .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0")
      .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0)
      .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
      .put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 0L)
      .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "some.ksql.service.id")
      .put(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
      .build();

  @Rule
  public final TemporaryFolder tmpFolder = KsqlTestFolder.temporaryFolder();

  // parameterized
  private final Path file;
  private final List<TestStatement> statements;

  // initialized in setUp
  private ServiceContext serviceContext;
  private KsqlEngine engine;
  private KsqlConfig config;
  private Injector formatInjector;
  private TestDriverPipeline driverPipeline;
  private FakeKafkaTopicClient topicClient;

  // populated during run
  private Map<String, Object> overrides;
  private final Map<QueryId, DriverAndProperties> drivers = new HashMap<>();

  // populated during execution to handle the expected exception
  // scenario - don't use Matchers because they do not create very
  // user friendly error messages
  private Class<? extends Throwable> expectedException;
  private String expectedMessage;

  @Parameterized.Parameters(name = "{0}")
  public static Object[][] data() throws IOException {
    final Path testDir = Paths.get(KsqlTesterTest.class.getResource(TEST_DIR).getFile());
    final SqlTestLoader loader = new SqlTestLoader(testDir);
    return loader.load()
        .map(test -> new Object[]{
            "(" + test.getFile().getParent().toFile().getName()
                + "/" + test.getFile().toFile().getName() + ") "
                + test.getName(),
            test.getFile(),
            test.getStatements()})
        .toArray(Object[][]::new);
  }

  @SuppressWarnings("unused")
  public KsqlTesterTest(final String testCase, final Path file, final List<TestStatement> statements) {
    this.file = Objects.requireNonNull(file, "file");
    this.statements = ImmutableList.copyOf(statements);
  }

  @Before
  public void setUp() {
    final MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();
    this.topicClient = new FakeKafkaTopicClient();
    this.serviceContext = TestServiceContext.create(topicClient, () -> srClient);
    this.config = new KsqlConfig(BASE_CONFIG);
    this.formatInjector = new DefaultFormatInjector();

    final MetaStoreImpl metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    final MetricCollectors metricCollectors = new MetricCollectors();
    this.engine = new KsqlEngine(
        serviceContext,
        NoopProcessingLogContext.INSTANCE,
        metaStore,
        ServiceInfo.create(config),
        new SequentialQueryIdGenerator(),
        this.config,
        Collections.singletonList(
            new QueryEventListener() {
              @Override
              public void onDeregister(QueryMetadata query) {
                final DriverAndProperties driverAndProperties = drivers.get(
                    query.getQueryId()
                );
                closeDriver(driverAndProperties.driver, driverAndProperties.properties, false);
              }
            }
        ),
        metricCollectors
    );

    this.expectedException = null;
    this.expectedMessage = null;

    this.overrides = new HashMap<>();
    this.driverPipeline = new TestDriverPipeline();
  }

  @After
  public void close() {
    engine.close(true);
    serviceContext.close();
  }

  @Test
  public void test() {
    for (final TestStatement testStatement : statements) {
      try {
        testStatement.consume(this::execute, this::doAssert, this::directive);
      } catch (final Throwable e) {
        handleExpectedException(testStatement, e);
        return;
      }
    }

    if (expectedException != null || expectedMessage != null) {
      final String clazz = expectedException == null ? "<any>" : expectedException.getName();
      final String msg = expectedMessage == null ? "<any>" : expectedMessage;
      throw new KsqlTestException(
          Iterables.getLast(statements),
          file,
          "Did not get expected exception of type " + clazz + " with message " + msg
      );
    }
  }

  @SuppressWarnings("unchecked")
  private void execute(final ParsedStatement parsedStatement) {
    final PreparedStatement<?> engineStatement = engine.prepare(parsedStatement);
    final ConfiguredStatement<?> configured = ConfiguredStatement
        .of(engineStatement, SessionConfig.of(config, overrides));

    createTopics(engineStatement);

    if (engineStatement.getStatement() instanceof InsertValues) {
      pipeInput((ConfiguredStatement<InsertValues>) configured);
      return;
    } else if (engineStatement.getStatement() instanceof SetProperty) {
      PropertyOverrider.set((ConfiguredStatement<SetProperty>) configured, overrides);
      return;
    } else if (engineStatement.getStatement() instanceof UnsetProperty) {
      PropertyOverrider.unset((ConfiguredStatement<UnsetProperty>) configured, overrides);
      return;
    }

    final ConfiguredStatement<?> injected = formatInjector.inject(configured);
    final ExecuteResult result = engine.execute(
        serviceContext,
        injected);

    // is DDL statement
    if (!result.getQuery().isPresent()) {
      return;
    }

    final PersistentQueryMetadata query = (PersistentQueryMetadata) result.getQuery().get();
    final Topology topology = query.getTopology();
    final Properties properties = new Properties();
    properties.putAll(query.getStreamsProperties());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, tmpFolder.getRoot().getAbsolutePath());

    final TopologyTestDriver driver = new TopologyTestDriver(topology, properties);

    final List<TopicInfo> inputTopics = query
        .getSourceNames()
        .stream()
        .map(sn -> engine.getMetaStore().getSource(sn))
        .map(ds -> new TopicInfo(ds.getKafkaTopicName(), keySerde(ds), valueSerde(ds)))
        .collect(Collectors.toList());

    // Sink may be Optional for source tables. Once source table query execution is supported, then
    // we would need have a condition to not create an output topic info
    final DataSource output = engine.getMetaStore().getSource(query.getSinkName().get());
    final TopicInfo outputInfo = new TopicInfo(
        output.getKafkaTopicName(),
        keySerde(output),
        valueSerde(output)
    );

    driverPipeline.addDriver(driver, inputTopics, outputInfo);
    drivers.put(query.getQueryId(), new DriverAndProperties(driver, properties));
  }

  private void closeDriver(
      final TopologyTestDriver driver,
      final Properties properties,
      final boolean deleteState
  ) {
    // this is a hack that lets us close the driver (releasing the lock on the state
    // directory) without actually cleaning up the resources. This essentially simulates
    // the behavior we have in QueryMetadata#close vs QueryMetadata#stop
    //
    // in production we have the additional safeguard of changelog topics, but the
    // test driver doesn't support pre-loading state stores from changelog topics,
    // so we're stuck with the solution of preserving the state store
    final String appId = properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
    final File stateDir = tmpFolder.getRoot().toPath().resolve(appId).toFile();
    final File tmp = tmpFolder.getRoot().toPath().resolve("tmp_" + appId).toFile();

    if (!deleteState && stateDir.exists()) {
      try {
        FileUtils.copyDirectory(stateDir, tmp);
      } catch (final IOException e) {
        if (!(e instanceof NoSuchFileException)) {
          throw new KsqlException(e);
        } else {
          // Log a warning instead of throwing an exception when the state directory does not
          // exist. The file could've been deleted manually by external factors.
          LOG.warn("The state or temp directory '{}' do not exist. "
                  + "The test will continue closing the driver.",
              ((NoSuchFileException) e).getFile());
        }
      }
    }

    try {
      driver.close();

      if (tmp.exists()) {
        FileUtils.copyDirectory(tmp, stateDir);
        FileUtils.deleteDirectory(tmp);
      }
    } catch (final IOException e) {
      throw new KsqlException(e);
    }
  }

  private void createTopics(final PreparedStatement<?> engineStatement) {
    if (engineStatement.getStatement() instanceof CreateSource) {
      final CreateSource statement = (CreateSource) engineStatement.getStatement();
      topicClient.preconditionTopicExists(
          statement.getProperties().getKafkaTopic(),
          statement.getProperties().getPartitions().orElse(1),
          statement.getProperties().getReplicas().orElse((short) 1),
          ImmutableMap.of()
      );
    } else if (engineStatement.getStatement() instanceof CreateAsSelect) {
      final CreateAsSelect statement = (CreateAsSelect) engineStatement.getStatement();
      topicClient.preconditionTopicExists(
          statement.getProperties().getKafkaTopic()
              .orElse(statement.getName().toString(FormatOptions.noEscape()).toUpperCase()),
          statement.getProperties().getPartitions().orElse(1),
          statement.getProperties().getReplicas().orElse((short) 1),
          ImmutableMap.of()
      );
    }
  }

  private void pipeInput(final ConfiguredStatement<InsertValues> statement) {
    final InsertValues insertValues = statement.getStatement();
    final DataSource dataSource = engine.getMetaStore().getSource(insertValues.getTarget());
    if (dataSource == null) {
      throw new KsqlException("Unknown data source " + insertValues.getTarget());
    }

    final KsqlGenericRecord record = new GenericRecordFactory(
        config, engine.getMetaStore(), System::currentTimeMillis
    ).build(
        insertValues.getColumns(),
        insertValues.getValues(),
        dataSource.getSchema(),
        dataSource.getDataSourceType()
    );
    driverPipeline.pipeInput(
        dataSource.getKafkaTopicName(),
        record.key,
        record.value,
        record.ts
    );
  }

  private void doAssert(final AssertStatement statement) {
    if (statement instanceof AssertValues) {
      AssertExecutor.assertValues(engine, config, (AssertValues) statement, driverPipeline);
    } else if (statement instanceof AssertTombstone) {
      AssertExecutor.assertTombstone(engine, config, (AssertTombstone) statement, driverPipeline);
    } else if (statement instanceof AssertStream) {
      AssertExecutor.assertStream(engine, config, ((AssertStream) statement));
    } else if (statement instanceof AssertTable) {
      AssertExecutor.assertTable(engine, config, ((AssertTable) statement));
    }
  }

  private Serde<GenericKey> keySerde(final DataSource sinkSource) {
    final PersistenceSchema schema = PersistenceSchema.from(
        sinkSource.getSchema().key(),
        sinkSource.getKsqlTopic().getKeyFormat().getFeatures()
    );

    return new GenericKeySerDe().create(
        sinkSource.getKsqlTopic().getKeyFormat().getFormatInfo(),
        schema,
        config,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    );
  }

  private Serde<GenericRow> valueSerde(final DataSource sinkSource) {
    return GenericRowSerDe.from(
        sinkSource.getKsqlTopic().getValueFormat().getFormatInfo(),
        PersistenceSchema.from(
            sinkSource.getSchema().value(),
            sinkSource.getKsqlTopic().getValueFormat().getFeatures()
        ),
        config,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    );
  }

  private void directive(final TestDirective directive) {
    try {
      switch (directive.getType()) {
        case EXPECTED_ERROR:
          handleExpectedClass(directive);
          break;
        case EXPECTED_MESSAGE:
          handleExpectedMessage(directive);
          break;
        default:
      }
    } catch (final Exception e) {
      throw new KsqlException("Failed to handle directive " + directive, e);
    }
  }

  private void handleExpectedException(final TestStatement testStatement, final Throwable e) {
    if (expectedException == null && expectedMessage == null) {
      throw new KsqlTestException(testStatement, file, e);
    }

    if (!e.getMessage().contains(expectedMessage)) {
      throw new KsqlTestException(
          testStatement,
          file,
          "Expected exception with message \"" + expectedMessage + "\" but got " + e);
    }

    if (!expectedException.isInstance(e)) {
      throw new KsqlTestException(
          testStatement,
          file,
          "Expected exception with class " + expectedException + " but got " + e);
    }
  }

  @SuppressWarnings("unchecked")
  private void handleExpectedClass(final TestDirective directive) throws ClassNotFoundException {
    expectedException = (Class<? extends Exception>) Class.forName(directive.getContents());
  }

  private void handleExpectedMessage(final TestDirective directive) {
    expectedMessage = directive.getContents();
  }

  private static class DriverAndProperties {
    final TopologyTestDriver driver;
    final Properties properties;

    private DriverAndProperties(final TopologyTestDriver driver, final Properties properties) {
      this.driver = driver;
      this.properties = properties;
    }
  }
}
