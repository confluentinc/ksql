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

package io.confluent.ksql.tools.test;

import static io.confluent.ksql.util.KsqlConfig.CONNECT_REQUEST_TIMEOUT_DEFAULT;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
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
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.AssertTable;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.parser.tree.AssertStatement;
import io.confluent.ksql.parser.tree.AssertStream;
import io.confluent.ksql.parser.tree.AssertTombstone;
import io.confluent.ksql.parser.tree.AssertValues;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.DropStatement;
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
import io.confluent.ksql.services.DefaultConnectClient;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.tools.test.driver.AssertExecutor;
import io.confluent.ksql.tools.test.driver.TestDriverPipeline;
import io.confluent.ksql.tools.test.driver.TestDriverPipeline.TopicInfo;
import io.confluent.ksql.tools.test.parser.SqlTestLoader.SqlTest;
import io.confluent.ksql.tools.test.parser.TestDirective;
import io.confluent.ksql.tools.test.parser.TestStatement;
import io.confluent.ksql.tools.test.stubs.StubKafkaClientSupplier;
import io.confluent.ksql.tools.test.stubs.StubKafkaConsumerGroupClient;
import io.confluent.ksql.tools.test.stubs.StubKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class SqlTestExecutor implements Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(SqlTestExecutor.class);

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

  private ServiceContext serviceContext;
  private KsqlEngine engine;
  private KsqlConfig config;
  private TestDriverPipeline driverPipeline;
  private Injector formatInjector;
  private KafkaTopicClient topicClient;
  private Path tmpFolder;
  private final Map<String, Object> overrides;
  private final Map<QueryId, DriverAndProperties> drivers;

  // populated during execution to handle the expected exception
  // scenario - don't use Matchers because they do not create very
  // user friendly error messages
  private Class<? extends Throwable> expectedException;
  private String expectedMessage;

  public static SqlTestExecutor create(final Path tmpFolder) {
    final KafkaTopicClient topicClient = new StubKafkaTopicClient();
    final KafkaClientSupplier kafkaClientSupplier = new StubKafkaClientSupplier();
    final SchemaRegistryClient srClient = new MockSchemaRegistryClient();
    final ServiceContext serviceContext = new DefaultServiceContext(
        kafkaClientSupplier,
        () -> kafkaClientSupplier.getAdmin(Collections.emptyMap()),
        () -> kafkaClientSupplier.getAdmin(Collections.emptyMap()),
        topicClient,
        () -> srClient,
        () -> new DefaultConnectClient(
            "http://localhost:8083",
            Optional.empty(),
            Collections.emptyMap(),
            Optional.empty(),
            false,
            CONNECT_REQUEST_TIMEOUT_DEFAULT),
        DisabledKsqlClient::instance,
        new StubKafkaConsumerGroupClient()
    );
    final Map<QueryId, DriverAndProperties> drivers = new HashMap<>();
    final KsqlConfig config = new KsqlConfig(BASE_CONFIG);

    return new SqlTestExecutor(
        serviceContext,
        topicClient,
        new KsqlEngine(
            serviceContext,
            NoopProcessingLogContext.INSTANCE,
            new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get()),
            ServiceInfo.create(config),
            new SequentialQueryIdGenerator(),
            config,
            Collections.singletonList(
                new QueryEventListener() {
                  @Override
                  public void onDeregister(final QueryMetadata query) {
                    final DriverAndProperties driverAndProperties = drivers.get(
                        query.getQueryId()
                    );
                    closeDriver(
                        driverAndProperties.driver,
                        driverAndProperties.properties,
                        false,
                        tmpFolder
                    );
                  }
                }
            ),
            new MetricCollectors()
        ),
        config,
        drivers,
        tmpFolder
    );
  }

  SqlTestExecutor(
      final ServiceContext serviceContext,
      final KafkaTopicClient topicClient,
      final KsqlEngine ksqlEngine,
      final KsqlConfig config,
      final Map<QueryId, DriverAndProperties> drivers,
      final Path tmpFolder
  ) {
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.engine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.config = config.cloneWithPropertyOverwrite(ImmutableMap.of());
    this.driverPipeline = new TestDriverPipeline();
    this.formatInjector = new DefaultFormatInjector();
    this.topicClient = requireNonNull(topicClient, "topicClient");
    this.overrides = new HashMap<>();
    this.drivers = drivers;
    this.tmpFolder = requireNonNull(tmpFolder, "tmpFolder");
  }

  public void executeTest(final SqlTest test) {
    for (final TestStatement statement : test.getStatements()) {
      try {
        statement.consume(this::execute, this::doAssert, this::directive);
      } catch (final Throwable e) {
        handleExpectedException(test.getFile(), statement, e);
        return;
      }
    }

    if (expectedException != null || expectedMessage != null) {
      final String clazz = expectedException == null ? "<any>" : expectedException.getName();
      final String msg = expectedMessage == null ? "<any>" : expectedMessage;
      throw new KsqlTestException(
          Iterables.getLast(test.getStatements()),
          test.getFile(),
          "Did not get expected exception of type " + clazz + " with message " + msg
      );
    }
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
      if (injected.getStatement() instanceof DropStatement) {
        return;
      }
      final SourceName name = injected.getStatement() instanceof CreateSource
          ? ((CreateSource) injected.getStatement()).getName()
          : ((AlterSource) injected.getStatement()).getName();
      final DataSource input = engine.getMetaStore().getSource(name);
      final TopicInfo inputTopic = new TopicInfo(
          input.getKafkaTopicName(),
          keySerde(input),
          valueSerde(input)
      );
      driverPipeline.addDdlTopic(inputTopic);
      return;
    }

    final PersistentQueryMetadata query = (PersistentQueryMetadata) result.getQuery().get();
    final Topology topology = query.getTopology();
    final Properties properties = new Properties();
    properties.putAll(query.getStreamsProperties());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, tmpFolder.toString());

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

  private Serde<?> keySerde(final DataSource sinkSource) {
    final PersistenceSchema schema = PersistenceSchema.from(
        sinkSource.getSchema().key(),
        sinkSource.getKsqlTopic().getKeyFormat().getFeatures()
    );

    if (sinkSource.getKsqlTopic().getKeyFormat().getWindowInfo().isPresent()) {
      return new GenericKeySerDe().create(
          sinkSource.getKsqlTopic().getKeyFormat().getFormatInfo(),
          sinkSource.getKsqlTopic().getKeyFormat().getWindowInfo().get(),
          schema,
          config,
          serviceContext.getSchemaRegistryClientFactory(),
          "",
          NoopProcessingLogContext.INSTANCE,
          Optional.empty()
      );
    } else {
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

  private void handleExpectedException(
      final Path file, final TestStatement testStatement, final Throwable e) {
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

  private void handleExpectedClass(final TestDirective directive) throws ClassNotFoundException {
    expectedException = (Class<? extends Exception>) Class.forName(directive.getContents());
  }

  private void handleExpectedMessage(final TestDirective directive) {
    expectedMessage = directive.getContents();
  }

  private static final class DriverAndProperties {
    final TopologyTestDriver driver;
    final Properties properties;

    private DriverAndProperties(final TopologyTestDriver driver, final Properties properties) {
      this.driver = driver;
      this.properties = properties;
    }
  }

  private static void closeDriver(
      final TopologyTestDriver driver,
      final Properties properties,
      final boolean deleteState,
      final Path tmpFolder
  ) {
    // this is a hack that lets us close the driver (releasing the lock on the state
    // directory) without actually cleaning up the resources. This essentially simulates
    // the behavior we have in QueryMetadata#close vs QueryMetadata#stop
    //
    // in production we have the additional safeguard of changelog topics, but the
    // test driver doesn't support pre-loading state stores from changelog topics,
    // so we're stuck with the solution of preserving the state store
    final String appId = properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
    final File stateDir = tmpFolder.resolve(appId).toFile();
    final File tmp = tmpFolder.resolve("tmp_" + appId).toFile();

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
      topicClient.createTopic(
          statement.getProperties().getKafkaTopic(),
          statement.getProperties().getPartitions().orElse(1),
          statement.getProperties().getReplicas().orElse((short) 1),
          ImmutableMap.of()
      );
    } else if (engineStatement.getStatement() instanceof CreateAsSelect) {
      final CreateAsSelect statement = (CreateAsSelect) engineStatement.getStatement();
      topicClient.createTopic(
          statement.getProperties().getKafkaTopic()
              .orElse(statement.getName().toString(FormatOptions.noEscape()).toUpperCase()),
          statement.getProperties().getPartitions().orElse(1),
          statement.getProperties().getReplicas().orElse((short) 1),
          ImmutableMap.of()
      );
    }
  }

  public void close() {
    engine.close(true);
    serviceContext.close();
  }
}
