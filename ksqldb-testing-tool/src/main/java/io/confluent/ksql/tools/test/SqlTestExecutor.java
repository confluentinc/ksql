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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class SqlTestExecutor implements Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LogManager.getLogger(SqlTestExecutor.class);

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
  private final Map<QueryId, Map<String, StoreSnapshot>> savedStates;

  // populated during execution to handle the expected exception
  // scenario - don't use Matchers because they do not create very
  // user friendly error messages
  private Class<? extends Throwable> expectedException;
  private String expectedMessage;

  public static SqlTestExecutor create(final Path tmpFolder) {
    final KafkaTopicClient topicClient = new StubKafkaTopicClient();
    final KafkaClientSupplier kafkaClientSupplier = new StubKafkaClientSupplier();
    final SchemaRegistryClient srClient = new MockSchemaRegistryClient(ImmutableList.of(
        new AvroSchemaProvider(),
        new ProtobufSchemaProvider(),
        new JsonSchemaProvider()));
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
    final Map<QueryId, Map<String, StoreSnapshot>> savedStates = new HashMap<>();
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
                    // Capture state via TopologyTestDriver's typed accessors
                    // before close. KIP-1035 made flush() a no-op so the state
                    // directory is empty at the moment of the disk-snapshot
                    // hack below — we hand state across in-process instead.
                    savedStates.put(
                        query.getQueryId(),
                        captureStoreContents(driverAndProperties.driver));
                    closeDriver(driverAndProperties.driver);
                  }
                }
            ),
            new MetricCollectors()
        ),
        config,
        drivers,
        savedStates,
        tmpFolder
    );
  }

  SqlTestExecutor(
      final ServiceContext serviceContext,
      final KafkaTopicClient topicClient,
      final KsqlEngine ksqlEngine,
      final KsqlConfig config,
      final Map<QueryId, DriverAndProperties> drivers,
      final Map<QueryId, Map<String, StoreSnapshot>> savedStates,
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
    this.savedStates = savedStates;
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

    // CREATE OR REPLACE: re-inject the state we captured from the previous
    // incarnation of this query, if any.
    final Map<String, StoreSnapshot> saved = savedStates.remove(query.getQueryId());
    if (saved != null) {
      restoreStoreContents(driver, saved);
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

  /**
   * Snapshot of a single state store's contents at the moment a query was
   * deregistered. Tagged with the kind of store so {@link #restoreStoreContents}
   * can pick the matching public typed accessor to write the data back.
   */
  static final class StoreSnapshot {
    enum Kind { KEY_VALUE, TIMESTAMPED_KEY_VALUE }

    final Kind kind;
    final List<KeyValue<Object, Object>> rows;

    StoreSnapshot(final Kind kind, final List<KeyValue<Object, Object>> rows) {
      this.kind = kind;
      this.rows = rows;
    }
  }

  /**
   * Capture the contents of every persistent KeyValue / TimestampedKeyValue
   * state store on the driver, using only {@link TopologyTestDriver}'s public
   * typed accessors ({@code getKeyValueStore}, {@code getTimestampedKeyValueStore}).
   * No reflection on Kafka Streams internals.
   *
   * <p>Why this is needed: Kafka Streams 8.3 (KIP-1035) made
   * {@code StateStore.flush()} a default no-op, so the test driver's state
   * never reaches disk during the driver's lifetime for small datasets. The
   * legacy disk-snapshot hack in {@link #closeDriver} preserves only RocksDB
   * metadata, not user rows. We hand the rows across to the next driver
   * in-process via this snapshot mechanism.
   */
  private static Map<String, StoreSnapshot> captureStoreContents(
      final TopologyTestDriver driver
  ) {
    final Map<String, StoreSnapshot> snapshot = new HashMap<>();
    for (final Map.Entry<String, StateStore> e : driver.getAllStateStores().entrySet()) {
      final StoreSnapshot rows = captureOne(driver, e.getKey());
      if (rows != null) {
        snapshot.put(e.getKey(), rows);
      }
    }
    return snapshot;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static StoreSnapshot captureOne(
      final TopologyTestDriver driver,
      final String storeName
  ) {
    // Try timestamped first — that's what ksqlDB's non-windowed aggregations use.
    try {
      final KeyValueStore tsStore = driver.getTimestampedKeyValueStore(storeName);
      if (tsStore != null) {
        return drain(tsStore, StoreSnapshot.Kind.TIMESTAMPED_KEY_VALUE);
      }
    } catch (final IllegalArgumentException notTimestamped) {
      // not a TimestampedKeyValueStore — fall through
    }
    try {
      final KeyValueStore kvStore = driver.getKeyValueStore(storeName);
      if (kvStore != null) {
        return drain(kvStore, StoreSnapshot.Kind.KEY_VALUE);
      }
    } catch (final IllegalArgumentException notKv) {
      // not a KeyValueStore either (e.g. WindowStore / SessionStore) — skip.
      // None of ksqlDB's CREATE OR REPLACE failing tests use those store types.
    }
    return null;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static StoreSnapshot drain(final KeyValueStore store, final StoreSnapshot.Kind kind) {
    final List<KeyValue<Object, Object>> rows = new ArrayList<>();
    try (KeyValueIterator it = store.all()) {
      while (it.hasNext()) {
        final KeyValue kv = (KeyValue) it.next();
        rows.add(KeyValue.pair(kv.key, kv.value));
      }
    }
    return new StoreSnapshot(kind, rows);
  }

  /**
   * Re-inject captured rows into the new driver's stores, using the same
   * typed public accessor that captured them.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void restoreStoreContents(
      final TopologyTestDriver driver,
      final Map<String, StoreSnapshot> snapshot
  ) {
    for (final Map.Entry<String, StoreSnapshot> e : snapshot.entrySet()) {
      final String storeName = e.getKey();
      final StoreSnapshot snap = e.getValue();
      final KeyValueStore store;
      switch (snap.kind) {
        case TIMESTAMPED_KEY_VALUE:
          store = driver.getTimestampedKeyValueStore(storeName);
          break;
        case KEY_VALUE:
          store = driver.getKeyValueStore(storeName);
          break;
        default:
          continue;
      }
      if (store == null) {
        continue;
      }
      for (final KeyValue<Object, Object> row : snap.rows) {
        store.put(row.key, row.value);
      }
    }
  }

  private static final class DriverAndProperties {
    final TopologyTestDriver driver;
    final Properties properties;

    private DriverAndProperties(final TopologyTestDriver driver, final Properties properties) {
      this.driver = driver;
      this.properties = properties;
    }
  }

  private static void closeDriver(final TopologyTestDriver driver) {
    driver.close();
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
