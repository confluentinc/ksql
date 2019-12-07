/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.tools;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.engine.SqlFormatInjector;
import io.confluent.ksql.engine.StubInsertValuesExecutor;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjector;
import io.confluent.ksql.schema.ksql.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("deprecation")
public final class TestExecutorUtil {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final ObjectMapper PLAN_MAPPER = PlanJsonMapper.create();

  private TestExecutorUtil() {
  }

  public static List<PersistentQueryMetadata> buildQueries(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final StubKafkaService stubKafkaService
  ) {
    return doBuildQueries(testCase, serviceContext, ksqlEngine, ksqlConfig, stubKafkaService)
        .stream()
        .map(q -> q.persistentQueryMetadata)
        .collect(Collectors.toList());
  }

  static List<TopologyTestDriverContainer> buildStreamsTopologyTestDrivers(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final StubKafkaService stubKafkaService) {
    final Map<String, String> persistedConfigs = testCase.persistedProperties();
    final KsqlConfig maybeUpdatedConfigs = persistedConfigs.isEmpty() ? ksqlConfig :
        ksqlConfig.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

    final List<PersistentQueryAndSortedSources> queryMetadataList = doBuildQueries(
        testCase,
        serviceContext,
        ksqlEngine,
        maybeUpdatedConfigs,
        stubKafkaService);
    final List<TopologyTestDriverContainer> topologyTestDrivers = new ArrayList<>();
    for (final PersistentQueryAndSortedSources persistentQueryAndSortedSources:
        queryMetadataList) {
      final PersistentQueryMetadata persistentQueryMetadata = persistentQueryAndSortedSources
          .getPersistentQueryMetadata();
      final Properties streamsProperties = new Properties();
      streamsProperties.putAll(persistentQueryMetadata.getStreamsProperties());
      final Topology topology = persistentQueryMetadata.getTopology();
      final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(
          topology,
          streamsProperties,
          0);
      final List<Topic> sourceTopics = persistentQueryAndSortedSources.getSources()
          .stream()
          .map(dataSource -> {
            stubKafkaService.requireTopicExists(dataSource.getKafkaTopicName());
            return stubKafkaService.getTopic(dataSource.getKafkaTopicName());
          })
          .collect(Collectors.toList());

      final Topic sinkTopic = buildSinkTopic(
          ksqlEngine.getMetaStore().getSource(persistentQueryMetadata.getSinkName()),
          stubKafkaService,
          serviceContext.getSchemaRegistryClient());
      testCase.setGeneratedTopologies(
          ImmutableList.of(persistentQueryMetadata.getTopologyDescription()));
      testCase.setGeneratedSchemas(
          ImmutableList.of(persistentQueryMetadata.getSchemasDescription()));
      topologyTestDrivers.add(TopologyTestDriverContainer.of(
          topologyTestDriver,
          sourceTopics,
          sinkTopic
      ));
    }
    return topologyTestDrivers;
  }

  private static Topic buildSinkTopic(
      final DataSource<?> sinkDataSource,
      final StubKafkaService stubKafkaService,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final String kafkaTopicName = sinkDataSource.getKafkaTopicName();

    final Optional<org.apache.avro.Schema> avroSchema =
        getAvroSchema(sinkDataSource, schemaRegistryClient);

    final SerdeSupplier<?> keySerdeFactory = SerdeUtil.getKeySerdeSupplier(
        sinkDataSource.getKsqlTopic().getKeyFormat(),
        sinkDataSource::getSchema
    );

    final SerdeSupplier<?> valueSerdeSupplier = SerdeUtil.getSerdeSupplier(
        sinkDataSource.getKsqlTopic().getValueFormat().getFormat(),
        sinkDataSource::getSchema
    );

    final Topic sinkTopic = new Topic(
        kafkaTopicName,
        avroSchema,
        keySerdeFactory,
        valueSerdeSupplier,
        KsqlConstants.legacyDefaultSinkPartitionCount,
        KsqlConstants.legacyDefaultSinkReplicaCount
    );

    if (stubKafkaService.topicExists(sinkTopic)) {
      stubKafkaService.updateTopic(sinkTopic);
    } else {
      stubKafkaService.createTopic(sinkTopic);
    }
    return sinkTopic;
  }

  private static Optional<Schema> getAvroSchema(
      final DataSource<?> dataSource,
      final SchemaRegistryClient schemaRegistryClient) {
    if (dataSource.getKsqlTopic().getValueFormat().getFormat() == Format.AVRO) {
      try {
        final SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(
            dataSource.getKafkaTopicName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
        return Optional.of(new org.apache.avro.Schema.Parser().parse(schemaMetadata.getSchema()));
      } catch (final Exception e) {
        // do nothing
      }
    }
    return Optional.empty();
  }

  private static List<PersistentQueryAndSortedSources> doBuildQueries(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final StubKafkaService stubKafkaService
  ) {
    initializeTopics(testCase, serviceContext, stubKafkaService);

    final String sql = testCase.statements().stream()
        .collect(Collectors.joining(System.lineSeparator()));

    final List<PersistentQueryAndSortedSources> queries = execute(
        ksqlEngine,
        sql,
        ksqlConfig,
        testCase.properties(),
        Optional.of(serviceContext.getSchemaRegistryClient()),
        stubKafkaService
    );

    assertThat("test did not generate any queries.", queries, is(not(empty())));
    return queries;
  }

  private static void initializeTopics(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final StubKafkaService stubKafkaService
  ) {
    final KafkaTopicClient topicClient = serviceContext.getTopicClient();
    final SchemaRegistryClient srClient = serviceContext.getSchemaRegistryClient();

    for (final Topic topic : testCase.getTopics()) {
      stubKafkaService.createTopic(topic);
      topicClient.createTopic(
          topic.getName(),
          topic.getNumPartitions(),
          topic.getReplicas());

      topic.getSchema().ifPresent(schema -> {
        try {
          srClient
              .register(topic.getName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schema);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  /**
   * @param srClient if supplied, then schemas can be inferred from the schema registry.
   */
  private static List<PersistentQueryAndSortedSources> execute(
      final KsqlEngine engine,
      final String sql,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<SchemaRegistryClient> srClient,
      final StubKafkaService stubKafkaService
  ) {
    final List<ParsedStatement> statements = engine.parse(sql);

    final Optional<DefaultSchemaInjector> schemaInjector = srClient
        .map(SchemaRegistryTopicSchemaSupplier::new)
        .map(DefaultSchemaInjector::new);

    return statements.stream()
        .map(stmt -> execute(
            engine, stmt, ksqlConfig, overriddenProperties, schemaInjector, stubKafkaService))
        .filter(executeResultAndSortedSources ->
            executeResultAndSortedSources.getSources() != null)
        .map(
            executeResultAndSortedSources -> new PersistentQueryAndSortedSources(
                (PersistentQueryMetadata) executeResultAndSortedSources
                    .getExecuteResult().getQuery().get(),
                executeResultAndSortedSources.getSources(),
                executeResultAndSortedSources.getWindowSize()
            ))
        .collect(Collectors.toList());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static ExecuteResultAndSortedSources execute(
      final KsqlExecutionContext executionContext,
      final ParsedStatement stmt,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<DefaultSchemaInjector> schemaInjector,
      final StubKafkaService stubKafkaService
  ) {
    final PreparedStatement<?> prepared = executionContext.prepare(stmt);
    final ConfiguredStatement<?> configured = ConfiguredStatement.of(
        prepared, overriddenProperties, ksqlConfig);

    if (prepared.getStatement() instanceof InsertValues) {
      StubInsertValuesExecutor.of(stubKafkaService).execute(
          (ConfiguredStatement<InsertValues>) configured,
          overriddenProperties,
          executionContext,
          executionContext.getServiceContext()
      );
      return new ExecuteResultAndSortedSources(null, null, null);
    }

    final ConfiguredStatement<?> withSchema =
        schemaInjector
            .map(injector -> injector.inject(configured))
            .orElse((ConfiguredStatement) configured);
    final ConfiguredStatement<?> reformatted =
        new SqlFormatInjector(executionContext).inject(withSchema);

    final ExecuteResult executeResult;
    try {
      executeResult = executeConfiguredStatement(executionContext, reformatted);
    } catch (final KsqlStatementException statementException) {
      // use the original statement text in the exception so that tests
      // can easily check that the failed statement is the input statement
      throw new KsqlStatementException(
          statementException.getMessage(),
          withSchema.getStatementText(),
          statementException.getCause());
    }
    if (prepared.getStatement() instanceof CreateAsSelect) {
      return new ExecuteResultAndSortedSources(
          executeResult,
          getSortedSources(
              ((CreateAsSelect) prepared.getStatement()).getQuery(),
              executionContext.getMetaStore()),
          getWindowSize(((CreateAsSelect) prepared.getStatement()).getQuery()));
    }
    if (prepared.getStatement() instanceof InsertInto) {
      return new ExecuteResultAndSortedSources(
          executeResult,
          getSortedSources(((InsertInto) prepared.getStatement()).getQuery(),
              executionContext.getMetaStore()),
          getWindowSize(((InsertInto) prepared.getStatement()).getQuery())
      );
    }
    return new ExecuteResultAndSortedSources(
        executeResult,
        null,
        Optional.empty());
  }

  @SuppressWarnings("unchecked")
  private static ExecuteResult executeConfiguredStatement(
      final KsqlExecutionContext executionContext,
      final ConfiguredStatement<?> stmt) {
    final ConfiguredKsqlPlan configuredPlan;
    try {
      configuredPlan = buildConfiguredPlan(executionContext, stmt);
    } catch (final IOException e) {
      throw new TestFrameworkException("Error (de)serializing plan: " + e.getMessage(), e);
    }
    return executionContext.execute(executionContext.getServiceContext(), configuredPlan);
  }

  private static ConfiguredKsqlPlan buildConfiguredPlan(
      final KsqlExecutionContext executionContext,
      final ConfiguredStatement<?> stmt
  ) throws IOException {
    final KsqlPlan plan = executionContext.plan(executionContext.getServiceContext(), stmt);
    final String serialized = PLAN_MAPPER.writeValueAsString(plan);
    return ConfiguredKsqlPlan.of(
        PLAN_MAPPER.readValue(serialized, KsqlPlan.class),
        stmt.getOverrides(),
        stmt.getConfig());
  }

  private static Optional<Long> getWindowSize(final Query query) {
    return query.getWindow().flatMap(window -> window
        .getKsqlWindowExpression()
        .getWindowInfo()
        .getSize()
        .map(Duration::toMillis));
  }

  private static List<DataSource<?>> getSortedSources(
      final Query query,
      final MetaStore metaStore) {
    final Relation from = query.getFrom();
    if (from instanceof Join) {
      final Join join = (Join) from;
      final AliasedRelation left = (AliasedRelation) join.getLeft();
      final AliasedRelation right = (AliasedRelation) join.getRight();

      final SourceName leftName = ((Table) left.getRelation()).getName();
      final SourceName rightName = ((Table) right.getRelation()).getName();

      if (metaStore.getSource(leftName) == null) {
        throw new KsqlException("Source does not exist: " + left.getRelation().toString());
      }
      if (metaStore.getSource(rightName) == null) {
        throw new KsqlException("Source does not exist: " + right.getRelation().toString());
      }
      return ImmutableList.of(
          metaStore.getSource(leftName),
          metaStore.getSource(rightName));
    } else {
      final SourceName fromName = ((Table) ((AliasedRelation) from).getRelation()).getName();
      if (metaStore.getSource(fromName) == null) {
        throw new KsqlException("Source does not exist: " + fromName);
      }
      return ImmutableList.of(metaStore.getSource(fromName));
    }
  }

  private static final class ExecuteResultAndSortedSources {

    private final ExecuteResult executeResult;
    private final List<DataSource<?>> sources;
    private final Optional<Long> windowSize;

    ExecuteResultAndSortedSources(
        final ExecuteResult executeResult,
        final List<DataSource<?>> sources,
        final Optional<Long> windowSize) {
      this.executeResult = executeResult;
      this.sources = sources;
      this.windowSize = windowSize;
    }

    ExecuteResult getExecuteResult() {
      return executeResult;
    }

    List<DataSource<?>> getSources() {
      return sources;
    }

    public Optional<Long> getWindowSize() {
      return windowSize;
    }
  }

  private static final class PersistentQueryAndSortedSources {

    private final PersistentQueryMetadata persistentQueryMetadata;
    private final List<DataSource<?>> sources;
    private final Optional<Long> windowSize;

    PersistentQueryAndSortedSources(
        final PersistentQueryMetadata persistentQueryMetadata,
        final List<DataSource<?>> sources,
        final Optional<Long> windowSize
    ) {
      this.persistentQueryMetadata = persistentQueryMetadata;
      this.sources = sources;
      this.windowSize = windowSize;
    }

    PersistentQueryMetadata getPersistentQueryMetadata() {
      return persistentQueryMetadata;
    }

    List<DataSource<?>> getSources() {
      return sources;
    }

    public Optional<Long> getWindowSize() {
      return windowSize;
    }
  }
}
