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

import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.kafka.schemaregistry.ParsedSchema;
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
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjector;
import io.confluent.ksql.schema.ksql.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("deprecation")
public final class TestExecutorUtil {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final ObjectMapper PLAN_MAPPER = PlanJsonMapper.create();

  private TestExecutorUtil() {
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

    final List<PersistentQueryAndSources> queryMetadataList = doBuildQueries(
        testCase,
        serviceContext,
        ksqlEngine,
        maybeUpdatedConfigs,
        stubKafkaService);
    final List<TopologyTestDriverContainer> topologyTestDrivers = new ArrayList<>();
    for (final PersistentQueryAndSources persistentQueryAndSources: queryMetadataList) {
      final PersistentQueryMetadata persistentQueryMetadata = persistentQueryAndSources
          .getPersistentQueryMetadata();
      final Properties streamsProperties = new Properties();
      streamsProperties.putAll(persistentQueryMetadata.getStreamsProperties());
      final Topology topology = persistentQueryMetadata.getTopology();
      final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(
          topology,
          streamsProperties,
          0);
      final List<Topic> sourceTopics = persistentQueryAndSources.getSources()
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
      testCase.setGeneratedSchemas(persistentQueryMetadata.getSchemasDescription());
      topologyTestDrivers.add(TopologyTestDriverContainer.of(
          topologyTestDriver,
          sourceTopics,
          sinkTopic
      ));
    }
    return topologyTestDrivers;
  }

  public static Iterable<ConfiguredKsqlPlan> planTestCase(
      final KsqlEngine engine,
      final TestCase testCase,
      final KsqlConfig ksqlConfig,
      final Optional<SchemaRegistryClient> srClient,
      final StubKafkaService stubKafkaService
  ) {
    initializeTopics(testCase, engine.getServiceContext(), stubKafkaService);
    if (testCase.getExpectedTopology().isPresent()
        && testCase.getExpectedTopology().get().getPlan().isPresent()) {
      return testCase.getExpectedTopology().get().getPlan().get()
          .stream()
          .map(p -> ConfiguredKsqlPlan.of(p, testCase.properties(), ksqlConfig))
          .collect(Collectors.toList());
    }
    return PlannedStatementIterator.of(engine, testCase, ksqlConfig, srClient, stubKafkaService);
  }

  private static Topic buildSinkTopic(
      final DataSource sinkDataSource,
      final StubKafkaService stubKafkaService,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final String kafkaTopicName = sinkDataSource.getKafkaTopicName();

    final Optional<ParsedSchema> schema = getSchema(sinkDataSource, schemaRegistryClient);

    final Topic sinkTopic = new Topic(
        kafkaTopicName,
        KsqlConstants.legacyDefaultSinkPartitionCount,
        KsqlConstants.legacyDefaultSinkReplicaCount,
        schema
    );

    if (stubKafkaService.topicExists(sinkTopic)) {
      stubKafkaService.updateTopic(sinkTopic);
    } else {
      stubKafkaService.createTopic(sinkTopic);
    }
    return sinkTopic;
  }

  private static Optional<ParsedSchema> getSchema(
      final DataSource dataSource,
      final SchemaRegistryClient schemaRegistryClient) {
    if (dataSource.getKsqlTopic().getValueFormat().getFormat().supportsSchemaInference()) {
      try {
        final String subject =
            dataSource.getKafkaTopicName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

        final SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
        return Optional.of(
            schemaRegistryClient.getSchemaBySubjectAndId(subject, metadata.getId())
        );
      } catch (final Exception e) {
        // do nothing
      }
    }
    return Optional.empty();
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
        .map(PersistentQueryAndSources::getPersistentQueryMetadata)
        .collect(Collectors.toList());
  }

  private static List<PersistentQueryAndSources> doBuildQueries(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final StubKafkaService stubKafkaService
  ) {
    final List<PersistentQueryAndSources> queries = execute(
        ksqlEngine,
        testCase,
        ksqlConfig,
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
          srClient.register(topic.getName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schema);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  /**
   * @param srClient if supplied, then schemas can be inferred from the schema registry.
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private static List<PersistentQueryAndSources> execute(
      final KsqlEngine engine,
      final TestCase testCase,
      final KsqlConfig ksqlConfig,
      final Optional<SchemaRegistryClient> srClient,
      final StubKafkaService stubKafkaService
  ) {
    final ImmutableList.Builder<PersistentQueryAndSources> queriesBuilder = new Builder<>();
    for (final ConfiguredKsqlPlan plan
        : planTestCase(engine, testCase, ksqlConfig, srClient, stubKafkaService)) {
      final ExecuteResultAndSources result = executePlan(engine, plan);
      if (!result.getSources().isPresent()) {
        continue;
      }
      queriesBuilder.add(new PersistentQueryAndSources(
          (PersistentQueryMetadata) result.getExecuteResult().getQuery().get(),
          result.getSources().get()
      ));
    }
    return queriesBuilder.build();
  }

  private static ExecuteResultAndSources executePlan(
      final KsqlExecutionContext executionContext,
      final ConfiguredKsqlPlan plan
  ) {
    final ExecuteResult executeResult = executionContext.execute(
        executionContext.getServiceContext(),
        plan
    );

    final Optional<List<DataSource>> dataSources = plan.getPlan().getQueryPlan()
        .map(queryPlan -> getSources(queryPlan.getSources(), executionContext.getMetaStore()));

    return new ExecuteResultAndSources(executeResult, dataSources);
  }

  private static List<DataSource> getSources(
      final Collection<SourceName> sources,
      final MetaStore metaStore) {
    final ImmutableList.Builder<DataSource> sourceBuilder = new Builder<>();
    for (final SourceName name : sources) {
      if (metaStore.getSource(name) == null) {
        throw new KsqlException("Source does not exist: " + name.toString());
      }
      sourceBuilder.add(metaStore.getSource(name));
    }
    return sourceBuilder.build();
  }

  private static final class PlannedStatementIterator implements
      Iterable<ConfiguredKsqlPlan>, Iterator<ConfiguredKsqlPlan> {
    private final Iterator<ParsedStatement> statements;
    private final KsqlExecutionContext executionContext;
    private final Map<String, Object> overrides;
    private final KsqlConfig ksqlConfig;
    private final StubKafkaService stubKafkaService;
    private final Optional<DefaultSchemaInjector> schemaInjector;
    private Optional<ConfiguredKsqlPlan> next = Optional.empty();

    private PlannedStatementIterator(
        final Iterator<ParsedStatement> statements,
        final KsqlExecutionContext executionContext,
        final Map<String, Object> overrides,
        final KsqlConfig ksqlConfig,
        final StubKafkaService stubKafkaService,
        final Optional<DefaultSchemaInjector> schemaInjector
    ) {
      this.statements = requireNonNull(statements, "statements");
      this.executionContext = requireNonNull(executionContext, "executionContext");
      this.overrides = requireNonNull(overrides, "overrides");
      this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
      this.stubKafkaService = requireNonNull(stubKafkaService, "stubKafkaService");
      this.schemaInjector = requireNonNull(schemaInjector, "schemaInjector");
    }

    public static PlannedStatementIterator of(
        final KsqlExecutionContext executionContext,
        final TestCase testCase,
        final KsqlConfig ksqlConfig,
        final Optional<SchemaRegistryClient> srClient,
        final StubKafkaService stubKafkaService
    ) {
      final Optional<DefaultSchemaInjector> schemaInjector = srClient
          .map(SchemaRegistryTopicSchemaSupplier::new)
          .map(DefaultSchemaInjector::new);
      final String sql = testCase.statements().stream()
          .collect(Collectors.joining(System.lineSeparator()));
      final Iterator<ParsedStatement> statements = executionContext.parse(sql).iterator();
      return new PlannedStatementIterator(
          statements,
          executionContext,
          testCase.properties(),
          ksqlConfig,
          stubKafkaService,
          schemaInjector
      );
    }

    @Override
    public boolean hasNext() {
      while (!next.isPresent() && statements.hasNext()) {
        next = planStatement(statements.next());
      }
      return next.isPresent();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public ConfiguredKsqlPlan next() {
      hasNext();
      final ConfiguredKsqlPlan current = next.orElseThrow(NoSuchElementException::new);
      next = Optional.empty();
      return current;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<ConfiguredKsqlPlan> iterator() {
      return this;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Optional<ConfiguredKsqlPlan> planStatement(final ParsedStatement stmt) {
      final PreparedStatement<?> prepared = executionContext.prepare(stmt);
      final ConfiguredStatement<?> configured = ConfiguredStatement.of(
          prepared, overrides, ksqlConfig);

      if (prepared.getStatement() instanceof InsertValues) {
        StubInsertValuesExecutor.of(stubKafkaService, executionContext).execute(
            (ConfiguredStatement<InsertValues>) configured,
            overrides,
            executionContext,
            executionContext.getServiceContext()
        );
        return Optional.empty();
      }

      final ConfiguredStatement<?> withSchema =
          schemaInjector
              .map(injector -> injector.inject(configured))
              .orElse((ConfiguredStatement) configured);
      final ConfiguredStatement<?> reformatted =
          new SqlFormatInjector(executionContext).inject(withSchema);

      try {
        final KsqlPlan plan = executionContext
            .plan(executionContext.getServiceContext(), reformatted);
        return Optional.of(
            ConfiguredKsqlPlan.of(
                rewritePlan(plan),
                reformatted.getOverrides(),
                reformatted.getConfig()
            )
        );
      } catch (final KsqlStatementException e) {
        throw new KsqlStatementException(
            e.getUnloggedMessage(), withSchema.getMaskedStatementText(), e.getCause());
      }
    }

    private static KsqlPlan rewritePlan(final KsqlPlan plan) {
      try {
        final String serialized = PLAN_MAPPER.writeValueAsString(plan);
        return PLAN_MAPPER.readValue(serialized, KsqlPlan.class);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final class ExecuteResultAndSources {

    private final ExecuteResult executeResult;
    private final Optional<List<DataSource>> sources;

    ExecuteResultAndSources(
        final ExecuteResult executeResult,
        final Optional<List<DataSource>> sources
    ) {
      this.executeResult = requireNonNull(executeResult, "executeResult");
      this.sources = requireNonNull(sources, "sources");
    }

    ExecuteResult getExecuteResult() {
      return executeResult;
    }

    Optional<List<DataSource>> getSources() {
      return sources;
    }
  }

  private static final class PersistentQueryAndSources {

    private final PersistentQueryMetadata persistentQueryMetadata;
    private final List<DataSource> sources;

    PersistentQueryAndSources(
        final PersistentQueryMetadata persistentQueryMetadata,
        final List<DataSource> sources
    ) {
      this.persistentQueryMetadata =
          requireNonNull(persistentQueryMetadata, "persistentQueryMetadata");
      this.sources = requireNonNull(sources, "sources");
    }

    PersistentQueryMetadata getPersistentQueryMetadata() {
      return persistentQueryMetadata;
    }

    List<DataSource> getSources() {
      return sources;
    }
  }
}
