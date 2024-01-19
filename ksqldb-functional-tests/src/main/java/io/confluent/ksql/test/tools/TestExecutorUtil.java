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

import static io.confluent.ksql.util.KsqlConstants.getSRSubject;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.engine.StubInsertValuesExecutor;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.format.DefaultFormatInjector;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjector;
import io.confluent.ksql.schema.ksql.inference.SchemaRegisterInjector;
import io.confluent.ksql.schema.ksql.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.InjectorChain;
import io.confluent.ksql.statement.SourcePropertyInjector;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.tools.test.model.Topic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hamcrest.StringDescription;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("deprecation")
public final class TestExecutorUtil {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final ObjectMapper PLAN_MAPPER = PlanJsonMapper.INSTANCE.get();

  private TestExecutorUtil() {
  }

  static List<TopologyTestDriverContainer> buildStreamsTopologyTestDrivers(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final StubKafkaService stubKafkaService,
      final TestExecutionListener listener
  ) {
    final KsqlConfig maybeUpdatedConfigs = testCase.applyPersistedProperties(ksqlConfig);

    final List<PersistentQueryAndSources> queryMetadataList = doBuildQueries(
        testCase,
        serviceContext,
        ksqlEngine,
        maybeUpdatedConfigs,
        stubKafkaService,
        listener
    );

    final List<TopologyTestDriverContainer> topologyTestDrivers = new ArrayList<>();
    for (final PersistentQueryAndSources persistentQueryAndSources : queryMetadataList) {
      final PersistentQueryMetadata persistentQueryMetadata = persistentQueryAndSources
          .getPersistentQueryMetadata();
      final Properties streamsProperties = new Properties();
      streamsProperties.putAll(persistentQueryMetadata.getStreamsProperties());
      final Topology topology = persistentQueryMetadata.getTopology();
      final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(
          topology,
          streamsProperties,
          Instant.EPOCH);
      final List<Topic> sourceTopics = persistentQueryAndSources.getSources()
          .stream()
          .map(dataSource -> {
            stubKafkaService.requireTopicExists(dataSource.getKafkaTopicName());
            return stubKafkaService.getTopic(dataSource.getKafkaTopicName());
          })
          .collect(Collectors.toList());

      final Optional<Topic> sinkTopic = persistentQueryMetadata.getSinkName()
          .map(name -> buildSinkTopic(
              ksqlEngine.getMetaStore().getSource(name),
              stubKafkaService,
              serviceContext.getSchemaRegistryClient()));

      testCase.setGeneratedTopologies(
          ImmutableList.of(persistentQueryMetadata.getTopologyDescription()));
      testCase.setGeneratedSchemas(persistentQueryMetadata.getQuerySchemas().getLoggerSchemaInfo());
      topologyTestDrivers.add(TopologyTestDriverContainer.of(
          topologyTestDriver,
          sourceTopics,
          sinkTopic
      ));
    }
    return topologyTestDrivers;
  }

  @VisibleForTesting
  static Iterator<PlannedStatement> planTestCase(
      final KsqlEngine engine,
      final TestCase testCase,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final Optional<SchemaRegistryClient> srClient,
      final StubKafkaService stubKafkaService
  ) {
    initializeTopics(
        testCase,
        engine.getServiceContext(),
        stubKafkaService,
        engine.getMetaStore(),
        ksqlConfig
    );

    if (testCase.getExpectedTopology().isPresent()
        && testCase.getExpectedTopology().get().getPlan().isPresent()
    ) {
      return testCase.getExpectedTopology().get().getPlan().get()
          .stream()
          .map(p -> ConfiguredKsqlPlan.of(p, SessionConfig.of(ksqlConfig, testCase.properties())))
          .map(PlannedStatement::new)
          .iterator();
    }
    return PlannedStatementIterator.of(engine, testCase, ksqlConfig, serviceContext, srClient);
  }

  private static Topic buildSinkTopic(
      final DataSource sinkDataSource,
      final StubKafkaService stubKafkaService,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final String kafkaTopicName = sinkDataSource.getKafkaTopicName();

    final KsqlTopic ksqlTopic = sinkDataSource.getKsqlTopic();
    final Optional<ParsedSchema> keySchema = getSchema(
        ksqlTopic.getKeyFormat().getFormat(),
        getSRSubject(ksqlTopic.getKafkaTopicName(), true),
        schemaRegistryClient
    );
    final Optional<ParsedSchema> valueSchema = getSchema(
        ksqlTopic.getValueFormat().getFormat(),
        getSRSubject(ksqlTopic.getKafkaTopicName(), false),
        schemaRegistryClient
    );

    final Topic sinkTopic = new Topic(kafkaTopicName, keySchema, valueSchema);

    stubKafkaService.ensureTopic(sinkTopic);
    return sinkTopic;
  }

  private static Optional<ParsedSchema> getSchema(
      final String format,
      final String subject,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final Format valueFormat = FormatFactory
        .fromName(format);

    if (!valueFormat.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)) {
      return Optional.empty();
    }

    try {
      final SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
      return Optional.of(
          schemaRegistryClient.getSchemaBySubjectAndId(subject, metadata.getId())
      );
    } catch (final Exception e) {
      // do nothing
    }
    return Optional.empty();
  }

  private static List<PersistentQueryAndSources> doBuildQueries(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final StubKafkaService stubKafkaService,
      final TestExecutionListener listener
  ) {
    final List<PersistentQueryAndSources> queries = execute(
        ksqlEngine,
        testCase,
        ksqlConfig,
        serviceContext,
        Optional.of(serviceContext.getSchemaRegistryClient()),
        stubKafkaService,
        listener
    );

    if (testCase.getInputRecords().isEmpty()) {
      testCase.expectedException().map(ee -> {
        throw new AssertionError("Expected test to throw" + StringDescription.toString(ee));
      });
    }

    assertThat("test did not generate any queries.", queries, is(not(empty())));
    return queries;
  }

  private static void initializeTopics(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final StubKafkaService stubKafkaService,
      final FunctionRegistry functionRegistry,
      final KsqlConfig ksqlConfig
  ) {
    final KafkaTopicClient topicClient = serviceContext.getTopicClient();
    final SchemaRegistryClient srClient = serviceContext.getSchemaRegistryClient();

    final List<String> statements = testCase.getExpectedTopology().isPresent()
        ? ImmutableList.of() // Historic plans have already their topics already captured
        : testCase.statements(); // Non-historic plans need to capture topics from stmts

    final Collection<Topic> topics = TestCaseBuilderUtil.getAllTopics(
        statements,
        testCase.getTopics(),
        testCase.getOutputRecords(),
        testCase.getInputRecords(),
        functionRegistry,
        testCase.applyProperties(ksqlConfig)
    );

    for (final Topic topic : topics) {
      stubKafkaService.ensureTopic(topic);
      topicClient.createTopic(
          topic.getName(),
          topic.getNumPartitions(),
          topic.getReplicas());

      topic.getKeySchema().ifPresent(schema -> {
        try {
          srClient.register(KsqlConstants.getSRSubject(topic.getName(), true), schema);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      topic.getValueSchema().ifPresent(schema -> {
        try {
          srClient.register(KsqlConstants.getSRSubject(topic.getName(), false), schema);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  /**
   * @param srClient if supplied, then schemas can be inferred from the schema registry.
   * @return a list of persistent queries that should be run by the test executor, if a query was
   *         replaced via a CREATE OR REPLACE statement it will only appear once in the output list
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private static List<PersistentQueryAndSources> execute(
      final KsqlEngine engine,
      final TestCase testCase,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final Optional<SchemaRegistryClient> srClient,
      final StubKafkaService stubKafkaService,
      final TestExecutionListener listener
  ) {
    final Map<QueryId, PersistentQueryAndSources> queries = new LinkedHashMap<>();

    int idx = 0;
    final Iterator<PlannedStatement> plans =
        planTestCase(engine, testCase, ksqlConfig, serviceContext, srClient, stubKafkaService);

    try {
      while (plans.hasNext()) {
        ++idx;
        final PlannedStatement planned = plans.next();
        if (planned.insertValues.isPresent()) {
          final ConfiguredStatement<InsertValues> insertValues = planned.insertValues.get();

          final SessionProperties sessionProperties = new SessionProperties(
              insertValues.getSessionConfig().getOverrides(),
              new KsqlHostInfo("host", 50),
              buildUrl(),
              false);

          StubInsertValuesExecutor.of(stubKafkaService).execute(
              insertValues,
              sessionProperties,
              engine,
              engine.getServiceContext()
          );
          continue;
        }

        final ConfiguredKsqlPlan plan = planned.plan.orElseThrow(IllegalStateException::new);

        listener.acceptPlan(plan);

        final ExecuteResultAndSources result = executePlan(engine, plan);
        if (!result.getSources().isPresent()) {
          continue;
        }

        final PersistentQueryMetadata query = (PersistentQueryMetadata) result
            .getExecuteResult().getQuery().get();

        listener.acceptQuery(query);

        queries.put(
            query.getQueryId(),
            new PersistentQueryAndSources(query, result.getSources().get()));
      }
      return ImmutableList.copyOf(queries.values());
    } catch (final KsqlStatementException e) {
      if (testCase.expectedException().isPresent() && plans.hasNext()) {
        throw new AssertionError("Only the last statement in a negative test should fail. "
            + "Yet in this case statement " + idx + " failed.", e);
      }
      throw e;
    }
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

  static final class PlannedStatement {

    final Optional<ConfiguredKsqlPlan> plan;
    final Optional<ConfiguredStatement<InsertValues>> insertValues;

    PlannedStatement(final ConfiguredKsqlPlan plan) {
      this.plan = Optional.of(plan);
      this.insertValues = Optional.empty();
    }

    PlannedStatement(final ConfiguredStatement<InsertValues> insertValues) {
      this.plan = Optional.empty();
      this.insertValues = Optional.of(insertValues);
    }
  }

  private static final class PlannedStatementIterator implements Iterator<PlannedStatement> {

    private final Iterator<ParsedStatement> statements;
    private final KsqlExecutionContext executionContext;
    private final Map<String, Object> overrides;
    private final KsqlConfig ksqlConfig;
    private final Optional<InjectorChain> schemaInjector;

    private PlannedStatementIterator(
        final Iterator<ParsedStatement> statements,
        final KsqlExecutionContext executionContext,
        final Map<String, Object> overrides,
        final KsqlConfig ksqlConfig,
        final Optional<InjectorChain> schemaInjector
    ) {
      this.statements = requireNonNull(statements, "statements");
      this.executionContext = requireNonNull(executionContext, "executionContext");
      this.overrides = requireNonNull(overrides, "overrides");
      this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
      this.schemaInjector = requireNonNull(schemaInjector, "schemaInjector");
    }

    public static PlannedStatementIterator of(
        final KsqlExecutionContext executionContext,
        final TestCase testCase,
        final KsqlConfig ksqlConfig,
        final ServiceContext serviceContext,
        final Optional<SchemaRegistryClient> srClient
    ) {
      final Optional<InjectorChain> schemaInjector = srClient
          .map(SchemaRegistryTopicSchemaSupplier::new)
          .map(supplier -> InjectorChain.of(
              new DefaultSchemaInjector(supplier, executionContext, serviceContext),
              new SchemaRegisterInjector(executionContext, serviceContext)));
      final String sql = testCase.statements().stream()
          .collect(Collectors.joining(System.lineSeparator()));
      final Iterator<ParsedStatement> statements = executionContext.parse(sql).iterator();
      return new PlannedStatementIterator(
          statements,
          executionContext,
          testCase.properties(),
          ksqlConfig,
          schemaInjector
      );
    }

    @Override
    public boolean hasNext() {
      return statements.hasNext();
    }

    @Override
    public PlannedStatement next() {
      return planStatement(statements.next());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private PlannedStatement planStatement(final ParsedStatement stmt) {
      final PreparedStatement<?> prepared = executionContext.prepare(stmt);
      final ConfiguredStatement<?> configured = ConfiguredStatement.of(
          prepared,
          SessionConfig.of(ksqlConfig, overrides)
      );

      if (prepared.getStatement() instanceof InsertValues) {
        return new PlannedStatement((ConfiguredStatement<InsertValues>) configured);
      }

      final ConfiguredStatement<?> withFormats =
          new DefaultFormatInjector().inject(configured);
      final ConfiguredStatement<?> withSourceProps =
          new SourcePropertyInjector().inject(withFormats);
      final ConfiguredStatement<?> withSchema =
          schemaInjector
              .map(injector -> injector.inject(withSourceProps))
              .orElse((ConfiguredStatement) withSourceProps);

      final KsqlPlan plan = executionContext
          .plan(executionContext.getServiceContext(), withSchema);

      return new PlannedStatement(
          ConfiguredKsqlPlan.of(rewritePlan(plan), withSchema.getSessionConfig())
      );
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

  private static URL buildUrl() {
    try {
      return new URL("https://someHost:9876");
    } catch (final MalformedURLException e) {
      throw new AssertionError("Failed to test URL");
    }
  }
}
