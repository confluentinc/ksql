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

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.FakeInsertValuesExecutor;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjector;
import io.confluent.ksql.schema.ksql.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
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

final class TestExecutorUtil {

  private TestExecutorUtil() {
  }

  static List<TopologyTestDriverContainer> buildStreamsTopologyTestDrivers(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final FakeKafkaService fakeKafkaService) {
    final Map<String, String> persistedConfigs = testCase.persistedProperties();
    final KsqlConfig maybeUpdatedConfigs = persistedConfigs.isEmpty() ? ksqlConfig :
        ksqlConfig.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

    final List<PersistentQueryAndSortedSources> queryMetadataList = buildQueries(
        testCase,
        serviceContext,
        ksqlEngine,
        maybeUpdatedConfigs,
        fakeKafkaService);
    final List<TopologyTestDriverContainer> topologyTestDrivers = new ArrayList<>();
    for (final PersistentQueryAndSortedSources persistentQueryAndSortedSources: queryMetadataList) {
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
            fakeKafkaService.requireTopicExists(dataSource.getKafkaTopicName());
            return fakeKafkaService.getTopic(dataSource.getKafkaTopicName());
          })
          .collect(Collectors.toList());

      final Topic sinkTopic = buildSinkTopic(
          ksqlEngine.getMetaStore().getSource(persistentQueryMetadata.getSinkName()),
          persistentQueryAndSortedSources.getWindowSize(),
          fakeKafkaService,
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
      final Optional<Long> windowSize,
      final FakeKafkaService fakeKafkaService,
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
        KsqlConstants.legacyDefaultSinkReplicaCount,
        windowSize
    );

    if (fakeKafkaService.topicExists(sinkTopic)) {
      fakeKafkaService.updateTopic(sinkTopic);
    } else {
      fakeKafkaService.createTopic(sinkTopic);
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


  private static List<PersistentQueryAndSortedSources> buildQueries(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final FakeKafkaService fakeKafkaService
  ) {
    testCase.initializeTopics(
        serviceContext.getTopicClient(),
        fakeKafkaService,
        serviceContext.getSchemaRegistryClient());

    final String sql = testCase.statements().stream()
        .collect(Collectors.joining(System.lineSeparator()));

    final List<PersistentQueryAndSortedSources> queries = execute(
        ksqlEngine,
        sql,
        ksqlConfig,
        testCase.properties(),
        Optional.of(serviceContext.getSchemaRegistryClient()),
        fakeKafkaService
    );

    assertThat("test did not generate any queries.", !queries.isEmpty());
    return queries;
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
      final FakeKafkaService fakeKafkaService
  ) {
    final List<ParsedStatement> statements = engine.parse(sql);

    final Optional<DefaultSchemaInjector> schemaInjector = srClient
        .map(SchemaRegistryTopicSchemaSupplier::new)
        .map(DefaultSchemaInjector::new);

    return statements.stream()
        .map(stmt -> execute(
                engine, stmt, ksqlConfig, overriddenProperties, schemaInjector, fakeKafkaService))
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


  @SuppressWarnings({"rawtypes","unchecked"})
  private static ExecuteResultAndSortedSources execute(
      final KsqlExecutionContext executionContext,
      final ParsedStatement stmt,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<DefaultSchemaInjector> schemaInjector,
      final FakeKafkaService fakeKafkaService
  ) {
    final PreparedStatement<?> prepared = executionContext.prepare(stmt);
    final ConfiguredStatement<?> configured = ConfiguredStatement.of(
            prepared, overriddenProperties, ksqlConfig);

    if (prepared.getStatement() instanceof InsertValues) {
      FakeInsertValuesExecutor.of(fakeKafkaService).execute(
              (ConfiguredStatement<InsertValues>) configured,
              executionContext,
              executionContext.getServiceContext()
      );
      return new ExecuteResultAndSortedSources(null, null, null);
    }

    final ConfiguredStatement<?> withSchema =
        schemaInjector
            .map(injector -> injector.inject(configured))
            .orElse((ConfiguredStatement) configured);
    final ExecuteResult executeResult = executionContext.execute(withSchema);
    if (prepared.getStatement() instanceof CreateAsSelect) {
      return new ExecuteResultAndSortedSources(
          executeResult,
          getSortedSources(
              ((CreateAsSelect)prepared.getStatement()).getQuery(),
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
      if (metaStore.getSource(left.getRelation().toString()) == null) {
        throw new KsqlException("Source does not exist: " + left.getRelation().toString());
      }
      if (metaStore.getSource(right.getRelation().toString()) == null) {
        throw new KsqlException("Source does not exist: " + right.getRelation().toString());
      }
      return ImmutableList.of(
          metaStore.getSource(left.getRelation().toString()),
          metaStore.getSource(right.getRelation().toString()));
    } else {
      final String fromName = ((AliasedRelation) from).getRelation().toString();
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
