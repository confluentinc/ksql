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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.schema.inference.DefaultSchemaInjector;
import io.confluent.ksql.schema.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;

final class TestExecutorUtil {

  private TestExecutorUtil() {}

  static List<TopologyTestDriverContainer> buildStreamsTopologyTestDrivers(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig) {
    final Map<String, String> persistedConfigs = testCase.persistedProperties();
    final KsqlConfig maybeUpdatedConfigs = persistedConfigs.isEmpty() ? ksqlConfig :
        ksqlConfig.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

    final List<PersistentQueryAndSortedSources> queryMetadataList = buildQueries(
        testCase, serviceContext, ksqlEngine, maybeUpdatedConfigs);
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
      topologyTestDrivers.add(TopologyTestDriverContainer.of(
          topologyTestDriver,
          persistentQueryAndSortedSources.getSources()
              .stream()
              .map(DataSource::getKsqlTopic)
              .collect(Collectors.toList()),
          ksqlEngine.getMetaStore().getSource(persistentQueryMetadata.getSinkNames()
              .iterator().next()).getKsqlTopic()
      ));
    }
    return topologyTestDrivers;
  }

  private static List<PersistentQueryAndSortedSources> buildQueries(
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

    final List<PersistentQueryAndSortedSources> queries = execute(
        ksqlEngine,
        sql,
        ksqlConfig,
        testCase.properties(),
        Optional.of(serviceContext.getSchemaRegistryClient())
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
      final Optional<SchemaRegistryClient> srClient
  ) {
    final List<ParsedStatement> statements = engine.parse(sql);

    final Optional<DefaultSchemaInjector> schemaInjector = srClient
        .map(SchemaRegistryTopicSchemaSupplier::new)
        .map(DefaultSchemaInjector::new);

    return statements.stream()
        .map(stmt -> execute(engine, stmt, ksqlConfig, overriddenProperties, schemaInjector))
        .filter(queryAndSortedSources -> queryAndSortedSources.getSources() != null)
        .map(
            queryAndSortedSources -> new PersistentQueryAndSortedSources(
                (PersistentQueryMetadata) queryAndSortedSources.getExecuteResult().getQuery().get(),
                queryAndSortedSources.getSources()
            ))
        .collect(Collectors.toList());
  }


  @SuppressWarnings({"rawtypes","unchecked"})
  private static QueryAndSortedSources execute(
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
    final ExecuteResult executeResult = executionContext.execute(withSchema);
    if (prepared.getStatement() instanceof CreateAsSelect) {
      return new QueryAndSortedSources(
          executeResult,
          getSortedSources(
              (CreateAsSelect)prepared.getStatement(),
              executionContext.getMetaStore()));
    }
    return new QueryAndSortedSources(executeResult, null);
  }

  private static List<DataSource> getSortedSources(
      final CreateAsSelect createAsSelect,
      final MetaStore metaStore) {
    final Relation from = createAsSelect.getQuery().getFrom();
    if (from instanceof Join) {
      final Join join = (Join) from;
      final AliasedRelation left = (AliasedRelation) join.getLeft();
      final AliasedRelation right = (AliasedRelation) join.getRight();
      if (metaStore.getSource(left.getRelation().toString()) == null) {
        throw new KsqlException("Sourde does not exist: " + left.getRelation().toString());
      }
      if (metaStore.getSource(right.getRelation().toString()) == null) {
        throw new KsqlException("Sourde does not exist: " + right.getRelation().toString());
      }
      return ImmutableList.of(
          metaStore.getSource(left.getRelation().toString()),
          metaStore.getSource(right.getRelation().toString()));
    } else {
      final String fromName = ((AliasedRelation) from).getRelation().toString();
      if (metaStore.getSource(fromName) == null) {
        throw new KsqlException("Sourde does not exist: " + fromName);
      }
      return ImmutableList.of(metaStore.getSource(fromName));
    }
  }


  static final class QueryAndSortedSources {
    private final ExecuteResult executeResult;
    private final List<DataSource> sources;

    QueryAndSortedSources(
        final ExecuteResult executeResult,
        final List<DataSource> sources) {
      this.executeResult = executeResult;
      this.sources = sources;
    }

    ExecuteResult getExecuteResult() {
      return executeResult;
    }

    List<DataSource> getSources() {
      return sources;
    }
  }

  static final class PersistentQueryAndSortedSources {
    private final PersistentQueryMetadata persistentQueryMetadata;
    private final List<DataSource> sources;

    PersistentQueryAndSortedSources(
        final PersistentQueryMetadata persistentQueryMetadata,
        final List<DataSource> sources
    ) {
      this.persistentQueryMetadata = persistentQueryMetadata;
      this.sources = sources;
    }

    PersistentQueryMetadata getPersistentQueryMetadata() {
      return persistentQueryMetadata;
    }

    List<DataSource> getSources() {
      return sources;
    }
  }
}
