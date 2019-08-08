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

package io.confluent.ksql.engine;

import io.confluent.ksql.analyzer.AggregateAnalysisResult;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.physical.KafkaStreamsBuilderImpl;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class QueryEngine {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(QueryEngine.class);

  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final Consumer<QueryMetadata> queryCloseCallback;
  private final QueryIdGenerator queryIdGenerator;

  QueryEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final QueryIdGenerator queryIdGenerator,
      final Consumer<QueryMetadata> queryCloseCallback
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext");
    this.queryCloseCallback = Objects.requireNonNull(queryCloseCallback, "queryCloseCallback");
    this.queryIdGenerator = Objects.requireNonNull(queryIdGenerator, "queryIdGenerator");
  }

  @SuppressWarnings("MethodMayBeStatic") // To allow action to be mocked.
  LogicalPlanNode buildLogicalPlan(
      final MetaStore metaStore,
      final ConfiguredStatement<?> statement
  ) {
    LOG.info("Build logical plan for {}.", statement.getStatementText());

    if (statement.getStatement() instanceof Query) {
      final OutputNode outputNode = buildQueryLogicalPlan(
          statement.getStatementText(),
          (Query)statement.getStatement(),
          Optional.empty(),
          metaStore,
          statement.getConfig()
      );

      return new LogicalPlanNode(statement.getStatementText(), Optional.of(outputNode));
    }

    if (statement.getStatement() instanceof QueryContainer) {
      final OutputNode outputNode = buildQueryLogicalPlan(
          statement.getStatementText(),
          (QueryContainer) statement.getStatement(),
          metaStore,
          statement.getConfig()
      );

      return new LogicalPlanNode(statement.getStatementText(), Optional.of(outputNode));
    }

    return new LogicalPlanNode(statement.getStatementText(), Optional.empty());
  }

  QueryMetadata buildPhysicalPlan(
      final LogicalPlanNode logicalPlanNode,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final MutableMetaStore metaStore
  ) {

    final StreamsBuilder builder = new StreamsBuilder();

    // Build a physical plan, in this case a Kafka Streams DSL
    final PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        builder,
        ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties),
        serviceContext,
        processingLogContext,
        metaStore,
        overriddenProperties,
        metaStore,
        queryIdGenerator,
        new KafkaStreamsBuilderImpl(serviceContext.getKafkaClientSupplier()),
        queryCloseCallback
    );

    return physicalPlanBuilder.buildPhysicalPlan(logicalPlanNode);
  }

  private static OutputNode buildQueryLogicalPlan(
      final String sqlExpression,
      final QueryContainer container,
      final MetaStore metaStore,
      final KsqlConfig config
  ) {
    final Query query = container.getQuery();
    final Sink sink = container.getSink();
    return buildQueryLogicalPlan(sqlExpression, query, Optional.of(sink), metaStore, config);
  }

  private static OutputNode buildQueryLogicalPlan(
      final String sqlExpression,
      final Query query,
      final Optional<Sink> sink,
      final MetaStore metaStore,
      final KsqlConfig config
  ) {
    final String outputPrefix = config.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG);

    final Set<SerdeOption> defaultSerdeOptions = SerdeOptions.buildDefaults(config);

    final QueryAnalyzer queryAnalyzer =
        new QueryAnalyzer(metaStore, outputPrefix, defaultSerdeOptions);

    final Analysis analysis = queryAnalyzer.analyze(sqlExpression, query, sink);
    final AggregateAnalysisResult aggAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    return new LogicalPlanner(config, analysis, aggAnalysis, metaStore).buildPlan();
  }
}
