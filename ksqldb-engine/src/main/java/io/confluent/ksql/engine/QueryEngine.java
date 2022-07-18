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

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.ExecutionPlan;
import io.confluent.ksql.execution.ExecutionPlanBuilder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.StreamsBuilder;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class QueryEngine {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;

  QueryEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext"
    );
  }

  static OutputNode buildQueryLogicalPlan(
      final Query query,
      final Optional<Sink> sink,
      final MetaStore metaStore,
      final KsqlConfig config,
      final boolean rowpartitionRowoffsetEnabled,
      final String statementTextMasked
  ) {
    final String outputPrefix = config.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG);
    final Boolean pullLimitClauseEnabled = config.getBoolean(
            KsqlConfig.KSQL_QUERY_PULL_LIMIT_CLAUSE_ENABLED);

    final QueryAnalyzer queryAnalyzer =
        new QueryAnalyzer(metaStore,
            outputPrefix,
            rowpartitionRowoffsetEnabled,
            pullLimitClauseEnabled
        );

    final Analysis analysis;
    try {
      analysis = queryAnalyzer.analyze(query, sink);
    } catch (final KsqlException e) {
      throw new KsqlStatementException(e.getMessage(), statementTextMasked, e);
    }

    return new LogicalPlanner(config, analysis, metaStore).buildPersistentLogicalPlan();
  }

  ExecutionPlan buildPhysicalPlan(
      final LogicalPlanNode logicalPlanNode,
      final SessionConfig config,
      final MetaStore metaStore,
      final QueryId queryId,
      final Optional<PlanInfo> oldPlanInfo
  ) {

    final StreamsBuilder builder = new StreamsBuilder();

    // Build a physical plan, in this case a Kafka Streams DSL
    final ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(
        builder,
        config.getConfig(true),
        serviceContext,
        processingLogContext,
        metaStore
    );

    return executionPlanBuilder.buildPhysicalPlan(logicalPlanNode, queryId, oldPlanInfo);
  }
}
