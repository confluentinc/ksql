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
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.physical.PhysicalPlan;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
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
      final KsqlConfig config
  ) {
    final String outputPrefix = config.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG);

    final QueryAnalyzer queryAnalyzer =
        new QueryAnalyzer(metaStore, outputPrefix);

    final Analysis analysis = queryAnalyzer.analyze(query, sink);

    return new LogicalPlanner(config, analysis, metaStore).buildPlan();
  }

  PhysicalPlan buildPhysicalPlan(
      final LogicalPlanNode logicalPlanNode,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final MutableMetaStore metaStore,
      final QueryId queryId
  ) {

    final StreamsBuilder builder = new StreamsBuilder();

    // Build a physical plan, in this case a Kafka Streams DSL
    final PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        builder,
        ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties),
        serviceContext,
        processingLogContext,
        metaStore
    );

    return physicalPlanBuilder.buildPhysicalPlan(logicalPlanNode, queryId);
  }
}
