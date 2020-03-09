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
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.physical.PhysicalPlan;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class QueryEngine {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(QueryEngine.class);

  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final QueryIdGenerator queryIdGenerator;

  QueryEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final QueryIdGenerator queryIdGenerator
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext"
    );
    this.queryIdGenerator = Objects.requireNonNull(queryIdGenerator, "queryIdGenerator");
  }

  static OutputNode buildQueryLogicalPlan(
      final Query query,
      final Optional<Sink> sink,
      final MetaStore metaStore,
      final KsqlConfig config
  ) {
    final String outputPrefix = config.getString(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG);

    final Set<SerdeOption> defaultSerdeOptions = SerdeOptions.buildDefaults(config);

    final QueryAnalyzer queryAnalyzer =
        new QueryAnalyzer(metaStore, outputPrefix, defaultSerdeOptions);

    final Analysis analysis = queryAnalyzer.analyze(query, sink);
    final AggregateAnalysisResult aggAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    return new LogicalPlanner(config, analysis, aggAnalysis, metaStore).buildPlan();
  }

  PhysicalPlan buildPhysicalPlan(
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
        queryIdGenerator
    );

    return physicalPlanBuilder.buildPhysicalPlan(logicalPlanNode);
  }
}
