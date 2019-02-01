/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.physical.KafkaStreamsBuilderImpl;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class QueryEngine {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(QueryEngine.class);

  private final ServiceContext serviceContext;
  private final Consumer<QueryMetadata> queryCloseCallback;
  private final QueryIdGenerator queryIdGenerator;

  QueryEngine(
      final ServiceContext serviceContext,
      final Consumer<QueryMetadata> queryCloseCallback
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.queryCloseCallback = Objects.requireNonNull(queryCloseCallback, "queryCloseCallback");
    this.queryIdGenerator = new QueryIdGenerator();
  }

  @SuppressWarnings("MethodMayBeStatic") // To allow action to be mocked.
  LogicalPlanNode buildLogicalPlan(
      final MetaStore metaStore,
      final PreparedStatement<?> statement,
      final KsqlConfig config
  ) {
    LOG.info("Build logical plan for {}.", statement.getStatementText());

    if (!(statement.getStatement() instanceof Query)) {
      return new LogicalPlanNode(statement.getStatementText(), null);
    }

    final PlanNode planNode = buildQueryLogicalPlan(
        statement.getStatementText(),
        (Query) statement.getStatement(),
        metaStore,
        config
    );

    return new LogicalPlanNode(statement.getStatementText(), planNode);
  }

  QueryMetadata buildPhysicalPlan(
      final LogicalPlanNode logicalPlanNode,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final KafkaClientSupplier clientSupplier,
      final MetaStore metaStore
  ) {

    final StreamsBuilder builder = new StreamsBuilder();

    // Build a physical plan, in this case a Kafka Streams DSL
    final PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        builder,
        ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties),
        serviceContext,
        metaStore,
        overriddenProperties,
        metaStore,
        queryIdGenerator,
        new KafkaStreamsBuilderImpl(clientSupplier),
        queryCloseCallback
    );

    return physicalPlanBuilder.buildPhysicalPlan(logicalPlanNode);
  }

  @SuppressWarnings("MethodMayBeStatic") // To allow action to be mocked.
  StructuredDataSource getResultDatasource(final Select select, final String name) {

    SchemaBuilder dataSource = SchemaBuilder.struct().name(name);
    for (final SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        final SingleColumn singleColumn = (SingleColumn) selectItem;
        final String fieldName = singleColumn.getAlias().get();
        dataSource = dataSource.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
      }
    }

    final KsqlTopic ksqlTopic = new KsqlTopic(name, name, null, true);
    return new KsqlStream<>(
        "QueryEngine-DDLCommand-Not-Needed",
        name,
        dataSource.schema(),
        null,
        null,
        ksqlTopic,
        Serdes.String()
    );
  }

  private static PlanNode buildQueryLogicalPlan(
      final String sqlExpression,
      final Query query,
      final MetaStore metaStore,
      final KsqlConfig config
  ) {
    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(metaStore, config);

    final Analysis analysis = queryAnalyzer.analyze(sqlExpression, query);
    final AggregateAnalysis aggAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    return new LogicalPlanner(analysis, aggAnalysis, metaStore).buildPlan();
  }
}
