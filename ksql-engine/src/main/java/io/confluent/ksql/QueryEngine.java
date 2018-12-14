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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
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
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
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

  private static final Logger log = LoggerFactory.getLogger(QueryEngine.class);
  private final KafkaTopicClient topicClient;
  private final Supplier<SchemaRegistryClient> schemaRegistryClientFactory;
  private final QueryIdGenerator queryIdGenerator;
  private final QueryIdGenerator tryQueryIdGenerator;

  QueryEngine(
      final KafkaTopicClient topicClient,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    this.queryIdGenerator = new QueryIdGenerator("");
    this.tryQueryIdGenerator = new QueryIdGenerator("_TRY");
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient");
    this.schemaRegistryClientFactory =
        Objects.requireNonNull(schemaRegistryClientFactory, "schemaRegistryClientFactory");
  }

  List<LogicalPlanNode> buildLogicalPlans(
      final MetaStore metaStore,
      final List<? extends PreparedStatement<?>> statements,
      final KsqlConfig config
  ) {
    final List<LogicalPlanNode> logicalPlansList = new ArrayList<>();

    for (final PreparedStatement<?> statement : statements) {
      if (statement.getStatement() instanceof Query) {
        final PlanNode logicalPlan = buildQueryLogicalPlan(
            statement.getStatementText(),
            (Query) statement.getStatement(),
            metaStore, config
        );
        logicalPlansList.add(new LogicalPlanNode(statement.getStatementText(), logicalPlan));
      } else {
        logicalPlansList.add(new LogicalPlanNode(statement.getStatementText(), null));
      }

      log.info("Build logical plan for {}.", statement.getStatementText());
    }
    return logicalPlansList;
  }

  private PlanNode buildQueryLogicalPlan(
      final String sqlExpression,
      final Query query,
      final MetaStore tempMetaStore,
      final KsqlConfig config) {
    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(
        tempMetaStore,
        tempMetaStore,
        config
    );
    final Analysis analysis = queryAnalyzer.analyze(sqlExpression, query);
    final AggregateAnalysis aggAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);
    final PlanNode logicalPlan
        = new LogicalPlanner(analysis, aggAnalysis, tempMetaStore).buildPlan();
    if (logicalPlan instanceof KsqlStructuredDataOutputNode) {
      final KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode =
          (KsqlStructuredDataOutputNode) logicalPlan;

      final StructuredDataSource
          structuredDataSource =
          (ksqlStructuredDataOutputNode.getNodeOutputType() == DataSourceType.KTABLE)
              ? new KsqlTable<>(
                  sqlExpression,
                  ksqlStructuredDataOutputNode.getId().toString(),
                  ksqlStructuredDataOutputNode.getSchema(),
                  ksqlStructuredDataOutputNode.getKeyField(),
                  ksqlStructuredDataOutputNode.getTimestampExtractionPolicy(),
                  ksqlStructuredDataOutputNode.getKsqlTopic(),
                  "", // Placeholder
                  Serdes.String()  // Placeholder
              )
              : new KsqlStream<>(
                  sqlExpression,
                  ksqlStructuredDataOutputNode.getId().toString(),
                  ksqlStructuredDataOutputNode.getSchema(),
                  ksqlStructuredDataOutputNode.getKeyField(),
                  ksqlStructuredDataOutputNode.getTimestampExtractionPolicy(),
                  ksqlStructuredDataOutputNode.getKsqlTopic(),
                  Serdes.String()  // Placeholder
              );

      if (analysis.isDoCreateInto()) {
        try {
          tempMetaStore.putTopic(ksqlStructuredDataOutputNode.getKsqlTopic());
        } catch (final KsqlException e) {
          final String sourceName = tempMetaStore.getSourceForTopic(
              ksqlStructuredDataOutputNode.getKsqlTopic().getName()).get().getName();
          throw new KsqlException(
              String.format("Cannot create the stream/table. "
                  + "The output topic %s is already used by %s",
                  ksqlStructuredDataOutputNode.getKsqlTopic().getKafkaTopicName(), sourceName), e);
        }
        tempMetaStore.putSource(structuredDataSource.cloneWithTimeKeyColumns());
      }
    }
    return logicalPlan;
  }

  QueryMetadata buildPhysicalPlan(
      final LogicalPlanNode logicalPlanNode,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final KafkaClientSupplier clientSupplier,
      final MetaStore metaStore,
      final boolean updateMetastore
  ) {

    final StreamsBuilder builder = new StreamsBuilder();

    // Build a physical plan, in this case a Kafka Streams DSL
    final PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        builder,
        ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties),
        topicClient,
        metaStore,
        overriddenProperties,
        updateMetastore,
        metaStore,
        schemaRegistryClientFactory,
        updateMetastore ? queryIdGenerator : tryQueryIdGenerator,
        new KafkaStreamsBuilderImpl(clientSupplier)
    );

    return physicalPlanBuilder.buildPhysicalPlan(logicalPlanNode);
  }

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
}
