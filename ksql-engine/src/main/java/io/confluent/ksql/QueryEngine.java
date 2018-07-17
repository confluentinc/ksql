/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql;

import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.StatementWithSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.DdlCommand;
import io.confluent.ksql.ddl.commands.DdlCommandFactory;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.physical.KafkaStreamsBuilderImpl;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.StringUtil;

class QueryEngine {

  private static final Logger log = LoggerFactory.getLogger(QueryEngine.class);
  private final KsqlEngine ksqlEngine;
  private final DdlCommandFactory ddlCommandFactory;


  QueryEngine(final KsqlEngine ksqlEngine, final DdlCommandFactory ddlCommandFactory) {
    this.ddlCommandFactory = ddlCommandFactory;
    this.ksqlEngine = ksqlEngine;
  }


  List<Pair<String, PlanNode>> buildLogicalPlans(
      final MetaStore metaStore,
      final List<Pair<String, Statement>> statementList,
      final KsqlConfig config) {

    List<Pair<String, PlanNode>> logicalPlansList = new ArrayList<>();
    // TODO: the purpose of tempMetaStore here
    MetaStore tempMetaStore = metaStore.clone();

    for (Pair<String, Statement> statementQueryPair : statementList) {
      if (statementQueryPair.getRight() instanceof Query) {
        PlanNode logicalPlan = buildQueryLogicalPlan(
            statementQueryPair.getLeft(),
            (Query) statementQueryPair.getRight(),
            tempMetaStore, config
        );
        logicalPlansList.add(new Pair<>(statementQueryPair.getLeft(), logicalPlan));
      } else {
        logicalPlansList.add(new Pair<>(statementQueryPair.getLeft(), null));
      }

      log.info("Build logical plan for {}.", statementQueryPair.getLeft());
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
        ksqlEngine.getFunctionRegistry(),
        config
    );
    final Analysis analysis = queryAnalyzer.analyze(sqlExpression, query);
    final AggregateAnalysis aggAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);
    final PlanNode logicalPlan
        = new LogicalPlanner(analysis, aggAnalysis, ksqlEngine.getFunctionRegistry()).buildPlan();
    if (logicalPlan instanceof KsqlStructuredDataOutputNode) {
      KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode =
          (KsqlStructuredDataOutputNode) logicalPlan;

      StructuredDataSource
          structuredDataSource =
          new KsqlStream(
              sqlExpression,
              ksqlStructuredDataOutputNode.getId().toString(),
              ksqlStructuredDataOutputNode.getSchema(),
              ksqlStructuredDataOutputNode.getKeyField(),
              ksqlStructuredDataOutputNode.getTimestampExtractionPolicy(),
              ksqlStructuredDataOutputNode.getKsqlTopic()
          );
      if (analysis.isDoCreateInto()) {
        tempMetaStore.putTopic(ksqlStructuredDataOutputNode.getKsqlTopic());
        tempMetaStore.putSource(structuredDataSource.cloneWithTimeKeyColumns());
      }
    }
    return logicalPlan;
  }

  List<QueryMetadata> buildPhysicalPlans(
      final List<Pair<String, PlanNode>> logicalPlans,
      final List<Pair<String, Statement>> statementList,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final KafkaClientSupplier clientSupplier,
      final boolean updateMetastore
  ) {

    List<QueryMetadata> physicalPlans = new ArrayList<>();

    for (int i = 0; i < logicalPlans.size(); i++) {

      Pair<String, PlanNode> statementPlanPair = logicalPlans.get(i);
      if (statementPlanPair.getRight() == null) {
        Statement statement = statementList.get(i).getRight();
        if (!(statement instanceof DdlStatement)) {
          throw new KsqlException("expecting a statement implementing DDLStatement but got: "
                                  + statement.getClass());
        }
        handleDdlStatement(
            statementPlanPair.getLeft(),
            (DdlStatement) statement
        );
      } else {
        buildQueryPhysicalPlan(
            physicalPlans, statementPlanPair, ksqlConfig,
            overriddenProperties, clientSupplier, updateMetastore
        );
      }

    }
    return physicalPlans;
  }

  private void buildQueryPhysicalPlan(
      final List<QueryMetadata> physicalPlans,
      final Pair<String, PlanNode> statementPlanPair,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final KafkaClientSupplier clientSupplier,
      final boolean updateMetastore
  ) {

    final StreamsBuilder builder = new StreamsBuilder();

    // Build a physical plan, in this case a Kafka Streams DSL
    final PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        builder,
        ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties),
        ksqlEngine.getTopicClient(),
        ksqlEngine.getFunctionRegistry(),
        overriddenProperties,
        updateMetastore,
        ksqlEngine.getMetaStore(),
        ksqlEngine.getSchemaRegistryClient(),
        ksqlEngine.getQueryIdGenerator(),
        new KafkaStreamsBuilderImpl(clientSupplier)
    );
    physicalPlans.add(physicalPlanBuilder.buildPhysicalPlan(statementPlanPair));
  }


  DdlCommandResult handleDdlStatement(String sqlExpression, DdlStatement statement) {

    if (statement instanceof AbstractStreamCreateStatement) {
      AbstractStreamCreateStatement streamCreateStatement = (AbstractStreamCreateStatement)
          statement;
      final StatementWithSchema statementWithSchema
          = maybeAddFieldsFromSchemaRegistry(streamCreateStatement, sqlExpression);

      statement = (DdlStatement) statementWithSchema.getStatement();
      sqlExpression = statementWithSchema.getStatementText();
    }
    final DdlCommand command = ddlCommandFactory.create(sqlExpression, statement);
    return ksqlEngine.getDdlCommandExec().execute(command, false);
  }

  StructuredDataSource getResultDatasource(final Select select, final String name) {

    SchemaBuilder dataSource = SchemaBuilder.struct().name(name);
    for (SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        SingleColumn singleColumn = (SingleColumn) selectItem;
        String fieldName = singleColumn.getAlias().get();
        dataSource = dataSource.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
      }
    }

    KsqlTopic ksqlTopic = new KsqlTopic(name, name, null);
    return new KsqlStream(
        "QueryEngine-DDLCommand-Not-Needed",
        name,
        dataSource.schema(),
        null,
        null,
        ksqlTopic
    );
  }

  private StatementWithSchema maybeAddFieldsFromSchemaRegistry(
      final AbstractStreamCreateStatement streamCreateStatement,
      final String statementText
  ) {
    if (streamCreateStatement.getProperties().containsKey(DdlConfig.TOPIC_NAME_PROPERTY)) {
      final String ksqlRegisteredTopicName = StringUtil.cleanQuotes(
          streamCreateStatement
              .getProperties()
              .get(DdlConfig.TOPIC_NAME_PROPERTY)
              .toString()
              .toUpperCase()
      );
      final KsqlTopic ksqlTopic = ksqlEngine.getMetaStore().getTopic(ksqlRegisteredTopicName);
      if (ksqlTopic == null) {
        throw new KsqlException(String.format(
            "Could not find %s topic in the metastore.",
            ksqlRegisteredTopicName
        ));
      }
      final Map<String, Expression> newProperties = new HashMap<>();
      newProperties.put(
          DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(ksqlTopic.getKafkaTopicName())
      );
      newProperties.put(
          DdlConfig.VALUE_FORMAT_PROPERTY,
          new StringLiteral(
              ksqlTopic.getKsqlTopicSerDe().getSerDe().toString()
          )
      );
      final AbstractStreamCreateStatement statementWithProperties = streamCreateStatement.copyWith(
          streamCreateStatement.getElements(),
          newProperties);
      return StatementWithSchema.forStatement(
          statementWithProperties,
          SqlFormatter.formatSql(statementWithProperties),
          new HashMap<>(),
          ksqlEngine.getSchemaRegistryClient()
      );
    }
    return StatementWithSchema.forStatement(
        streamCreateStatement,
        statementText,
        new HashMap<>(),
        ksqlEngine.getSchemaRegistryClient());
  }
}
