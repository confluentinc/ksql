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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.AvroUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.StringUtil;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;

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
      final List<Pair<String, Statement>> statementList
  ) {

    List<Pair<String, PlanNode>> logicalPlansList = new ArrayList<>();
    // TODO: the purpose of tempMetaStore here
    MetaStore tempMetaStore = metaStore.clone();

    for (Pair<String, Statement> statementQueryPair : statementList) {
      if (statementQueryPair.getRight() instanceof Query) {
        PlanNode logicalPlan = buildQueryLogicalPlan(
            statementQueryPair.getLeft(),
            (Query) statementQueryPair.getRight(),
            tempMetaStore
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
      String sqlExpression,
      final Query query,
      final MetaStore tempMetaStore
  ) {
    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(
        tempMetaStore,
        ksqlEngine.getFunctionRegistry()
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

      tempMetaStore.putTopic(ksqlStructuredDataOutputNode.getKsqlTopic());
      tempMetaStore.putSource(structuredDataSource.cloneWithTimeKeyColumns());
    }
    return logicalPlan;
  }

  private TimestampExtractionPolicy getTimestampExtractionPolicy(final OutputNode outputNode) {
    return outputNode.getTimestampExtractionPolicy() instanceof MetadataTimestampExtractionPolicy
        ? outputNode.getTheSourceNode().getTimestampExtractionPolicy()
        : outputNode.getTimestampExtractionPolicy();
  }

  List<QueryMetadata> buildPhysicalPlans(
      final List<Pair<String, PlanNode>> logicalPlans,
      final List<Pair<String, Statement>> statementList,
      final Map<String, Object> overriddenProperties,
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
            (DdlStatement) statement,
            overriddenProperties
        );
      } else {
        buildQueryPhysicalPlan(
            physicalPlans, statementPlanPair,
            overriddenProperties, updateMetastore
        );
      }

    }
    return physicalPlans;
  }

  private void buildQueryPhysicalPlan(
      final List<QueryMetadata> physicalPlans,
      final Pair<String, PlanNode> statementPlanPair,
      final Map<String, Object> overriddenProperties,
      final boolean updateMetastore
  ) {

    final StreamsBuilder builder = new StreamsBuilder();

    // Build a physical plan, in this case a Kafka Streams DSL
    final PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        builder,
        ksqlEngine.getKsqlConfig().cloneWithPropertyOverwrite(overriddenProperties),
        ksqlEngine.getTopicClient(),
        ksqlEngine.getFunctionRegistry(),
        overriddenProperties,
        updateMetastore,
        ksqlEngine.getMetaStore(),
        ksqlEngine.getSchemaRegistryClient()
    );
    physicalPlans.add(physicalPlanBuilder.buildPhysicalPlan(statementPlanPair));
  }


  DdlCommandResult handleDdlStatement(
      String sqlExpression, DdlStatement statement,
      final Map<String, Object> overriddenProperties
  ) {

    if (statement instanceof AbstractStreamCreateStatement) {
      AbstractStreamCreateStatement streamCreateStatement = (AbstractStreamCreateStatement)
          statement;
      Pair<DdlStatement, String> avroCheckResult =
          maybeAddFieldsFromSchemaRegistry(streamCreateStatement);

      if (avroCheckResult.getRight() != null) {
        statement = avroCheckResult.getLeft();
        sqlExpression = avroCheckResult.getRight();
      }
    }
    DdlCommand command = ddlCommandFactory.create(sqlExpression, statement, overriddenProperties);
    return ksqlEngine.getDdlCommandExec().execute(command, false);
  }

  StructuredDataSource getResultDatasource(final Select select, final String name) {

    SchemaBuilder dataSource = SchemaBuilder.struct().name(name);
    for (SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        SingleColumn singleColumn = (SingleColumn) selectItem;
        String fieldName = singleColumn.getAlias().get();
        dataSource = dataSource.field(fieldName, Schema.BOOLEAN_SCHEMA);
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

  private Pair<DdlStatement, String> maybeAddFieldsFromSchemaRegistry(
      AbstractStreamCreateStatement streamCreateStatement
  ) {
    if (streamCreateStatement.getProperties().containsKey(DdlConfig.TOPIC_NAME_PROPERTY)) {
      String ksqlRegisteredTopicName = StringUtil.cleanQuotes(
          streamCreateStatement
              .getProperties()
              .get(DdlConfig.TOPIC_NAME_PROPERTY)
              .toString()
              .toUpperCase()
      );
      KsqlTopic ksqlTopic = ksqlEngine.getMetaStore().getTopic(ksqlRegisteredTopicName);
      if (ksqlTopic == null) {
        throw new KsqlException(String.format(
            "Could not find %s topic in the metastore.",
            ksqlRegisteredTopicName
        ));
      }
      Map<String, Expression> newProperties = new HashMap<>();
      newProperties.put(
          DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(ksqlTopic.getKafkaTopicName())
      );
      newProperties.put(
          DdlConfig.VALUE_FORMAT_PROPERTY,
          new StringLiteral(
              ksqlTopic.getKsqlTopicSerDe().getSerDe().toString()
          )
      );
      streamCreateStatement = streamCreateStatement.copyWith(
          streamCreateStatement.getElements(),
          newProperties
      );
    }
    Pair<AbstractStreamCreateStatement, String> avroCheckResult =
        new AvroUtil().checkAndSetAvroSchema(
            streamCreateStatement,
            new HashMap<>(),
            ksqlEngine.getSchemaRegistryClient()
        );
    if (avroCheckResult.getRight() != null) {
      if (avroCheckResult.getLeft() instanceof CreateStream) {
        return new Pair<>((CreateStream) avroCheckResult.getLeft(), avroCheckResult.getRight());
      } else if (avroCheckResult.getLeft() instanceof CreateTable) {
        return new Pair<>((CreateTable) avroCheckResult.getLeft(), avroCheckResult.getRight());
      }
    }
    return new Pair<>(null, null);
  }

}
