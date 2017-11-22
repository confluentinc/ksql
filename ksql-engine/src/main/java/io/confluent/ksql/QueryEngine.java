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

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.ddl.commands.DDLCommand;
import io.confluent.ksql.ddl.commands.DDLCommandFactory;
import io.confluent.ksql.ddl.commands.DDLCommandResult;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.DDLStatement;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class QueryEngine {

  private static final Logger log = LoggerFactory.getLogger(QueryEngine.class);
  private final AtomicLong queryIdCounter;
  private final KsqlEngine ksqlEngine;
  private final DDLCommandFactory ddlCommandFactory;


  QueryEngine(final KsqlEngine ksqlEngine, final DDLCommandFactory ddlCommandFactory) {
    this.ddlCommandFactory = ddlCommandFactory;
    this.queryIdCounter = new AtomicLong(1);
    this.ksqlEngine = ksqlEngine;
  }


  List<Pair<String, PlanNode>> buildLogicalPlans(
      final MetaStore metaStore,
      final List<Pair<String, Statement>> statementList) {

    List<Pair<String, PlanNode>> logicalPlansList = new ArrayList<>();
    // TODO: the purpose of tempMetaStore here
    MetaStore tempMetaStore = metaStore.clone();

    for (Pair<String, Statement> statementQueryPair : statementList) {
      if (statementQueryPair.getRight() instanceof Query) {
        PlanNode logicalPlan = buildQueryLogicalPlan((Query) statementQueryPair.getRight(),
                                                     tempMetaStore);
        logicalPlansList.add(new Pair<>(statementQueryPair.getLeft(), logicalPlan));
      } else {
        logicalPlansList.add(new Pair<>(statementQueryPair.getLeft(), null));
      }

      log.info("Build logical plan for {}.", statementQueryPair.getLeft());
    }
    return logicalPlansList;
  }

  private PlanNode buildQueryLogicalPlan(final Query query, final MetaStore tempMetaStore) {
    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(tempMetaStore, ksqlEngine.getFunctionRegistry());
    final Analysis analysis = queryAnalyzer.analyize(query);
    final AggregateAnalysis aggAnalysis = queryAnalyzer.analyizeAggregate(query, analysis);
    final PlanNode logicalPlan
        = new LogicalPlanner(analysis, aggAnalysis, ksqlEngine.getFunctionRegistry()).buildPlan();
    if (logicalPlan instanceof KsqlStructuredDataOutputNode) {
      KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode =
          (KsqlStructuredDataOutputNode) logicalPlan;

      StructuredDataSource
          structuredDataSource =
          new KsqlStream(ksqlStructuredDataOutputNode.getId().toString(),
                         ksqlStructuredDataOutputNode.getSchema(),
                         ksqlStructuredDataOutputNode.getKeyField(),
                         ksqlStructuredDataOutputNode.getTimestampField() == null
                         ? ksqlStructuredDataOutputNode.getTheSourceNode().getTimestampField()
                         : ksqlStructuredDataOutputNode.getTimestampField(),
                         ksqlStructuredDataOutputNode.getKsqlTopic());

      tempMetaStore.putTopic(ksqlStructuredDataOutputNode.getKsqlTopic());
      tempMetaStore.putSource(structuredDataSource.cloneWithTimeKeyColumns());
    }
    return logicalPlan;
  }

  List<QueryMetadata> buildPhysicalPlans(
      final boolean addUniqueTimeSuffix,
      final List<Pair<String, PlanNode>> logicalPlans,
      final List<Pair<String, Statement>> statementList,
      final Map<String, Object> overriddenStreamsProperties,
      final boolean updateMetastore
  ) throws Exception {

    List<QueryMetadata> physicalPlans = new ArrayList<>();

    for (int i = 0; i < logicalPlans.size(); i++) {

      Pair<String, PlanNode> statementPlanPair = logicalPlans.get(i);
      if (statementPlanPair.getRight() == null) {
        Statement statement = statementList.get(i).getRight();
        if (!(statement instanceof  DDLStatement)) {
          throw new KsqlException("expecting a statement implementing DDLStatement but got: " + statement.getClass());
        }
        handleDdlStatement((DDLStatement)statement, overriddenStreamsProperties);
      } else {
        buildQueryPhysicalPlan(physicalPlans, addUniqueTimeSuffix, statementPlanPair,
                               overriddenStreamsProperties, updateMetastore);
      }

    }
    return physicalPlans;
  }

  private void buildQueryPhysicalPlan(final List<QueryMetadata> physicalPlans,
                                      final boolean addUniqueTimeSuffix,
                                      final Pair<String, PlanNode> statementPlanPair,
                                      final Map<String, Object> overriddenStreamsProperties,
                                      final boolean updateMetastore) throws Exception {

    final StreamsBuilder builder = new StreamsBuilder();
    final KsqlConfig ksqlConfigClone = ksqlEngine.getKsqlConfig().clone();

    // Build a physical plan, in this case a Kafka Streams DSL
    final PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder,
        ksqlConfigClone,
        ksqlEngine.getTopicClient(),
        new MetastoreUtil(),
        ksqlEngine.getFunctionRegistry(),
        addUniqueTimeSuffix,
        overriddenStreamsProperties,
        updateMetastore,
        ksqlEngine.getMetaStore(),
        queryIdCounter);

    physicalPlans.add(physicalPlanBuilder.buildPhysicalPlan(statementPlanPair));
  }


  DDLCommandResult handleDdlStatement(
      final DDLStatement statement,
      final Map<String, Object> overriddenProperties) {
    DDLCommand command = ddlCommandFactory.create(statement, overriddenProperties);
    return ksqlEngine.getDDLCommandExec().execute(command);
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
    return new KsqlStream(name, dataSource.schema(), null, null, ksqlTopic);
  }
}