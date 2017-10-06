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
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.CreateTableCommand;
import io.confluent.ksql.ddl.commands.DDLCommand;
import io.confluent.ksql.ddl.commands.DDLCommandResult;
import io.confluent.ksql.ddl.commands.DropSourceCommand;
import io.confluent.ksql.ddl.commands.DropTopicCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.structured.QueuedSchemaKStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.timestamp.KsqlTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class QueryEngine {

  private static final Logger log = LoggerFactory.getLogger(QueryEngine.class);
  private final AtomicLong queryIdCounter;
  private final KsqlEngine ksqlEngine;


  public QueryEngine(final KsqlEngine ksqlEngine) {
    this.queryIdCounter = new AtomicLong(1);
    this.ksqlEngine = ksqlEngine;
  }


  public List<Pair<String, PlanNode>> buildLogicalPlans(
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

  public PlanNode buildQueryLogicalPlan(final Query query, final MetaStore tempMetaStore) {

    // Analyze the query to resolve the references and extract operations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, tempMetaStore);
    analyzer.process(query, new AnalysisContext(null, null));

    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new
        AggregateAnalyzer(aggregateAnalysis, tempMetaStore, analysis);
    AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter();
    for (Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer
          .process(expression, new AnalysisContext(null, null));
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.getNonAggResultColumns().add(expression);
      }
      aggregateAnalysis.getFinalSelectExpressions()
          .add(ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, expression));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }

    if (!aggregateAnalysis.getAggregateFunctionArguments().isEmpty() &&
        analysis.getGroupByExpressions().isEmpty()) {
      throw new KsqlException("Aggregate query needs GROUP BY clause.");
    }
    // TODO: make sure only aggregates are in the expression. For now we assume this is the case.
    if (analysis.getHavingExpression() != null) {
      aggregateAnalyzer.process(analysis.getHavingExpression(),
                                new AnalysisContext(null, null));
      if (!aggregateAnalyzer.isHasAggregateFunction()) {
        aggregateAnalysis.getNonAggResultColumns().add(analysis.getHavingExpression());
      }
      aggregateAnalysis
          .setHavingExpression(ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter,
                                                                  analysis.getHavingExpression()));
      aggregateAnalyzer.setHasAggregateFunction(false);
    }

    enforceAggregateRules(query, aggregateAnalysis);


    // Build a logical plan
    PlanNode logicalPlan = new LogicalPlanner(analysis, aggregateAnalysis).buildPlan();
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

  public List<QueryMetadata> buildPhysicalPlans(
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
        handleDdlStatement(statementList.get(i).getRight(), overriddenStreamsProperties);
      } else {
        buildQueryPhysicalPlan(physicalPlans, addUniqueTimeSuffix, statementPlanPair,
                               overriddenStreamsProperties, updateMetastore);
      }

    }
    return physicalPlans;
  }

  public void buildQueryPhysicalPlan(final List<QueryMetadata> physicalPlans,
                                     final boolean addUniqueTimeSuffix,
                                     final Pair<String, PlanNode> statementPlanPair,
                                     final Map<String, Object> overriddenStreamsProperties,
                                     final boolean updateMetastore) throws Exception {

    PlanNode logicalPlan = statementPlanPair.getRight();
    KStreamBuilder builder = new KStreamBuilder();

    KsqlConfig ksqlConfigClone = ksqlEngine.getKsqlConfig().clone();

    // Build a physical plan, in this case a Kafka Streams DSL
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder, ksqlConfigClone, ksqlEngine.getTopicClient());
    SchemaKStream schemaKStream = physicalPlanBuilder.buildPhysicalPlan(logicalPlan);

    OutputNode outputNode = physicalPlanBuilder.getPlanSink();
    boolean isBareQuery = outputNode instanceof KsqlBareOutputNode;

    // Check to make sure the logical and physical plans match up;
    // important to do this BEFORE actually starting up
    // the corresponding Kafka Streams job
    if (isBareQuery && !(schemaKStream instanceof QueuedSchemaKStream)) {
      throw new Exception(String.format(
          "Mismatch between logical and physical output; "
          + "expected a QueuedSchemaKStream based on logical "
          + "KsqlBareOutputNode, found a %s instead",
          schemaKStream.getClass().getCanonicalName()
      ));
    }
    String serviceId = ksqlEngine.getKsqlConfig().get(KsqlConfig.KSQL_SERVICE_ID_CONFIG).toString();
    String persistanceQueryPrefix = ksqlEngine.getKsqlConfig().get(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG).toString();
    String transientQueryPrefix = ksqlEngine.getKsqlConfig().get(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG).toString();

    if (isBareQuery) {

      physicalPlans.add(buildPlanForBareQuery(addUniqueTimeSuffix, statementPlanPair, overriddenStreamsProperties,
                                              builder, ksqlConfigClone, (QueuedSchemaKStream) schemaKStream, (KsqlBareOutputNode) outputNode,
                                              serviceId, transientQueryPrefix));

    } else if (outputNode instanceof KsqlStructuredDataOutputNode) {

      physicalPlans.add(buildPlanForStructuredOutputNode(addUniqueTimeSuffix, statementPlanPair,
                                                         overriddenStreamsProperties, updateMetastore, builder, ksqlConfigClone, schemaKStream,
                                                         (KsqlStructuredDataOutputNode) outputNode, serviceId, persistanceQueryPrefix));

    } else {
      throw new KsqlException("Sink data source is not correct.");
    }

    log.info("Build physical plan for {}.", statementPlanPair.getLeft());
    log.info(" Execution plan: \n");
    log.info(schemaKStream.getExecutionPlan(""));
  }

  /**
   *
   * @param addUniqueTimeSuffix
   * @param statementPlanPair
   * @param overriddenStreamsProperties
   * @param builder
   * @param ksqlConfigClone
   * @param bareOutputNode
   * @param serviceId
   * @param transientQueryPrefix
   */
  private QueryMetadata buildPlanForBareQuery(boolean addUniqueTimeSuffix,
                                              Pair<String, PlanNode> statementPlanPair, Map<String, Object> overriddenStreamsProperties,
                                              KStreamBuilder builder, KsqlConfig ksqlConfigClone, QueuedSchemaKStream schemaKStream,
                                              KsqlBareOutputNode bareOutputNode, String serviceId, String transientQueryPrefix) {

    String applicationId = getBareQueryApplicationId(serviceId, transientQueryPrefix);
    if (addUniqueTimeSuffix) {
      applicationId = addTimeSuffix(applicationId);
    }

    KafkaStreams streams = buildStreams(builder, applicationId, ksqlConfigClone, overriddenStreamsProperties);

    SchemaKStream sourceSchemaKstream = schemaKStream.getSourceSchemaKStreams().get(0);

    return new QueuedQueryMetadata(
        statementPlanPair.getLeft(),
        streams,
        bareOutputNode,
        schemaKStream.getExecutionPlan(""),
        schemaKStream.getQueue(),
        (sourceSchemaKstream instanceof SchemaKTable) ?
        DataSource.DataSourceType.KTABLE : DataSource.DataSourceType.KSTREAM
    );
  }

  /**
   *
   * @param addUniqueTimeSuffix
   * @param statementPlanPair
   * @param overriddenStreamsProperties
   * @param updateMetastore
   * @param builder
   * @param ksqlConfigClone
   * @param schemaKStream
   * @param serviceId
   * @param persistanceQueryPrefix
   */
  private QueryMetadata buildPlanForStructuredOutputNode(boolean addUniqueTimeSuffix,
                                                         Pair<String, PlanNode> statementPlanPair, Map<String, Object> overriddenStreamsProperties,
                                                         boolean updateMetastore, KStreamBuilder builder, KsqlConfig ksqlConfigClone, SchemaKStream schemaKStream,
                                                         KsqlStructuredDataOutputNode outputNode, String serviceId, String persistanceQueryPrefix) {

    long queryId = getNextQueryId();

    String applicationId = serviceId + persistanceQueryPrefix + queryId;
    if (addUniqueTimeSuffix) {
      applicationId = addTimeSuffix(applicationId);
    }

    MetaStore metaStore = ksqlEngine.getMetaStore();
    if (metaStore.getTopic(outputNode.getKafkaTopicName()) == null) {
      metaStore.putTopic(outputNode.getKsqlTopic());
    }
    StructuredDataSource sinkDataSource;
    if (schemaKStream instanceof SchemaKTable) {
      SchemaKTable schemaKTable = (SchemaKTable) schemaKStream;
      sinkDataSource =
          new KsqlTable(outputNode.getId().toString(),
                        outputNode.getSchema(),
                        schemaKStream.getKeyField(),
                        outputNode.getTimestampField(),
                        outputNode.getKsqlTopic(),
                        outputNode.getId().toString() +
                        ksqlEngine.getKsqlConfig().get(KsqlConfig.KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG),
                        schemaKTable.isWindowed());
    } else {
      sinkDataSource =
          new KsqlStream(outputNode.getId().toString(),
                         outputNode.getSchema(),
                         schemaKStream.getKeyField(),
                         outputNode.getTimestampField(),
                         outputNode.getKsqlTopic());
    }

    if (updateMetastore) {
      metaStore.putSource(sinkDataSource.cloneWithTimeKeyColumns());
    }
    KafkaStreams streams = buildStreams(builder, applicationId, ksqlConfigClone, overriddenStreamsProperties);

    return new PersistentQueryMetadata(statementPlanPair.getLeft(),
                                       streams, outputNode, schemaKStream
                                           .getExecutionPlan(""), queryId,
                                       (schemaKStream instanceof SchemaKTable) ? DataSource
                                           .DataSourceType.KTABLE : DataSource.DataSourceType.KSTREAM);
  }


  public DDLCommandResult handleDdlStatement(
      final Statement statement,
      final Map<String, Object> overriddenProperties) {
    if (statement instanceof SetProperty) {
      SetProperty setProperty = (SetProperty) statement;
      overriddenProperties.put(setProperty.getPropertyName(), setProperty.getPropertyValue());
      return null;
    }
    DDLCommand command = generateDDLCommand(statement, overriddenProperties);
    return ksqlEngine.getDDLCommandExec().execute(command);
  }

  private DDLCommand generateDDLCommand(
      final Statement statement,
      final Map<String, Object> overriddenProperties) {
    if (statement instanceof RegisterTopic) {
      return new RegisterTopicCommand((RegisterTopic) statement, overriddenProperties);
    } else if (statement instanceof CreateStream) {
      return new CreateStreamCommand((CreateStream) statement, overriddenProperties,
                                     ksqlEngine.getTopicClient());
    } else if (statement instanceof CreateTable) {
      return new CreateTableCommand((CreateTable) statement, overriddenProperties,
                                    ksqlEngine.getTopicClient());
    } else if (statement instanceof DropStream) {
      return new DropSourceCommand((DropStream) statement);
    } else if (statement instanceof DropTable) {
      return new DropSourceCommand((DropTable) statement);
    } else if (statement instanceof DropTopic) {
      return new DropTopicCommand((DropTopic) statement);
    } else {
      throw new KsqlException(
          "Corresponding command not found for statement: " + statement.toString());
    }
  }

  public StructuredDataSource getResultDatasource(final Select select, final String name) {

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

  private KafkaStreams buildStreams(
      final KStreamBuilder builder,
      final String applicationId,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    Map<String, Object> newStreamsProperties = ksqlConfig.getKsqlConfigProps();
    newStreamsProperties.putAll(overriddenProperties);
    newStreamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    newStreamsProperties.put(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        ksqlConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    newStreamsProperties.put(
        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        ksqlConfig.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
    newStreamsProperties.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        ksqlConfig.get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG));
    if (ksqlConfig.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX) != null) {
      newStreamsProperties.put(
          KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX,
          ksqlConfig.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX));
      newStreamsProperties.put(
          StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, KsqlTimestampExtractor.class);
    }


    return new KafkaStreams(builder, new StreamsConfig(newStreamsProperties));
  }

  private long getNextQueryId() {
    return queryIdCounter.getAndIncrement();
  }

  // TODO: This should probably be changed
  private String getBareQueryApplicationId(String serviceId, String transientQueryPrefix) {
    return  serviceId + transientQueryPrefix +
            Math.abs(ThreadLocalRandom.current().nextLong());
  }

  private String addTimeSuffix(String original) {
    return String.format("%s_%d", original, System.currentTimeMillis());
  }

  private void enforceAggregateRules(Query query, AggregateAnalysis aggregateAnalysis) {
    if (!((QuerySpecification) query.getQueryBody()).getGroupBy().isPresent()) {
      return;
    }
    int numberOfNonAggProjections = aggregateAnalysis.getNonAggResultColumns().size();
    int groupBySize = ((QuerySpecification) query.getQueryBody()).getGroupBy().get()
        .getGroupingElements().size();
    if (numberOfNonAggProjections != groupBySize) {
      throw new KsqlException("Group by elements should match the SELECT expressions.");
    }
  }

}