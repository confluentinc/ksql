/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql;

import io.confluent.kql.analyzer.AggregateAnalysis;
import io.confluent.kql.analyzer.AggregateAnalyzer;
import io.confluent.kql.analyzer.Analysis;
import io.confluent.kql.analyzer.AnalysisContext;
import io.confluent.kql.analyzer.Analyzer;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.KQLSTDOUT;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.MetaStoreImpl;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.rewrite.AggregateExpressionRewriter;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.ExpressionTreeRewriter;
import io.confluent.kql.physical.PhysicalPlanBuilder;
import io.confluent.kql.planner.LogicalPlanner;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.planner.plan.KQLBareOutputNode;
import io.confluent.kql.planner.plan.OutputNode;
import io.confluent.kql.planner.plan.PlanNode;
import io.confluent.kql.structured.QueuedSchemaKStream;
import io.confluent.kql.structured.SchemaKStream;
import io.confluent.kql.structured.SchemaKTable;
import io.confluent.kql.util.KQLConfig;
import io.confluent.kql.util.KQLException;
import io.confluent.kql.util.Pair;
import io.confluent.kql.util.QueryMetadata;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.SelectItem;
import io.confluent.kql.parser.tree.SingleColumn;
import io.confluent.kql.parser.tree.Select;

import io.confluent.kql.util.QueuedQueryMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryEngine {

  private KQLConfig kqlConfig;

  public QueryEngine(final KQLConfig kqlConfig) {
    this.kqlConfig = kqlConfig;
  }

  public QueryMetadata processQuery(final String queryId, final Query queryNode,
                                                     final MetaStore metaStore)
      throws Exception {

    // Analyze the query to resolve the references and extract operations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(queryNode, new AnalysisContext(null, null));

    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, metaStore);
    AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter();
    for (Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null, null));
      aggregateAnalysis.getFinalSelectExpressions().add(ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, expression));

    }


    // Build a logical plan
    PlanNode logicalPlan = new LogicalPlanner(analysis, aggregateAnalysis).buildPlan();

    String applicationId = queryId + "_" + System.currentTimeMillis();
    Map<String, Object> streamsProperties = kqlConfig.getResetStreamsProperties(applicationId);

    KStreamBuilder builder = new KStreamBuilder();

    //Build a physical plan, in this case a Kafka Streams DSL
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
    SchemaKStream schemaKStream = physicalPlanBuilder.buildPhysicalPlan(logicalPlan);

    KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(streamsProperties));
    streams.start();

    QueryMetadata baseQueryMetadata = new QueryMetadata(queryId, streams, physicalPlanBuilder.getPlanSink());
    if (schemaKStream instanceof QueuedSchemaKStream) {
      QueuedSchemaKStream queuedSchemaKStream = (QueuedSchemaKStream) schemaKStream;
      return new QueuedQueryMetadata(baseQueryMetadata, queuedSchemaKStream.getQueue());
    } else {
      return baseQueryMetadata;
    }
  }

  public List<Pair<String, PlanNode>> buildLogicalPlans(final MetaStore metaStore,
                                                        final List<Pair<String, Query>> queryList) {

    List<Pair<String, PlanNode>> logicalPlansList = new ArrayList<>();
    MetaStore tempMetaStore = new MetaStoreImpl();
    for (String topicName : metaStore.getAllKQLTopics().keySet()) {
      tempMetaStore.putTopic(metaStore.getTopic(topicName));
    }
    for (String dataSourceName : metaStore.getAllStructuredDataSourceNames()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }

    for (Pair<String, Query> query : queryList) {
      // Analyze the query to resolve the references and extract operations
      Analysis analysis = new Analysis();
      Analyzer analyzer = new Analyzer(analysis, tempMetaStore);
      analyzer.process(query.getRight(), new AnalysisContext(null, null));

      AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
      AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, metaStore);
      AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter();
      for (Expression expression: analysis.getSelectExpressions()) {
        aggregateAnalyzer.process(expression, new AnalysisContext(null, null));
        if (!aggregateAnalyzer.isHasAggregateFunction()) {
          aggregateAnalysis.getNonAggResultColumns().add(expression);
        }
        aggregateAnalysis.getFinalSelectExpressions().add(ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, expression));
        aggregateAnalyzer.setHasAggregateFunction(false);
      }

      // TODO: make sure only aggregates are in the expression. For now we assume this is the case.
      if (analysis.getHavingExpression() != null) {
        aggregateAnalyzer.process(analysis.getHavingExpression(), new AnalysisContext(null, null));
        if (!aggregateAnalyzer.isHasAggregateFunction()) {
          aggregateAnalysis.getNonAggResultColumns().add(analysis.getHavingExpression());
        }
        aggregateAnalysis.setHavingExpression(ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter,
                                                              analysis.getHavingExpression()));
        aggregateAnalyzer.setHasAggregateFunction(false);
      }

      // Build a logical plan
      PlanNode logicalPlan = new LogicalPlanner(analysis, aggregateAnalysis).buildPlan();
      if (logicalPlan instanceof KQLStructuredDataOutputNode) {
        KQLStructuredDataOutputNode kqlStructuredDataOutputNode = (KQLStructuredDataOutputNode)
            logicalPlan;
        StructuredDataSource
            structuredDataSource =
            new KQLStream(kqlStructuredDataOutputNode.getId().toString(),
                          kqlStructuredDataOutputNode.getSchema(),
                          kqlStructuredDataOutputNode.getKeyField(),
                          kqlStructuredDataOutputNode.getKqlTopic());

        tempMetaStore.putTopic(kqlStructuredDataOutputNode.getKqlTopic());
        tempMetaStore.putSource(structuredDataSource);
      }

      logicalPlansList.add(new Pair<String, PlanNode>(query.getLeft(), logicalPlan));
    }
    return logicalPlansList;
  }

  private StructuredDataSource getPlanDataSource(PlanNode outputNode) {

    KQLTopic
        kqlTopic = new KQLTopic(outputNode.getId().toString(), outputNode.getId().toString(), null);
    StructuredDataSource
        structuredDataSource =
        new KQLStream(outputNode.getId().toString(), outputNode.getSchema(),
                      outputNode.getKeyField(),
                      kqlTopic);
    return structuredDataSource;
  }

  public List<QueryMetadata> buildRunPhysicalPlans(
      final boolean addUniqueTimeSuffix, final MetaStore metaStore,
      final List<Pair<String, PlanNode>> queryLogicalPlans)
      throws Exception {

    List<QueryMetadata> physicalPlans = new ArrayList<>();

    for (Pair<String, PlanNode> queryLogicalPlan : queryLogicalPlans) {
      String applicationId = queryLogicalPlan.getLeft();
      if (addUniqueTimeSuffix) {
        applicationId += "_" + System.currentTimeMillis();
      }
      Map<String, Object> streamsProperties = kqlConfig.getResetStreamsProperties(applicationId);

      KStreamBuilder builder = new KStreamBuilder();

      //Build a physical plan, in this case a Kafka Streams DSL
      PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
      SchemaKStream
          schemaKStream =
          physicalPlanBuilder.buildPhysicalPlan(queryLogicalPlan.getRight());

      KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(streamsProperties));
      streams.start();

      OutputNode outputNode = physicalPlanBuilder.getPlanSink();

      StructuredDataSource sinkDataSource;
      if (outputNode instanceof KQLStructuredDataOutputNode) {
        KQLStructuredDataOutputNode outputKafkaTopicNode = (KQLStructuredDataOutputNode) outputNode;
        physicalPlans.add(new QueryMetadata(queryLogicalPlan.getLeft(), streams, outputKafkaTopicNode));
        if (metaStore.getTopic(outputKafkaTopicNode.getKafkaTopicName()) == null) {
          metaStore.putTopic(outputKafkaTopicNode.getKqlTopic());
        }
        if (schemaKStream instanceof SchemaKTable) {
          SchemaKTable schemaKTable = (SchemaKTable) schemaKStream;
          sinkDataSource =
              new KQLTable(outputKafkaTopicNode.getId().toString(),
                           outputKafkaTopicNode.getSchema(),
                           schemaKStream.getKeyField(),
                           outputKafkaTopicNode.getKqlTopic(), outputKafkaTopicNode.getId()
                                                                   .toString() + "_statestore",
                           schemaKTable.isWindowed());
        } else {
          sinkDataSource =
              new KQLStream(outputKafkaTopicNode.getId().toString(),
                            outputKafkaTopicNode.getSchema(),
                            schemaKStream.getKeyField(),
                            outputKafkaTopicNode.getKqlTopic());
        }

        metaStore.putSource(sinkDataSource);
      } else if (outputNode instanceof KQLBareOutputNode) {
        if (!(schemaKStream instanceof QueuedSchemaKStream)) {
          throw new Exception(String.format(
              "Mismatch between logical and physical output; expected a QueuedSchemaKStream based on logical "
              + "KQLBareOutputNode, found a %s instead",
              schemaKStream.getClass().getCanonicalName()
          ));
        }
        QueuedSchemaKStream queuedSchemaKStream = (QueuedSchemaKStream) schemaKStream;
        KQLBareOutputNode kqlBareOutputNode = (KQLBareOutputNode) outputNode;
        sinkDataSource = new KQLSTDOUT(KQLSTDOUT.KQL_STDOUT_NAME, null, null, null);
        physicalPlans.add(
            new QueuedQueryMetadata(queryLogicalPlan.getLeft(), streams, kqlBareOutputNode, queuedSchemaKStream.getQueue())
        );
      } else {
        throw new KQLException("Sink data source is not correct.");
      }

    }
    return physicalPlans;
  }

  public StructuredDataSource getResultDatasource(final Select select, final String name) {

    SchemaBuilder dataSource = SchemaBuilder.struct().name(name);

    for (SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        SingleColumn singleColumn = (SingleColumn) selectItem;
        String fieldName = singleColumn.getAlias().get();
        String fieldType = null;
        dataSource = dataSource.field(fieldName, Schema.BOOLEAN_SCHEMA);
      }


    }

    KQLTopic kqlTopic = new KQLTopic(name, name,
                                     null);
    StructuredDataSource
        resultStream =
        new KQLStream(name, dataSource.schema(), dataSource.fields().get(0),
                      kqlTopic
        );
    return resultStream;
  }
}
