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
import io.confluent.kql.planner.plan.KQLConsoleOutputNode;
import io.confluent.kql.planner.plan.OutputNode;
import io.confluent.kql.planner.plan.PlanNode;
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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class QueryEngine {

  KQLConfig kqlConfig;

  public QueryEngine(final KQLConfig kqlConfig) {
    this.kqlConfig = kqlConfig;
  }

  public Pair<KafkaStreams, OutputNode> processQuery(final String queryId, final Query queryNode,
                                                     final MetaStore metaStore)
      throws Exception {

    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(queryNode, new AnalysisContext(null, null));

    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, metaStore);
    AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter();
    for (Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null, null));
      aggregateAnalysis.getFinalSelectExpressions().add(ExpressionTreeRewriter.rewriteWith
          (aggregateExpressionRewriter, expression));

    }


    // Build a logical plan
    PlanNode logicalPlan = new LogicalPlanner(analysis, aggregateAnalysis).buildPlan();

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, queryId + "-" + System.currentTimeMillis());
    props = initProps(props);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    KStreamBuilder builder = new KStreamBuilder();

    //Build a physical plan, in this case a Kafka Streams DSL
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
    SchemaKStream schemaKStream = physicalPlanBuilder.buildPhysicalPlan(logicalPlan);

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    return new Pair<>(streams, physicalPlanBuilder.getPlanSink());

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
      // Analyze the query to resolve the references and extract oeprations
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
        aggregateAnalysis.getFinalSelectExpressions().add(ExpressionTreeRewriter.rewriteWith
            (aggregateExpressionRewriter, expression));
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
      Properties props = new Properties();
      if (addUniqueTimeSuffix) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                  queryLogicalPlan.getLeft() + "_" + System.currentTimeMillis());
      } else {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, queryLogicalPlan.getLeft());
      }

      props = initProps(props);
      props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
      props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

      KStreamBuilder builder = new KStreamBuilder();

      //Build a physical plan, in this case a Kafka Streams DSL
      PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
      SchemaKStream
          schemaKStream =
          physicalPlanBuilder.buildPhysicalPlan(queryLogicalPlan.getRight());

      KafkaStreams streams = new KafkaStreams(builder, props);
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
          sinkDataSource =
              new KQLTable(outputKafkaTopicNode.getId().toString(),
                           outputKafkaTopicNode.getSchema(),
                           outputKafkaTopicNode.getKeyField(),
                           outputKafkaTopicNode.getKqlTopic(), outputKafkaTopicNode.getId()
                                                                   .toString() + "_statestore");
        } else {
          sinkDataSource =
              new KQLStream(outputKafkaTopicNode.getId().toString(),
                            outputKafkaTopicNode.getSchema(),
                            outputKafkaTopicNode.getKeyField(),
                            outputKafkaTopicNode.getKqlTopic());
        }

        metaStore.putSource(sinkDataSource);
      } else if (outputNode instanceof KQLConsoleOutputNode) {
        KQLConsoleOutputNode kqlConsoleOutputNode = (KQLConsoleOutputNode) outputNode;
        sinkDataSource = new KQLSTDOUT(KQLSTDOUT.KQL_STDOUT_NAME, null, null, null);
        physicalPlans.add(new QueryMetadata(queryLogicalPlan.getLeft(), streams, kqlConsoleOutputNode));
      } else {
        throw new KQLException("Sink data source is not correct.");
      }

    }
    return physicalPlans;
  }

  private Properties initProps(final Properties props) {

    if ((kqlConfig.getList(KQLConfig.BOOTSTRAP_SERVERS_CONFIG) != null) && (!kqlConfig
        .getList(KQLConfig.BOOTSTRAP_SERVERS_CONFIG).isEmpty())) {
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                kqlConfig.getList(KQLConfig.BOOTSTRAP_SERVERS_CONFIG));
    } else {
      props
          .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KQLConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
    }

    if (kqlConfig.values().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) != null) {
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                kqlConfig.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    } else {
      // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                KQLConfig.DEFAULT_AUTO_OFFSET_RESET_CONFIG);
    }
    return props;
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
