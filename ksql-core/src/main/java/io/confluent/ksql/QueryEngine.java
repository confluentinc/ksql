/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.rewrite.AggregateExpressionRewriter;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
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
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class QueryEngine {

  private static final Logger log = LoggerFactory.getLogger(QueryEngine.class);
  private final AtomicLong queryIdCounter;


  public QueryEngine(KsqlConfig ksqlConfig) {
    Objects.requireNonNull(
        ksqlConfig, "Streams properties map cannot be null as it may be mutated later on");
    this.queryIdCounter = new AtomicLong(1);
  }


  public List<Pair<String, PlanNode>> buildLogicalPlans(MetaStore metaStore,
                                                        List<Pair<String, Query>> queryList) {

    List<Pair<String, PlanNode>> logicalPlansList = new ArrayList<>();
    // TODO: the purpose of tempMetaStore here
    MetaStore tempMetaStore = metaStore.clone();

    for (Pair<String, Query> statementQueryPair : queryList) {
      Query query = statementQueryPair.getRight();
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

      logicalPlansList.add(new Pair<>(statementQueryPair.getLeft(), logicalPlan));
      log.info("Build logical plan for {}.", statementQueryPair.getLeft());
    }
    return logicalPlansList;
  }

  public List<QueryMetadata> buildPhysicalPlans(
      boolean addUniqueTimeSuffix,
      MetaStore metaStore,
      List<Pair<String, PlanNode>> logicalPlans,
      KsqlConfig ksqlConfig,
      Map<String, Object> overriddenStreamsProperties,
      boolean updateMetastore
  ) throws Exception {

    List<QueryMetadata> physicalPlans = new ArrayList<>();

    for (Pair<String, PlanNode> statementPlanPair : logicalPlans) {

      PlanNode logicalPlan = statementPlanPair.getRight();
      KStreamBuilder builder = new KStreamBuilder();

      KsqlConfig ksqlConfigClone = ksqlConfig.clone();

      // Build a physical plan, in this case a Kafka Streams DSL
      PhysicalPlanBuilder physicalPlanBuilder =
          new PhysicalPlanBuilder(builder, ksqlConfigClone);
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

      if (isBareQuery) {
        String applicationId = getBareQueryApplicationId();
        if (addUniqueTimeSuffix) {
          applicationId = addTimeSuffix(applicationId);
        }

        KafkaStreams streams =
            buildStreams(builder, applicationId, ksqlConfigClone, overriddenStreamsProperties);

        QueuedSchemaKStream queuedSchemaKStream = (QueuedSchemaKStream) schemaKStream;
        KsqlBareOutputNode ksqlBareOutputNode = (KsqlBareOutputNode) outputNode;
        physicalPlans.add(new QueuedQueryMetadata(
            statementPlanPair.getLeft(),
            streams,
            ksqlBareOutputNode,
            schemaKStream.getExecutionPlan(""),
            queuedSchemaKStream.getQueue()
        ));

      } else if (outputNode instanceof KsqlStructuredDataOutputNode) {
        long queryId = getNextQueryId();
        String applicationId = String.format("ksql_query_%d", queryId);
        if (addUniqueTimeSuffix) {
          applicationId = addTimeSuffix(applicationId);
        }

        KafkaStreams streams =
            buildStreams(builder, applicationId, ksqlConfigClone, overriddenStreamsProperties);

        KsqlStructuredDataOutputNode kafkaTopicOutputNode =
            (KsqlStructuredDataOutputNode) outputNode;
        physicalPlans.add(
            new PersistentQueryMetadata(statementPlanPair.getLeft(),
                                        streams, kafkaTopicOutputNode, schemaKStream
                                            .getExecutionPlan(""), queryId)
        );
        if (metaStore.getTopic(kafkaTopicOutputNode.getKafkaTopicName()) == null) {
          metaStore.putTopic(kafkaTopicOutputNode.getKsqlTopic());
        }
        StructuredDataSource sinkDataSource;
        if (schemaKStream instanceof SchemaKTable) {
          SchemaKTable schemaKTable = (SchemaKTable) schemaKStream;
          sinkDataSource =
              new KsqlTable(kafkaTopicOutputNode.getId().toString(),
                  kafkaTopicOutputNode.getSchema(),
                  schemaKStream.getKeyField(),
                  kafkaTopicOutputNode.getTimestampField(),
                  kafkaTopicOutputNode.getKsqlTopic(), kafkaTopicOutputNode.getId()
                  .toString() + "_statestore",
                  schemaKTable.isWindowed());
        } else {
          sinkDataSource =
              new KsqlStream(kafkaTopicOutputNode.getId().toString(),
                  kafkaTopicOutputNode.getSchema(),
                  schemaKStream.getKeyField(),
                  kafkaTopicOutputNode.getTimestampField(),
                  kafkaTopicOutputNode.getKsqlTopic());
        }

        if (updateMetastore) {
          metaStore.putSource(sinkDataSource.cloneWithTimeKeyColumns());
        }
      } else {
        throw new KsqlException("Sink data source is not correct.");
      }
      log.info("Build physical plan for {}.", statementPlanPair.getLeft());
      log.info(" Execution plan: \n");
      log.info(schemaKStream.getExecutionPlan(""));
    }
    return physicalPlans;
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
  private String getBareQueryApplicationId() {
    return String.format("ksql_bare_query_%d", Math.abs(ThreadLocalRandom.current().nextLong()));
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
