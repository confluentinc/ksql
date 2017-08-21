/**
 * Copyright 2017 Confluent Inc.
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
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.rewrite.AggregateExpressionRewriter;
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
    PhysicalPlanBuilder physicalPlanBuilder =
        new PhysicalPlanBuilder(builder, ksqlConfigClone, ksqlEngine.getKafkaTopicClient());
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
    String serviceId = ksqlEngine.getKsqlConfig()
        .get(KsqlConfig.KSQL_SERVICE_ID_CONFIG).toString();
    String persistance_query_prefix = ksqlEngine.getKsqlConfig()
        .get(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG).toString();
    String transient_query_prefix = ksqlEngine.getKsqlConfig()
        .get(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG).toString();
    if (isBareQuery) {
      String applicationId = getBareQueryApplicationId(serviceId, transient_query_prefix);
      if (addUniqueTimeSuffix) {
        applicationId = addTimeSuffix(applicationId);
      }

      KafkaStreams streams =
          buildStreams(builder, applicationId, ksqlConfigClone, overriddenStreamsProperties);

      QueuedSchemaKStream queuedSchemaKStream = (QueuedSchemaKStream) schemaKStream;
      KsqlBareOutputNode ksqlBareOutputNode = (KsqlBareOutputNode) outputNode;

      SchemaKStream sourceSchemaKstream = schemaKStream.getSourceSchemaKStreams().get(0);

      physicalPlans.add(new QueuedQueryMetadata(
          statementPlanPair.getLeft(),
          streams,
          ksqlBareOutputNode,
          schemaKStream.getExecutionPlan(""),
          queuedSchemaKStream.getQueue(),
          (sourceSchemaKstream instanceof SchemaKTable)?
          DataSource.DataSourceType.KTABLE: DataSource.DataSourceType.KSTREAM
      ));

    } else if (outputNode instanceof KsqlStructuredDataOutputNode) {
      long queryId = getNextQueryId();

      String applicationId =  serviceId + persistance_query_prefix +
                             queryId;
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
                                          .getExecutionPlan(""), queryId,
                                      (schemaKStream instanceof SchemaKTable)? DataSource
                                          .DataSourceType.KTABLE: DataSource.DataSourceType.KSTREAM)
      );

      MetaStore metaStore = ksqlEngine.getMetaStore();
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
                          kafkaTopicOutputNode.getKsqlTopic(),
                          kafkaTopicOutputNode.getId().toString() +
                          ksqlEngine.getKsqlConfig().get(KsqlConfig.KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG),
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
                                     ksqlEngine.getKafkaTopicClient());
    } else if (statement instanceof CreateTable) {
      return new CreateTableCommand((CreateTable) statement, overriddenProperties,
                                    ksqlEngine.getKafkaTopicClient());
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