package io.confluent.ksql;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.KQLStream;
import io.confluent.ksql.metastore.KQLTable;
import io.confluent.ksql.metastore.KQLTopic;
import io.confluent.ksql.metastore.KQL_STDOUT;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.KQLConsoleOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KSQLConfig;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.Triplet;

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

  KSQLConfig ksqlConfig;

  public QueryEngine(KSQLConfig ksqlConfig) {
    this.ksqlConfig = ksqlConfig;
  }

  public Pair<KafkaStreams, OutputNode> processQuery(String queryId, Query queryNode,
                                                               MetaStore metaStore)
      throws Exception {

    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(queryNode, new AnalysisContext(null, null));

    // Build a logical plan
    PlanNode logicalPlan = new LogicalPlanner(analysis).buildPlan();

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

  public List<Pair<String, PlanNode>> buildLogicalPlans(MetaStore metaStore,
                                                        List<Pair<String, Query>> queryList) {

    List<Pair<String, PlanNode>> logicalPlansList = new ArrayList<>();
    MetaStore tempMetaStore = new MetaStoreImpl();
    for (String topicName: metaStore.getAllKafkaTopics().keySet()) {
      tempMetaStore.putTopic(metaStore.getTopic(topicName));
    }
    for (String dataSourceName : metaStore.getAllStructuredDataSource().keySet()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }

    for (Pair<String, Query> query : queryList) {
      // Analyze the query to resolve the references and extract oeprations
      Analysis analysis = new Analysis();
      Analyzer analyzer = new Analyzer(analysis, tempMetaStore);
      analyzer.process(query.getRight(), new AnalysisContext(null, null));

      // Build a logical plan
      PlanNode logicalPlan = new LogicalPlanner(analysis).buildPlan();
      if (logicalPlan instanceof KQLStructuredDataOutputNode) {
        KQLStructuredDataOutputNode kqlStructuredDataOutputNode = (KQLStructuredDataOutputNode)
            logicalPlan;
        StructuredDataSource
            structuredDataSource =
            new KQLStream(kqlStructuredDataOutputNode.getId().toString(), kqlStructuredDataOutputNode.getSchema(),
                          kqlStructuredDataOutputNode.getKeyField(),
                          kqlStructuredDataOutputNode.getKqlTopic());

        tempMetaStore.putTopic(kqlStructuredDataOutputNode.getKqlTopic());
        tempMetaStore.putSource(structuredDataSource);
//        tempMetaStore.putSource(getPlanDataSource(logicalPlan));
      }

      logicalPlansList.add(new Pair<String, PlanNode>(query.getLeft(), logicalPlan));
    }
    return logicalPlansList;
  }

  private StructuredDataSource getPlanDataSource(PlanNode outputNode) {

    KQLTopic
        KQLTopic = new KQLTopic(outputNode.getId().toString(), outputNode.getId().toString(), null);
//    StructuredDataSource
//        structuredDataSource =
//        new KQLStream(outputNode.getId().toString(), outputNode.getSchema(),
//                       outputNode.getKeyField(),
//                      KQLTopic);
    StructuredDataSource
        structuredDataSource =
        new KQLStream(outputNode.getId().toString(), outputNode.getSchema(),
                      outputNode.getKeyField(),
                      KQLTopic);
    return structuredDataSource;
  }

  public List<Triplet<String, KafkaStreams, OutputNode>> buildRunPhysicalPlans(
      boolean isCli, MetaStore metaStore, List<Pair<String, PlanNode>> queryLogicalPlans)
      throws Exception {

    List<Triplet<String, KafkaStreams, OutputNode>> physicalPlans = new ArrayList<>();

    for (Pair<String, PlanNode> queryLogicalPlan : queryLogicalPlans) {
      Properties props = new Properties();
      if (isCli) {
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
//        if (outputKafkaTopicNode.getKqlTopic().getKqlTopicSerDe() instanceof KQLAvroTopicSerDe) {
//          outputKafkaTopicNode = addAvroSchemaToResultTopic(outputKafkaTopicNode);
//        }
        physicalPlans.add(new Triplet<>(queryLogicalPlan.getLeft(), streams, outputKafkaTopicNode));
        if (metaStore.getTopic(outputKafkaTopicNode.getKafkaTopicName()) == null) {
          metaStore.putTopic(outputKafkaTopicNode.getKqlTopic());
        }
        if (schemaKStream instanceof SchemaKTable) {
          sinkDataSource =
              new KQLTable(outputKafkaTopicNode.getId().toString(), outputKafkaTopicNode.getSchema(),
                           outputKafkaTopicNode.getKeyField(),
                           outputKafkaTopicNode.getKqlTopic(), outputKafkaTopicNode.getId()
                                                                   .toString()+"_statestore");
        } else {
          sinkDataSource =
              new KQLStream(outputKafkaTopicNode.getId().toString(), outputKafkaTopicNode.getSchema(),
                            outputKafkaTopicNode.getKeyField(),
                            outputKafkaTopicNode.getKqlTopic());
        }

        metaStore.putSource(sinkDataSource);
      } else if (outputNode instanceof KQLConsoleOutputNode) {
        KQLConsoleOutputNode KQLConsoleOutputNode = (KQLConsoleOutputNode) outputNode;
        sinkDataSource = new KQL_STDOUT(KQL_STDOUT.KQL_STDOUT_NAME, null, null, null);
        physicalPlans.add(new Triplet<>(queryLogicalPlan.getLeft(), streams, KQLConsoleOutputNode));
      } else {
        throw new KSQLException("Sink data source is not correct.");
      }

    }
    return physicalPlans;
  }

  public void buildRunSingleConsolePhysicalPlans(MetaStore metaStore,
                                                 Pair<String, PlanNode> queryLogicalPlan, long terminateIn)
      throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG,
              "KSQL_CONSOLE_QUERY_" + queryLogicalPlan.getLeft() + "_" + System
                  .currentTimeMillis());

    props = initProps(props);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    PlanNode logicalPlan = queryLogicalPlan.getRight();
    KQLConsoleOutputNode KQLConsoleOutputNode = null;
    if (logicalPlan instanceof KQLStructuredDataOutputNode) {
      KQLConsoleOutputNode =
          new KQLConsoleOutputNode(logicalPlan.getId(),
                                   ((KQLStructuredDataOutputNode) logicalPlan).getSource(),
                                   logicalPlan.getSchema());
    }

    KStreamBuilder builder = new KStreamBuilder();

    //Build a physical plan, in this case a Kafka Streams DSL
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
    SchemaKStream schemaKStream = physicalPlanBuilder.buildPhysicalPlan(KQLConsoleOutputNode);

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    if (terminateIn >= 0) {
      Thread.sleep(terminateIn);
      streams.close();
    }

  }

  private Properties initProps(Properties props) {

    if ((ksqlConfig.getList(KSQLConfig.BOOTSTRAP_SERVERS_CONFIG) != null) && (!ksqlConfig
        .getList(KSQLConfig.BOOTSTRAP_SERVERS_CONFIG).isEmpty())) {
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                ksqlConfig.getList(KSQLConfig.BOOTSTRAP_SERVERS_CONFIG));
    } else {
      props
          .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KSQLConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
    }

    if (ksqlConfig.values().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) != null) {
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                ksqlConfig.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    } else {
      // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                KSQLConfig.DEFAULT_AUTO_OFFSET_RESET_CONFIG);
    }
    return props;
  }

  public StructuredDataSource getResultDatasource(Select select, String name) {

    SchemaBuilder dataSource = SchemaBuilder.struct().name(name);

    for (SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        SingleColumn singleColumn = (SingleColumn) selectItem;
        String fieldName = singleColumn.getAlias().get();
        String fieldType = null;
        dataSource = dataSource.field(fieldName, Schema.BOOLEAN_SCHEMA);
      }


    }

    KQLTopic KQLTopic = new KQLTopic(name, name,
                                     null);
    StructuredDataSource
        resultStream =
        new KQLStream(name, dataSource.schema(), dataSource.fields().get(0),
                      KQLTopic
        );
    return resultStream;
  }

//  private KQLStructuredDataOutputNode addAvroSchemaToResultTopic(KQLStructuredDataOutputNode
//                                                                     kqlStructuredDataOutputNode) {
//
//    String avroSchemaFilePath = "/tmp/"+kqlStructuredDataOutputNode.getKqlTopic().getName()+".avro";
//    MetastoreUtil metastoreUtil = new MetastoreUtil();
//    String avroSchema = metastoreUtil.buildAvroSchema(kqlStructuredDataOutputNode.getSchema(), kqlStructuredDataOutputNode.getKqlTopic().getName());
//    metastoreUtil.writeAvroSchemaFile(avroSchema,avroSchemaFilePath);
//    KQLAvroTopicSerDe kqlAvroTopicSerDe = new KQLAvroTopicSerDe(avroSchemaFilePath, avroSchema);
//    KQLTopic newKQLTopic = new KQLTopic(kqlStructuredDataOutputNode.getKqlTopic()
//                                            .getName(), kqlStructuredDataOutputNode
//                                            .getKqlTopic().getKafkaTopicName(),kqlAvroTopicSerDe);
//
//    KQLStructuredDataOutputNode newKQLStructuredDataOutputNode = new KQLStructuredDataOutputNode
//        (kqlStructuredDataOutputNode.getId(), kqlStructuredDataOutputNode.getSource(),
//         kqlStructuredDataOutputNode.getSchema(), newKQLTopic, kqlStructuredDataOutputNode.getKafkaTopicName());
//    return newKQLStructuredDataOutputNode;
//  }

}
