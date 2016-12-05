package io.confluent.ksql;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KQL_STDOUT;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.OutputKSQLConsoleNode;
import io.confluent.ksql.planner.plan.OutputKafkaTopicNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KSQLConfig;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.Triplet;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    for (String dataSourceName : metaStore.getAllDataSources().keySet()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }

    for (Pair<String, Query> query : queryList) {
      // Analyze the query to resolve the references and extract oeprations
      Analysis analysis = new Analysis();
      Analyzer analyzer = new Analyzer(analysis, tempMetaStore);
      analyzer.process(query.getRight(), new AnalysisContext(null, null));

      // Build a logical plan
      PlanNode logicalPlan = new LogicalPlanner(analysis).buildPlan();
      tempMetaStore.putSource(getPlanDataSource(logicalPlan));
      logicalPlansList.add(new Pair<String, PlanNode>(query.getLeft(), logicalPlan));
    }
    return logicalPlansList;
  }

  private DataSource getPlanDataSource(PlanNode outputNode) {

    KafkaTopic
        kafkaTopic =
        new KafkaTopic(outputNode.getId().toString(), outputNode.getSchema(),
                       outputNode.getKeyField(), DataSource.DataSourceType.KSTREAM,
                       outputNode.getId().toString());
    return kafkaTopic;
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

      KStreamBuilder builder = new KStreamBuilder();

      //Build a physical plan, in this case a Kafka Streams DSL
      PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
      SchemaKStream
          schemaKStream =
          physicalPlanBuilder.buildPhysicalPlan(queryLogicalPlan.getRight());

      KafkaStreams streams = new KafkaStreams(builder, props);
      streams.start();

      OutputNode outputNode = physicalPlanBuilder.getPlanSink();

      DataSource sinkDataSource;
//      OutputKafkaTopicNode outputKafkaTopicNode = physicalPlanBuilder.getPlanSink();
      if (outputNode instanceof OutputKafkaTopicNode) {
        OutputKafkaTopicNode outputKafkaTopicNode = (OutputKafkaTopicNode) outputNode;
            physicalPlans.add(new Triplet<>(queryLogicalPlan.getLeft(), streams, outputKafkaTopicNode));
        sinkDataSource =
            new KafkaTopic(outputKafkaTopicNode.getId().toString(), outputKafkaTopicNode.getSchema(),
                           outputKafkaTopicNode.getKeyField(), DataSource.DataSourceType.KSTREAM,
                           outputKafkaTopicNode.getKafkaTopicName());
        metaStore.putSource(sinkDataSource);
      } else if (outputNode instanceof OutputKSQLConsoleNode) {
        OutputKSQLConsoleNode outputKSQLConsoleNode = (OutputKSQLConsoleNode) outputNode;
        sinkDataSource = new KQL_STDOUT(KQL_STDOUT.KQL_STDOUT_NAME, null, null, null);
        physicalPlans.add(new Triplet<>(queryLogicalPlan.getLeft(), streams, outputKSQLConsoleNode));
      } else {
        throw new KSQLException("Sink data source is not correct.");
      }

//      metaStore.putSource(sinkDataSource);
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

    PlanNode logicalPlan = queryLogicalPlan.getRight();
    OutputKSQLConsoleNode outputKSQLConsoleNode = null;
    if (logicalPlan instanceof OutputKafkaTopicNode) {
      outputKSQLConsoleNode =
          new OutputKSQLConsoleNode(logicalPlan.getId(),
                                    ((OutputKafkaTopicNode) logicalPlan).getSource(),
                                    logicalPlan.getSchema());
    }

    KStreamBuilder builder = new KStreamBuilder();

    //Build a physical plan, in this case a Kafka Streams DSL
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
    SchemaKStream schemaKStream = physicalPlanBuilder.buildPhysicalPlan(outputKSQLConsoleNode);

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

}
