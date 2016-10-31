package io.confluent.ksql;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.OutputKafkaTopicNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.structured.SchemaStream;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KSQLConfig;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.List;
import java.util.Properties;


public class QueryEngine {

    KSQLConfig ksqlConfig;
    public QueryEngine(KSQLConfig ksqlConfig) {
        this.ksqlConfig = ksqlConfig;
    }

//    public List<Statement> getStatements(String sqlString) {
//        // First parse the query and build the AST
//        KSQLParser ksqlParser = new KSQLParser();
//        Node root = ksqlParser.buildAST(sqlString);
//
//        if(root instanceof Statements) {
//            Statements statements = (Statements) root;
//            return statements.statementList;
//        }
//        throw new StreamsException("Error in parsing. Cannot get the set of statements.");
//    }

    public Pair<KafkaStreams, OutputKafkaTopicNode> processQuery(String queryId, Query queryNode, MetaStore metaStore, DataSourceExtractor dataSourceExtractor) throws Exception {

        // Analyze the query to resolve the references and extract oeprations
        Analysis analysis = new Analysis();
        Analyzer analyzer = new Analyzer(analysis,metaStore, dataSourceExtractor);
        analyzer.process(queryNode, new AnalysisContext(null, null));

        // Build a physical plan
        PlanNode logicalPlan = new LogicalPlanner(analysis).buildPlan();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, queryId);

        if((ksqlConfig.getList(KSQLConfig.BOOTSTRAP_SERVERS_CONFIG) != null) && (!ksqlConfig.getList(KSQLConfig.BOOTSTRAP_SERVERS_CONFIG).isEmpty())){
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ksqlConfig.getList(KSQLConfig.BOOTSTRAP_SERVERS_CONFIG));
        } else {
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        }

        if(ksqlConfig.values().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) != null) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ksqlConfig.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        } else {
            // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        KStreamBuilder builder = new KStreamBuilder();

        //Build a physical plan, in this case a Kafka Streams DSL
        PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
        SchemaStream schemaStream = physicalPlanBuilder.buildPhysicalPlan(logicalPlan);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        return new Pair<>(streams, physicalPlanBuilder.getPlanSink());

    }

}
