package io.confluent.ksql;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.Schema;
import io.confluent.ksql.planner.SchemaField;
import io.confluent.ksql.planner.plan.OutputKafkaTopicNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.types.IntegerType;
import io.confluent.ksql.planner.types.LongType;
import io.confluent.ksql.planner.types.StringType;
import io.confluent.ksql.structured.SchemaStream;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class QueryEngine {

    MetaStore metaStore = null;

    public void initMetaStore() {

        metaStore = new MetaStoreImpl();

        List<SchemaField> schemaFields = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        SchemaField orderTime = new SchemaField("ordertime", LongType.LONG);
        schemaFields.add(orderTime);
        fieldNames.add(orderTime.getFieldName());

        SchemaField orderId = new SchemaField("orderid", StringType.STRING);
        schemaFields.add(orderId);
        fieldNames.add(orderId.getFieldName());

        SchemaField orderItemId = new SchemaField("itemId", StringType.STRING);
        schemaFields.add(orderItemId);
        fieldNames.add(orderItemId.getFieldName());

        SchemaField orderUnits = new SchemaField("orderunits", IntegerType.INTEGER);
        schemaFields.add(orderUnits);
        fieldNames.add(orderUnits.getFieldName());

        Schema orderSchema = new Schema(schemaFields, fieldNames);

//        KafkaTopic kafkaTopic = new KafkaTopic("orders", orderSchema, DataSource.DataSourceType.STREAM, "order");

        KafkaTopic kafkaTopic = new KafkaTopic("orders", orderSchema, DataSource.DataSourceType.STREAM, "StreamExample1-GenericRow-order");

        metaStore.putSource(kafkaTopic);
    }

    public List<Statement> getStatements(String sqlString) {
        // First parse the query and build the AST
        KSQLParser ksqlParser = new KSQLParser();
        Node root = ksqlParser.buildAST(sqlString);

        if(root instanceof Statements) {
            Statements statements = (Statements) root;
            return statements.statementList;
        }
        throw new StreamsException("Error in parsing. Cannot get the set of statements.");
    }


    public void executeCommand(Statement command) {
        System.out.println(command.toString());
    }

    public Pair<KafkaStreams, OutputKafkaTopicNode> processQuery(Query queryNode) throws Exception {

        // Analyze the query to resolve the references and extract oeprations
        Analysis analysis = new Analysis();
        Analyzer analyzer = new Analyzer(analysis,metaStore);
        analyzer.process(queryNode, new AnalysisContext(null, null));

        // Build a physical plan
        PlanNode logicalPlan = new LogicalPlanner(analysis).buildPlan();



        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KSQL_"+System.currentTimeMillis());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        //Build a physical plan, in this case a Kafka Streams DSL
        PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
        SchemaStream schemaStream = physicalPlanBuilder.buildPhysicalPlan(logicalPlan);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        return new Pair<>(streams, physicalPlanBuilder.getPlanSink());

    }


    public void processStatements(String statementsString) throws Exception {
        if (!statementsString.endsWith(";")) {
            statementsString = statementsString + ";";
        }
        List<Statement> statements = getStatements(statementsString);
        for(Statement statement: statements) {
            if(statement instanceof Query) {
                processQuery((Query)statement);
            } else {
                executeCommand(statement);
            }
        }
    }

    public QueryEngine() {
        initMetaStore();
    }

    public MetaStore getMetaStore() {
        return metaStore;
    }

    public static void main(String[] args) throws Exception {

        QueryEngine queryEngine = new QueryEngine();
//        queryEngine.processQuery("SELECT timeField, idField, itemIdField, unitsField FROM ordersStream WHERE idField = 100".toLowerCase());
//        queryEngine.processQuery("SELECT t1, unitsField INTO test FROM ordersStream WHERE idField = 100;".toUpperCase());
//        queryEngine.processQuery("SELECT ordertime AS timeValue, orderid, orderunits*10 FROM orders WHERE NOT (orderunits > 5 AND orderunits < 8);".toUpperCase());
//        queryEngine.processQuery("SELECT ordertime AS timeValue, orderid, orderunits*10+5 FROM orders WHERE orderunits > 5 ;".toUpperCase());
//        queryEngine.processQuery("SELECT ordertime AS timeValue, orderid, orderunits*10+5 FROM orders WHERE orderunits > 5 ; show queries;".toUpperCase());

//        queryEngine.processStatements("show queries; SELECT ordertime AS timeValue, orderid, orderunits*10+5 FROM orders WHERE orderunits > 5 ; ".toUpperCase());
//        queryEngine.processStatements("show queries".toUpperCase());
//        queryEngine.processStatements("SELECT ordertime AS timeValue, orderid, orderunits*10+5 FROM orders WHERE orderunits > 5 ;".toUpperCase());
//        queryEngine.processStatements("SELECT ordertime AS timeValue, itemid, orderid, orderunits*10+5 FROM orders WHERE itemid = 'Item_5' ;".toUpperCase());
//        queryEngine.processStatements("SELECT ordertime AS timeValue, orderid, orderunits*10+5 FROM orders WHERE orderunits > 5 ;".toUpperCase());
        queryEngine.processStatements("SELECT ordertime AS timeValue, orderid, orderunits*10+5 FROM orders WHERE orderunits = 5 ;".toUpperCase());
    }
}
