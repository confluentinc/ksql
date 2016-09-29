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
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.Schema;
import io.confluent.ksql.planner.SchemaField;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.types.IntegerType;
import io.confluent.ksql.planner.types.LongType;
import io.confluent.ksql.planner.types.StringType;
import io.confluent.ksql.structured.SchemaStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
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

    public void processQuery(String sqlQuery) {
        KSQLParser ksqlParser = new KSQLParser();
        Node root = ksqlParser.buildAST(sqlQuery);
        Analysis analysis = new Analysis();
        Analyzer analyzer = new Analyzer(analysis,metaStore);
        analyzer.process(root, new AnalysisContext(null, null));

        PlanNode logicalPlan = new LogicalPlanner(analysis).buildPlan();


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamExample1-GenericRow-Processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();


        PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
        SchemaStream schemaStream = physicalPlanBuilder.buildPhysicalPlan(logicalPlan);

//        schemaStream.getkStream().


        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        try {
            Thread.sleep(1000000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        streams.close();
        System.out.print("");
    }

    public QueryEngine() {
        initMetaStore();
    }


    public static void main(String[] args) {

        QueryEngine queryEngine = new QueryEngine();
//        queryEngine.processQuery("SELECT timeField, idField, itemIdField, unitsField FROM ordersStream WHERE idField = 100".toLowerCase());
//        queryEngine.processQuery("SELECT t1, unitsField INTO test FROM ordersStream WHERE idField = 100;".toUpperCase());
        queryEngine.processQuery("SELECT ordertime, orderid, orderunits FROM orders WHERE orderunits > 5 ;".toUpperCase());

    }
}
