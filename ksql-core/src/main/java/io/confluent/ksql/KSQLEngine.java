package io.confluent.ksql;


import io.confluent.ksql.ddl.DDLEngine;
import io.confluent.ksql.metastore.*;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KSQLConfig;
import io.confluent.ksql.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KSQLEngine {

    KSQLConfig ksqlConfig;

    QueryEngine queryEngine;
    DDLEngine ddlEngine = new DDLEngine(this);


    MetaStore metaStore = null;

    public void initMetaStore() {

        metaStore = new MetaStoreImpl();

        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .field("ordertime", SchemaBuilder.INT64_SCHEMA)
                .field("orderid", SchemaBuilder.STRING_SCHEMA)
                .field("itemid", SchemaBuilder.STRING_SCHEMA)
                .field("orderunits", SchemaBuilder.FLOAT64_SCHEMA);

        KafkaTopic kafkaTopic = new KafkaTopic("orders", schemaBuilder, schemaBuilder.field("ordertime"), DataSource.DataSourceType.KSTREAM, "StreamExample1-GenericRow-order");

        metaStore.putSource(kafkaTopic);
    }

    public Pair<List<Statement>, DataSourceExtractor> getStatements(String sqlString) {
        // First parse the query and build the AST
        KSQLParser ksqlParser = new KSQLParser();
        Pair<Node, DataSourceExtractor> builtAST = ksqlParser.buildAST(sqlString, metaStore);
        Node root = builtAST.getLeft();
        if(root instanceof Statements) {
            Statements statements = (Statements) root;
            return new Pair<>(statements.statementList, builtAST.getRight());
        }
        throw new StreamsException("Error in parsing. Cannot get the set of statements.");
    }

    public void processStatements(String queryId, String statementsString) throws Exception {
        if (!statementsString.endsWith(";")) {
            statementsString = statementsString + ";";
        }
        Pair<List<Statement>, DataSourceExtractor> statementsInfo = getStatements(statementsString);
        List<Statement> statements = statementsInfo.getLeft();
        int internalIndex = 0;
        for(Statement statement: statements) {
            if(statement instanceof Query) {
                queryEngine.processQuery(queryId+"_"+internalIndex, (Query)statement, metaStore, statementsInfo.getRight());
                internalIndex++;
            }  else if (statement instanceof CreateTable) {
                ddlEngine.createTopic((CreateTable) statement);
                return;
            } else if (statement instanceof DropTable) {
                ddlEngine.dropTopic((DropTable) statement);
                return;
            }
        }
    }

    public MetaStore getMetaStore() {
        return metaStore;
    }

    public QueryEngine getQueryEngine() {
        return queryEngine;
    }

    public DDLEngine getDdlEngine() {
        return ddlEngine;
    }

    public KSQLConfig getKsqlConfig() {
        return ksqlConfig;
    }

    public KSQLEngine(String schemaFilePath) throws IOException {
        this.metaStore = new MetastoreUtil().loadMetastoreFromJSONFile(schemaFilePath);
    }

    public KSQLEngine(Map<String, String> ksqlConfProperties) throws IOException {
        String schemaPath = ksqlConfProperties.get(KSQLConfig.SCHEMA_FILE_PATH_CONFIG);
        Properties ksqlProperties = new Properties();
        ksqlProperties.load(new FileReader(ksqlConfProperties.get(KSQLConfig.PROP_FILE_PATH_CONFIG)));
        ksqlProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "KSQL_APP");
        this.ksqlConfig = new KSQLConfig(ksqlProperties);
        this.metaStore = new MetastoreUtil().loadMetastoreFromJSONFile(schemaPath);
        this.queryEngine = new QueryEngine(this.ksqlConfig);
    }

    public static void main(String[] args) throws Exception {
        Map<String,String> ksqlConfProperties = new HashMap<>();
        ksqlConfProperties.put(KSQLConfig.SCHEMA_FILE_PATH_CONFIG, "/Users/hojjat/userschema.json");
        ksqlConfProperties.put(KSQLConfig.PROP_FILE_PATH_CONFIG,"/Users/hojjat/ksql_config.conf");
        KSQLEngine ksqlEngine = new KSQLEngine(ksqlConfProperties);

//        ksqlEngine.processStatements("CREATE TOPIC orders ( orderkey bigint, orderstatus varchar, totalprice double, orderdate date)".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1","SELECT ordertime AS timeValue, orderid, orderunits*10+5 into stream5 FROM orders WHERE orderunits > 5 ;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select o.ordertime+1, o.itemId, orderunits into stream1 from orders o where o.orderunits > 5;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select * into stream1 from orders JOIN shipment ON orderid = shipmentorderid where orderunits > 5;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select u.userid, p.pageid , p.viewtime, regionid into stream3 from  pageview p LEFT JOIN users u ON u.userid = p.userid;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select u.userid, p.userid, p.pageid , p.viewtime, regionid into stream1 from  pageview p JOIN users u ON u.userid = p.userid;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select pageview.USERID, users.USERID, PAGEID, REGIONID, VIEWTIME into stream2 FROM pageview JOIN users ON pageview.USERID = users.USERID;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select USERID, REGIONID into stream8 from users where REGIONID = 'Region_5';".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select * from users where USERID = 'User_58';".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select * from orders;".toUpperCase());
        ksqlEngine.processStatements("KSQL_1", "select pageview.USERID, users.USERID, PAGEID, REGIONID, VIEWTIME into stream6 FROM pageview LEFT JOIN users ON pageview.USERID = users.USERID;".toUpperCase());

    }
}
