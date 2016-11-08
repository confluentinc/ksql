package io.confluent.ksql;


import io.confluent.ksql.ddl.DDLEngine;
import io.confluent.ksql.metastore.*;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.plan.OutputKafkaTopicNode;
import io.confluent.ksql.util.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

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

    public void runMultipleQueries(String queriesString) throws Exception {
        KSQLParser ksqlParser = new KSQLParser();
        List<SqlBaseParser.SingleStatementContext> parsedStatements =  ksqlParser.getStatements(queriesString);
        int queryIndex = 0;
        for (SqlBaseParser.SingleStatementContext singleStatementContext: parsedStatements) {
            Pair<Statement, DataSourceExtractor> statementInfo = ksqlParser.prepareStatement(singleStatementContext, metaStore);
            Statement statement = statementInfo.getLeft();
            if (statement instanceof Query) {
                Query query = (Query) statement;
                if (query.getQueryBody() instanceof  QuerySpecification) {
                    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
                    if (querySpecification.getInto().get() instanceof Table) {
                        Table table = (Table)querySpecification.getInto().get();
                        if(metaStore.getSource(table.getName().getSuffix().toUpperCase()) != null) {
                            throw  new KSQLException("Sink specified in INTO clause already exists: "+table.getName().getSuffix().toUpperCase());
                        }
                    }
                }
                Pair<KafkaStreams, OutputKafkaTopicNode> queryPairInfo = queryEngine.processQuery("ksql_"+queryIndex, query, metaStore, statementInfo.getRight());

                KafkaTopic kafkaTopic = new KafkaTopic(queryPairInfo.getRight().getId().toString(), queryPairInfo.getRight().getSchema(), queryPairInfo.getRight().getKeyField(), DataSource.DataSourceType.KSTREAM, queryPairInfo.getRight().getKafkaTopicName());
                metaStore.putSource(kafkaTopic);
                Thread.sleep(5000);

            } else if (statement instanceof CreateTable) {
                ddlEngine.createTopic((CreateTable) statement);
            } else if (statement instanceof DropTable) {
                ddlEngine.dropTopic((DropTable) statement);
            }
        }

    }

    public List<Pair<Statement, DataSourceExtractor>> getStatements(String sqlString) {
        // First parse the query and build the AST
        KSQLParser ksqlParser = new KSQLParser();
        List<Pair<Statement, DataSourceExtractor>> builtASTStatements = ksqlParser.buildAST(sqlString, metaStore);
        return builtASTStatements;
//        Node root = builtAST.getLeft();
//        List<Pair<Statement, DataSourceExtractor>> statementPairList = new ArrayList<>();
//        for (Pair<Node, DataSourceExtractor> statementPair: builtASTStatements) {
//            if(statementPair.getLeft() instanceof Statements) {
//                Statements statements = (Statements) statementPair.getLeft();
////                return new Pair<>(statements.statementList, builtAST.getRight());
//                statementPairList.add(new Pair<>(s))
//            }
//        }
//        return statementPairList;
//        if(root instanceof Statements) {
//            Statements statements = (Statements) root;
//            return new Pair<>(statements.statementList, builtAST.getRight());
//        }
//        throw new StreamsException("Error in parsing. Cannot get the set of statements.");
    }



    public void processStatements(String queryId, String statementsString) throws Exception {
        if (!statementsString.endsWith(";")) {
            statementsString = statementsString + ";";
        }
        List<Pair<Statement, DataSourceExtractor>> statementsInfo = getStatements(statementsString);
//        List<Statement> statements = statementsInfo.getLeft();
        int internalIndex = 0;
        for(Pair<Statement, DataSourceExtractor> statementInfo: statementsInfo) {
            if(statementInfo.getLeft() instanceof Query) {
                queryEngine.processQuery(queryId+"_"+internalIndex, (Query)statementInfo.getLeft(), metaStore, statementInfo.getRight());
                internalIndex++;
            }  else if (statementInfo.getLeft() instanceof CreateTable) {
                ddlEngine.createTopic((CreateTable) statementInfo.getLeft());
                return;
            } else if (statementInfo.getLeft() instanceof DropTable) {
                ddlEngine.dropTopic((DropTable) statementInfo.getLeft());
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
        ksqlProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "KSQL-Default-"+System.currentTimeMillis());
        ksqlProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KSQLConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
        ksqlProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KSQLConfig.DEFAULT_AUTO_OFFSET_RESET_CONFIG);
        if (! ksqlConfProperties.get(KSQLConfig.PROP_FILE_PATH_CONFIG).equalsIgnoreCase(KSQLConfig.DEFAULT_PROP_FILE_PATH_CONFIG)) {
            ksqlProperties.load(new FileReader(ksqlConfProperties.get(KSQLConfig.PROP_FILE_PATH_CONFIG)));
        }


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
//        ksqlEngine.processStatements("KSQL_1", "select USERID, REGIONID, userid = '*****', 12 into stream11 from users;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select * into ktable1 from users;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select * from orders;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select pageview.USERID, users.USERID, PAGEID, REGIONID, VIEWTIME into stream6 FROM pageview LEFT JOIN users ON pageview.USERID = users.USERID;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select ORDERTIME, ITEMID, ORDERUNITS into stream6 from orders where ORDERUNITS > 8 AND ITEMID = 'Item_3';".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select users.userid, pageid, regionid, gender into stream3 from pageview left join users on pageview.userid = users.userid;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "SELECT PAGEVIEW.USERID, PAGEID, REGIONID, GENDER INTO PAGEVIEWJOIN1 FROM PAGEVIEW LEFT JOIN USERS ON PAGEVIEW.USERID = USERS.USERID;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select USERTIME, USERID, REGIONID into stream5 from users;".toUpperCase());
//        ksqlEngine.processStatements("KSQL_1", "select ordertime, itemId, orderunits, '**===*' AS t into stream3 from orders;".toUpperCase());
        ksqlEngine.processStatements("KSQL_1", "SELECT users.userid AS userid, pageid, regionid, gender INTO enrichedpageview FROM pageview LEFT JOIN users ON pageview.userid = users.userid;".toUpperCase());
        ksqlEngine.processStatements("KSQL_1", "SELECT userid, pageid, regionid, gender INTO region_pageview FROM enrichedpageview WHERE regionid IS NOT NULL AND regionid = 'Region_5';".toUpperCase());

    }
}
