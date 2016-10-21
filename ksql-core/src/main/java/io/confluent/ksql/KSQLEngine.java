package io.confluent.ksql;


import io.confluent.ksql.ddl.DDLEngine;
import io.confluent.ksql.metastore.*;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.tree.*;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.errors.StreamsException;

import java.io.IOException;
import java.util.List;

public class KSQLEngine {

    QueryEngine queryEngine = new QueryEngine();
    DDLEngine ddlEngine = new DDLEngine(this);


    MetaStore metaStore = null;

    public void initMetaStore() {

        metaStore = new MetaStoreImpl();

        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .field("ordertime", SchemaBuilder.INT64_SCHEMA)
                .field("orderid", SchemaBuilder.STRING_SCHEMA)
                .field("itemid", SchemaBuilder.STRING_SCHEMA)
                .field("orderunits", SchemaBuilder.FLOAT64_SCHEMA);


        KafkaTopic kafkaTopic = new KafkaTopic("orders", schemaBuilder, DataSource.DataSourceType.STREAM, "StreamExample1-GenericRow-order");

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

    public void processStatements(String statementsString) throws Exception {
        if (!statementsString.endsWith(";")) {
            statementsString = statementsString + ";";
        }
        List<Statement> statements = getStatements(statementsString);
        for(Statement statement: statements) {
            if(statement instanceof Query) {
                queryEngine.processQuery((Query)statement, metaStore);
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

    public KSQLEngine(String schemaFilePath) throws IOException {
//        initMetaStore();
        this.metaStore = new MetastoreUtil().loadMetastoreFromJSONFile(schemaFilePath);
    }


    public static void main(String[] args) throws Exception {
        KSQLEngine ksqlEngine = new KSQLEngine("/Users/hojjat/userschema.json");
//        ksqlEngine.processStatements("CREATE TOPIC orders ( orderkey bigint, orderstatus varchar, totalprice double, orderdate date)".toUpperCase());
//        ksqlEngine.processStatements("SELECT ordertime AS timeValue, orderid, orderunits*10+5 FROM orders WHERE orderunits > 5 ;".toUpperCase());
        ksqlEngine.processStatements("select ordertime, itemId, orderunits into stream1 from orders where orderunits > 5;".toUpperCase());
    }
}
