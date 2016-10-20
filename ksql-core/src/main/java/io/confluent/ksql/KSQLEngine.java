package io.confluent.ksql;


import io.confluent.ksql.ddl.DDLEngine;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.KSQLSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.errors.StreamsException;

import java.util.ArrayList;
import java.util.List;

public class KSQLEngine {

    QueryEngine queryEngine = new QueryEngine();
    DDLEngine ddlEngine = new DDLEngine(this);


    MetaStore metaStore = null;

    public void initMetaStore() {

        metaStore = new MetaStoreImpl();

        List<Field> fields = new ArrayList<>();

        Field orderTimeField = new Field("ordertime", 0, SchemaBuilder.INT64_SCHEMA);
        fields.add(orderTimeField);

        Field orderIdField = new Field("orderid", 1, SchemaBuilder.STRING_SCHEMA);
        fields.add(orderIdField);

        Field orderItemIdField = new Field("itemid", 2, SchemaBuilder.STRING_SCHEMA);
        fields.add(orderItemIdField);

        Field orderUnitsField = new Field("orderunits", 3, SchemaBuilder.FLOAT64_SCHEMA);
        fields.add(orderUnitsField);

        KSQLSchema orderKSQLSchema = new KSQLSchema(org.apache.kafka.connect.data.Schema.Type.STRUCT, false, null, "orders", 0, "", null, fields, null, null);

        KafkaTopic kafkaTopic = new KafkaTopic("orders", orderKSQLSchema, DataSource.DataSourceType.STREAM, "StreamExample1-GenericRow-order");

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

    public KSQLEngine() {
        initMetaStore();
    }


    public static void main(String[] args) throws Exception {
        KSQLEngine ksqlEngine = new KSQLEngine();
//        ksqlEngine.processStatements("CREATE TOPIC orders ( orderkey bigint, orderstatus varchar, totalprice double, orderdate date)".toUpperCase());
//        ksqlEngine.processStatements("SELECT ordertime AS timeValue, orderid, orderunits*10+5 FROM orders WHERE orderunits > 5 ;".toUpperCase());
        ksqlEngine.processStatements("select ordertime, itemId, orderunits into stream1 from orders where orderunits > 5;".toUpperCase());
    }
}
