package io.confluent.ksql;


import io.confluent.ksql.ddl.DDLEngine;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.Schema;
import io.confluent.ksql.planner.SchemaField;
import io.confluent.ksql.planner.types.IntegerType;
import io.confluent.ksql.planner.types.LongType;
import io.confluent.ksql.planner.types.StringType;
import org.apache.kafka.streams.errors.StreamsException;

import java.util.ArrayList;
import java.util.List;

public class KSQLEngine {

    QueryEngine queryEngine = new QueryEngine();
    DDLEngine ddlEngine = new DDLEngine(this);


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

    public void processStatements(String statementsString) throws Exception {
        if (!statementsString.endsWith(";")) {
            statementsString = statementsString + ";";
        }
        List<Statement> statements = getStatements(statementsString);
        for(Statement statement: statements) {
            if(statement instanceof Query) {
                queryEngine.processQuery((Query)statement);
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
        ksqlEngine.processStatements("CREATE TOPIC orders ( orderkey bigint, orderstatus varchar, totalprice double, orderdate date)".toUpperCase());

    }
}
