package io.confluent.ksql;


import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.plan.OutputKafkaTopicNode;
import io.confluent.ksql.util.*;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.completer.AnsiStringsCompleter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.KafkaStreams;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class KSQL {

    KSQLEngine ksqlEngine;
    Map<String, Triplet<String,KafkaStreams, OutputKafkaTopicNode>> liveQueries = new HashMap<>();
    static final String queryIdPrefix = "ksql_";

    int queryIdCounter = 0;

    Map<String, String> cliProperties = null;

    ConsoleReader console = null;

    public void startConsole() throws Exception {
        try {
            console = new ConsoleReader();
            console.setPrompt("ksql> ");
            console.println("KSQL 0.0.1");
            console.addCompleter(new AnsiStringsCompleter("select", "show", "from", "where", "terminate", "exit", "describe", "topics", "queries", "print"));
            String line = null;
            while ((line = console.readLine().toUpperCase()) != null) {
                if (line.trim().toLowerCase().startsWith("exit")) {
                    // Close all running queries first!
                    for (String runningQueryId: liveQueries.keySet()) {
                        liveQueries.get(runningQueryId).getSecond().close();
                    }
                    console.println("");
                    console.println("Goodbye!");
                    console.println("");
                    console.flush();
                    console.close();
                    System.exit(0);
                } else if (line.trim().toLowerCase().startsWith("help")) {
                    printCommandList();
                    continue;
                }
                Statement statement = getSingleStatement(line);
                if(statement != null) {
                    processStatement(statement, line);
                }
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                TerminalFactory.get().restore();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void processStatement(Statement statement, String statementStr) throws Exception {
        if (statement instanceof Query) {
            startQuery(statementStr, (Query) statement);
            return;
        } else if (statement instanceof CreateTable) {
            ksqlEngine.getDdlEngine().createTopic((CreateTable) statement);
            return;
        } else if (statement instanceof DropTable) {
            ksqlEngine.getDdlEngine().dropTopic((DropTable) statement);
            return;
        } else if (statement instanceof ShowQueries) {
            showQueries();
            return;
        } else if (statement instanceof ShowTopics) {
            showTables();
            return;
        } else if (statement instanceof ShowColumns) {
            ShowColumns showColumns = (ShowColumns) statement;
            showColumns(showColumns.getTable().getSuffix());
            return;
        } else if (statement instanceof ShowTables) {
            showTables();
            return;
        } else if (statement instanceof TerminateQuery) {
            terminateQuery((TerminateQuery) statement);
            return;
        } else if (statement instanceof PrintTopic) {
            PrintTopic printTopic = (PrintTopic) statement;
            DataSource dataSource = ksqlEngine.getMetaStore().getSource(printTopic.getTopic().getSuffix().toLowerCase());
            if(dataSource instanceof KafkaTopic) {
                KafkaTopic kafkaTopic = (KafkaTopic) dataSource;
                String topicsName = kafkaTopic.getTopicName();
                printTopic(topicsName);
            }

            return;
        } else if (statement instanceof SetProperty) {

        } else if (statement instanceof LoadProperties) {

        }
        console.println("Command/Statement is incorrect or not supported.");
    }

    private List<Statement> parseStatements(String statementString) {
        if (!statementString.endsWith(";")) {
            statementString = statementString + ";";
        }
        List<Statement> statements = ksqlEngine.getStatements(statementString);
        return statements;
    }

    public Statement getSingleStatement(String statementString) throws IOException {
        if (!statementString.endsWith(";")) {
            statementString = statementString + ";";
        }
        List<Statement> statements = null;
        try {
            statements = parseStatements(statementString);
        } catch (Exception ex) {
        }

        if((statements == null) || (statements.size() != 1)) {
            console.println("KSQL CLI Processes one statement/command at a time.");
            console.flush();
            return null;
        } else {
            return statements.get(0);
        }
    }

    private void printCommandList() throws IOException {
        console.println("KSQL cli commands: ");
        console.println("------------------------------------------------------------------------------------ ");
        console.println("show topics           .................... Show the list of available topics/streams.");
        console.println("describe <topic name> .................... Show the schema of the given topic/stream.");
        console.println("show queries          .................... Show the list of running queries.");
        console.println("print <topic name>    .................... Print the content of a given topic/stream.");
        console.println("terminate <query id>  .................... Terminate the running query with the given id.");
        console.println();
        console.println("For more information refer to www.ksql.confluent.io");
        console.println();
        console.flush();
    }

    private  void startQuery(String queryString, Query query) throws Exception {
        if (query.getQueryBody() instanceof  QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
            if (querySpecification.getInto().get() instanceof Table) {
                Table table = (Table)querySpecification.getInto().get();
                if(ksqlEngine.metaStore.getSource(table.getName().getSuffix().toLowerCase()) != null) {
                    console.println("Sink specified in INTO clause already exists: "+table.getName().getSuffix().toLowerCase());
                    return;
                }
            }
        }
        String queryId = getNextQueryId();
        Pair<KafkaStreams, OutputKafkaTopicNode> queryPairInfo = ksqlEngine.getQueryEngine().processQuery(queryId, query, ksqlEngine.metaStore);

        Triplet<String, KafkaStreams, OutputKafkaTopicNode> queryInfo = new Triplet<>(queryString, queryPairInfo.getLeft(), queryPairInfo.getRight());
        liveQueries.put(queryId , queryInfo);
        KafkaTopic kafkaTopic = new KafkaTopic(queryInfo.getThird().getId().toString(), queryInfo.getThird().getSchema(), DataSource.DataSourceType.STREAM, queryInfo.getThird().getKafkaTopicName());
        ksqlEngine.getMetaStore().putSource(kafkaTopic);
        console.println("Added the result topic to the metastore:");
        console.println("Topic count: "+ksqlEngine.getMetaStore().getAllDataSources().size());
        console.println("");
    }

    private void terminateQuery(TerminateQuery terminateQuery) throws IOException {
        String queryId = terminateQuery.getQueryId().toString();

        if(!liveQueries.containsKey(queryId)) {
            console.println("No running query with id = "+queryId+" was found!");
            console.flush();
            return;
        }
        Triplet<String, KafkaStreams, OutputKafkaTopicNode> kafkaStreamsQuery = liveQueries.get(queryId);
        console.println("Query: "+kafkaStreamsQuery.getFirst());
        kafkaStreamsQuery.getSecond().close();
        liveQueries.remove(queryId);
        console.println("Terminated query with id = "+queryId);
    }

    private void showTables() throws IOException {
        MetaStore metaStore = ksqlEngine.getMetaStore();
        Map<String, DataSource> allDataSources = metaStore.getAllDataSources();
        if (allDataSources.isEmpty()) {
            console.println("No topic is available.");
            return;
        }
        console.println("        KSQL topic          |       Corresponding Kafka topic     ");
        console.println("----------------------------+-------------------------------------");
        for(String datasourceName: allDataSources.keySet()) {
            DataSource dataSource = allDataSources.get(datasourceName);
            if(dataSource instanceof KafkaTopic) {
                KafkaTopic kafkaTopic = (KafkaTopic) dataSource;
                console.println(" "+padRight(datasourceName, 27)+"|  "+padRight(kafkaTopic.getTopicName(),26));
            }

        }
        console.println("----------------------------+-------------------------------------");
        console.println("( "+allDataSources.size()+" rows)");
        console.flush();
    }

    private void showColumns(String name) throws IOException {
        DataSource dataSource = ksqlEngine.getMetaStore().getSource(name.toLowerCase());
        if(dataSource == null) {
            console.println("Could not find topic "+name+" in the metastore!");
            console.flush();
            return;
        }
        console.println("      Column       |         Type         |                   Comment                   ");
        console.println("-------------------+----------------------+---------------------------------------------");
        for(Field schemaField: dataSource.getSchema().fields()) {
            console.println(padRight(schemaField.name(), 19)+"|  "+padRight(SchemaUtil.typeMap.get(schemaField.schema().type().getName().toLowerCase()).toUpperCase() +" ("+schemaField.schema().type().getName()+")", 18)+"  |");
        }
        console.println("-------------------+----------------------+---------------------------------------------");
        console.println("( "+dataSource.getSchema().fields().size()+" rows)");
        console.flush();
    }

    private void showQueries() throws IOException {
        console.println("Running queries: ");
        console.println(" Query ID   |         Query                                                                    |         Query Sink Topic");
        for(String queryId: liveQueries.keySet()) {
            console.println("------------+----------------------------------------------------------------------------------+------------------------");
            Triplet<String, KafkaStreams, OutputKafkaTopicNode> queryInfo = liveQueries.get(queryId);
            console.println(padRight(queryId, 12)+"|   "+padRight(queryInfo.getFirst(), 80)+"|   "+queryInfo.getThird().getKafkaTopicName());
        }
        console.println("------------+----------------------------------------------------------------------------------+------------------------");
        console.println("( "+liveQueries.size()+" rows)");
        console.flush();

    }

    private void printTopic(String topicName) {
        new TopicPrinter().printGenericRowTopic(topicName, console);
    }

    private String getNextQueryId() {
        String queryId = queryIdPrefix+queryIdCounter;
        queryIdCounter++;
        return queryId;
    }

    public static String padRight(String s, int n) {
        return String.format("%1$-" + n + "s", s);
    }

    private KSQL(Map<String, String> cliProperties) throws IOException {
        this.cliProperties = cliProperties;

        ksqlEngine = new KSQLEngine(cliProperties);
    }

    public static void printUsageFromatMessage() {

        System.err.println("Incorrect format: ");
        System.err.println("Usage: ");
        System.err.println(" ksql "+ KSQLConfig.SCHEMA_FILE_PATH_CONFIG+"=<path to schema json file> "+KSQLConfig.PROP_FILE_PATH_CONFIG+"=<path to the properties file>");
    }

    public static void main(String[] args)
            throws Exception
    {
        Map<String, String> cliProperties = new HashMap<>();

        // For now only pne parameter for CLI
        if(args.length != 2) {
            printUsageFromatMessage();
            System.exit(0);
        }

        for (String propertyStr:args) {
            if(!propertyStr.contains("=")) {
                printUsageFromatMessage();
                System.exit(0);
            }
            String[] property = propertyStr.split("=");
            if(property[0].equalsIgnoreCase(KSQLConfig.PROP_FILE_PATH_CONFIG) || property[0].equalsIgnoreCase(KSQLConfig.SCHEMA_FILE_PATH_CONFIG)) {

                cliProperties.put(property[0], property[1]);
            } else {
                printUsageFromatMessage();
            }
        }

        new KSQL(cliProperties).startConsole();
    }

}
