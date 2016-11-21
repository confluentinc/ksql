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
import java.util.Set;


public class KSQL {

    KSQLEngine ksqlEngine;
//    Map<String, Triplet<String,KafkaStreams, OutputKafkaTopicNode>> liveQueries = new HashMap<>();
    LiveQueryMap liveQueries = new LiveQueryMap();
    static final String queryIdPrefix = "ksql_";
    int queryIdCounter = 0;

    boolean isCLI = true;

    Map<String, String> cliProperties = null;

    ConsoleReader console = null;

    public void startConsole() throws Exception {
        try {
            console = new ConsoleReader();
            console.setPrompt("ksql> ");
            console.println("=========================================================================");
            console.println("KSQL 0.0.1");
            console.println("=========================================================================");
            console.addCompleter(new AnsiStringsCompleter("select", "show topics", "show queries", "from", "where", "terminate", "exit", "describe", "print"));
            String line = null;
            while ((line = console.readLine()) != null) {
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
                // Parse the command and create AST.
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

    public void runQueries(String queryFilePath) throws Exception {

        System.out.println("********************************************************************************************************");
        System.out.println("Starting the KSQL stream processing app with query fila path : "+queryFilePath);
        System.out.println("********************************************************************************************************");
        String queryString = KSQLUtil.readQueryFile(queryFilePath);
        List<Triplet<String,KafkaStreams, OutputKafkaTopicNode>> runningQueries = ksqlEngine.runMultipleQueries(queryString);

    }


    private void processStatement(Statement statement, String statementStr) throws IOException {
        try {
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
                showColumns(showColumns.getTable().getSuffix().toUpperCase());
                return;
            } else if (statement instanceof ShowTables) {
                showTables();
                return;
            } else if (statement instanceof TerminateQuery) {
                terminateQuery((TerminateQuery) statement);
                return;
            } else if (statement instanceof PrintTopic) {
                PrintTopic printTopic = (PrintTopic) statement;
                DataSource dataSource = ksqlEngine.getMetaStore().getSource(printTopic.getTopic().getSuffix().toUpperCase());
                if (dataSource == null) {
                    console.println("Topic does not exist: "+ printTopic.getTopic().getSuffix());
                    return;
                }
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

        } catch (Exception e) {
            console.println("Exception: "+e.getMessage());
        }

    }

    private List<Statement> parseStatements(String statementString) {
        List<Statement> statements = ksqlEngine.getStatements(statementString);
        return statements;
    }


    /**
     * Given a ksql command string, parse the command and return the parse tree.
     * @param statementString
     * @return
     * @throws IOException
     */
    public Statement getSingleStatement(String statementString) throws IOException {
        if (!statementString.endsWith(";")) {
            statementString = statementString + ";";
        }
        List<Statement> statements = null;
        try {
            statements = ksqlEngine.getStatements(statementString);
        } catch (Exception ex) {
            // Do nothing
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

    private  void startQuery_old(String queryString, Query query) throws Exception {
        if (query.getQueryBody() instanceof  QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
            if (querySpecification.getInto().get() instanceof Table) {
                Table table = (Table)querySpecification.getInto().get();
                if(ksqlEngine.metaStore.getSource(table.getName().getSuffix().toUpperCase()) != null) {
                    console.println("Sink specified in INTO clause already exists: "+table.getName().getSuffix().toUpperCase());
                    return;
                }
            }
        }
        String queryId = getNextQueryId();
        Pair<KafkaStreams, OutputKafkaTopicNode> queryPairInfo = ksqlEngine.getQueryEngine().processQuery(queryId, query, ksqlEngine.metaStore);

        Triplet<String, KafkaStreams, OutputKafkaTopicNode> queryInfo = new Triplet<>(queryString, queryPairInfo.getLeft(), queryPairInfo.getRight());
        liveQueries.put(queryId.toUpperCase() , queryInfo);
        KafkaTopic kafkaTopic = new KafkaTopic(queryInfo.getThird().getId().toString(), queryInfo.getThird().getSchema(), queryInfo.getThird().getKeyField(), DataSource.DataSourceType.KSTREAM, queryInfo.getThird().getKafkaTopicName());
        ksqlEngine.getMetaStore().putSource(kafkaTopic);
        if (isCLI) {
            console.println("Added the result topic to the metastore:");
            console.println("Topic count: "+ksqlEngine.getMetaStore().getAllDataSources().size());
            console.println("");
        }
    }

    private  void startQuery(String queryString, Query query) throws Exception {
        if (query.getQueryBody() instanceof  QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
            if (querySpecification.getInto().get() instanceof Table) {
                Table table = (Table)querySpecification.getInto().get();
                if(ksqlEngine.metaStore.getSource(table.getName().getSuffix().toUpperCase()) != null) {
                    console.println("Sink specified in INTO clause already exists: "+table.getName().getSuffix().toUpperCase());
                    return;
                }
            }
        }

        String queryId = getNextQueryId();

        Triplet<String,KafkaStreams, OutputKafkaTopicNode> queryPairInfo = ksqlEngine.runSingleQuery(new Pair<>(queryId, query));

        Triplet<String, KafkaStreams, OutputKafkaTopicNode> queryInfo = new Triplet<>(queryString, queryPairInfo.getSecond(), queryPairInfo.getThird());
        liveQueries.put(queryId.toUpperCase() , queryInfo);

        if (isCLI) {
            console.println("Added the result topic to the metastore:");
            console.println("Topic count: "+ksqlEngine.getMetaStore().getAllDataSources().size());
            console.println("");
        }
    }

    private void terminateQuery(TerminateQuery terminateQuery) throws IOException {
        String queryId = terminateQuery.getQueryId().toString();

        if(!liveQueries.containsKey(queryId.toUpperCase())) {
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
        console.println("        KSQL Topic          |       Corresponding Kafka Topic        |             Topic Key          |          Topic Type                 ");
        console.println("----------------------------+----------------------------------------+--------------------------------+-------------------------------------");
        for(String datasourceName: allDataSources.keySet()) {
            DataSource dataSource = allDataSources.get(datasourceName);
            if(dataSource instanceof KafkaTopic) {
                KafkaTopic kafkaTopic = (KafkaTopic) dataSource;
                console.println(" "+padRight(datasourceName, 27)+"|  "+padRight(kafkaTopic.getTopicName(),38)+"|  "+padRight(kafkaTopic.getKeyField().name().toString(),30)+"|          "+padRight(kafkaTopic.getDataSourceType().toString(),30));
            }

        }
        console.println("----------------------------+----------------------------------------+--------------------------------+-------------------------------------");
        console.println("( "+allDataSources.size()+" rows)");
        console.flush();
    }

    private void showColumns(String name) throws IOException {
        DataSource dataSource = ksqlEngine.getMetaStore().getSource(name.toUpperCase());
        if(dataSource == null) {
            console.println("Could not find topic "+name+" in the metastore!");
            console.flush();
            return;
        }
        console.println("TOPIC: "+name.toUpperCase()+"    Key: "+dataSource.getKeyField().name()+"    Type: "+dataSource.getDataSourceType());
        console.println("");
        console.println("      Column       |         Type         |                   Comment                   ");
        console.println("-------------------+----------------------+---------------------------------------------");
        for(Field schemaField: dataSource.getSchema().fields()) {
            console.println(padRight(schemaField.name(), 19)+"|  "+padRight(SchemaUtil.typeMap.get(schemaField.schema().type().getName().toUpperCase()).toUpperCase() +" ("+schemaField.schema().type().getName()+")", 18)+"  |");
        }
        console.println("-------------------+----------------------+---------------------------------------------");
        console.println("( "+dataSource.getSchema().fields().size()+" rows)");
        console.flush();
    }

    private void showQueries() throws IOException {
        console.println("Running queries: ");
        console.println(" Query ID   |         Query                                                                    |         Query Sink Topic");
        for(String queryId: liveQueries.keySet()) {
            console.println("------------+----------------------------------------------------------------------------------+-----------------------------------");
            Triplet<String, KafkaStreams, OutputKafkaTopicNode> queryInfo = liveQueries.get(queryId);
            console.println(padRight(queryId, 12)+"|   "+padRight(queryInfo.getFirst(), 80)+"|   "+queryInfo.getThird().getKafkaTopicName().toUpperCase());
        }
        console.println("------------+----------------------------------------------------------------------------------+-----------------------------------");
        console.println("( "+liveQueries.size()+" rows)");
        console.flush();
    }

    private void printTopic(String topicName) throws IOException {
        new TopicPrinter().printGenericRowTopic(topicName, console, this.cliProperties);
    }

    private String getNextQueryId() {
        String queryId = queryIdPrefix+queryIdCounter;
        queryIdCounter++;
        return queryId.toUpperCase();
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
        System.err.println(" ksql ["+ KSQLConfig.QUERY_FILE_PATH_CONFIG+"=<cli|path to query file> ] ["+ KSQLConfig.SCHEMA_FILE_PATH_CONFIG+"=<path to schema json file>] ["+KSQLConfig.PROP_FILE_PATH_CONFIG+"=<path to the properties file>]");
    }

    private static void loadDefaultSettings(Map<String, String> cliProperties) {
        cliProperties.put(KSQLConfig.QUERY_FILE_PATH_CONFIG, KSQLConfig.DEFAULT_QUERY_FILE_PATH_CONFIG);
        cliProperties.put(KSQLConfig.SCHEMA_FILE_PATH_CONFIG, KSQLConfig.DEFAULT_SCHEMA_FILE_PATH_CONFIG);
        cliProperties.put(KSQLConfig.PROP_FILE_PATH_CONFIG, KSQLConfig.DEFAULT_PROP_FILE_PATH_CONFIG);

    }

    public static void main(String[] args)
            throws Exception
    {
        Map<String, String> cliProperties = new HashMap<>();
        loadDefaultSettings(cliProperties);
        // For now only pne parameter for CLI
        if(args.length > 3) {
            printUsageFromatMessage();
            System.exit(0);
        }

        for (String propertyStr:args) {
            if(!propertyStr.contains("=")) {
                printUsageFromatMessage();
                System.exit(0);
            }
            String[] property = propertyStr.split("=");
            if(property[0].equalsIgnoreCase(KSQLConfig.QUERY_FILE_PATH_CONFIG) || property[0].equalsIgnoreCase(KSQLConfig.PROP_FILE_PATH_CONFIG) || property[0].equalsIgnoreCase(KSQLConfig.SCHEMA_FILE_PATH_CONFIG)) {
                cliProperties.put(property[0], property[1]);
            } else {
                printUsageFromatMessage();
            }
        }


        KSQL ksql = new KSQL(cliProperties);
        // Start the ksql cli ?
        if(cliProperties.get(KSQLConfig.QUERY_FILE_PATH_CONFIG).equalsIgnoreCase("cli")) {
            ksql.startConsole();
        } else { // Use the query file to run the queries.
            ksql.isCLI = false;
            ksql.runQueries(cliProperties.get(KSQLConfig.QUERY_FILE_PATH_CONFIG));

        }

    }

    class LiveQueryMap {
        Map<String, Triplet<String,KafkaStreams, OutputKafkaTopicNode>> liveQueries = new HashMap<>();

        public Triplet<String,KafkaStreams, OutputKafkaTopicNode> get(String key) {
            return liveQueries.get(key.toUpperCase());
        }

        public void put(String key, Triplet<String,KafkaStreams, OutputKafkaTopicNode> value) {
            liveQueries.put(key.toUpperCase(), value);
        }

        public Set<String> keySet() {
            return liveQueries.keySet();
        }

        public void remove(String key) {
            liveQueries.remove(key);
        }

        public int size() {
            return liveQueries.size();
        }

        public boolean containsKey(String key) {
            return liveQueries.containsKey(key.toUpperCase());
        }
    }
}
