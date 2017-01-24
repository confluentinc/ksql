package io.confluent.kql;

import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.MetaStoreImpl;
import io.confluent.kql.metastore.MetastoreUtil;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.tree.*;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.planner.plan.KQLConsoleOutputNode;
import io.confluent.kql.planner.plan.OutputNode;
import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.util.*;

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


public class KQL {

  KQLEngine kqlEngine;
  LiveQueryMap liveQueries = new LiveQueryMap();
  Triplet<String, KafkaStreams, KQLConsoleOutputNode> cliCurrentQuery = null;

  static final String queryIdPrefix = "kql_";
  int queryIdCounter = 0;

  boolean isCLI = true;

  Map<String, String> cliProperties = null;

  ConsoleReader console = null;

  public void startConsole() throws Exception {
    try {
      console = new ConsoleReader();
      console.setPrompt("kql> ");
      console.println("=========================================================================");
      console.println("KQL (Kafka Query Language) 0.0.1");
      console.println("=========================================================================");
      console.addCompleter(
          new AnsiStringsCompleter("select", "show queries",
                                   "terminate", "exit", "describe", "print", "list topics",
                                   "list streams", "create topic"));
      String line = null;
      while ((line = console.readLine()) != null) {
        if (line.length() == 0) {
          continue;
        }
        if (line.trim().toLowerCase().startsWith("exit")) {
          // Close all running queries first!
          for (String runningQueryId : liveQueries.keySet()) {
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
        } else if (line.trim().equalsIgnoreCase("close")) {
          if (cliCurrentQuery != null) {
            cliCurrentQuery.getSecond().close();
          }
          continue;
        }
        // Parse the command and create AST.
        Statement statement = getSingleStatement(line);
        if (statement != null) {
          processStatement(statement, line);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        TerminalFactory.get().restore();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void runQueries(String queryFilePath) throws Exception {

    System.out.println(
        "********************************************************************************************************");
    System.out
        .println("Starting the KQL stream processing app with query file path : " + queryFilePath);
    System.out.println(
        "********************************************************************************************************");
    String queryString = KQLUtil.readQueryFile(queryFilePath);
    List<Triplet<String, KafkaStreams, OutputNode>>
        runningQueries =
        kqlEngine.runMultipleQueries(queryString);

  }


  private void processStatement(Statement statement, String statementStr) throws IOException {
    try {
      if (statement instanceof Query) {
        startQuery(statementStr, (Query) statement);
        return;
      } else if (statement instanceof CreateTopic) {
        kqlEngine.getDdlEngine().createTopic((CreateTopic) statement);
        return;
      } else if (statement instanceof CreateStream) {
        kqlEngine.getDdlEngine().createStream((CreateStream) statement);
        return;
      } else if (statement instanceof CreateTable) {
        kqlEngine.getDdlEngine().createTable((CreateTable) statement);
        return;
      } else if (statement instanceof CreateStreamAsSelect) {
        CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect)statement;
        QuerySpecification querySpecification = (QuerySpecification)createStreamAsSelect.getQuery()
            .getQueryBody();
        Query query = kqlEngine.addInto(createStreamAsSelect.getQuery(), querySpecification,
                              createStreamAsSelect.getName().getSuffix(),createStreamAsSelect
                                  .getProperties());
        startQuery(statementStr, query);
        return;
      } else if (statement instanceof CreateTableAsSelect) {
        CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
        QuerySpecification querySpecification = (QuerySpecification)createTableAsSelect.getQuery()
            .getQueryBody();

        Query query = kqlEngine.addInto(createTableAsSelect.getQuery(), querySpecification,
                              createTableAsSelect.getName().getSuffix(),createTableAsSelect
                                  .getProperties());
        startQuery(statementStr, query);
        return;
      } else if (statement instanceof DropTable) {
        kqlEngine.getDdlEngine().dropTopic((DropTable) statement);
        return;
      } else if (statement instanceof ExportCatalog) {
        ExportCatalog exportCatalog = (ExportCatalog)statement;
        exportCatalog(exportCatalog.getCatalogFilePath());
        return;
      } else if (statement instanceof ShowQueries) {
        showQueries();
        return;
      } else if (statement instanceof ListTopics) {
        listTopics();
        return;
      } else if (statement instanceof ListStreams) {
        listStreams();
        return;
      } else if (statement instanceof ShowColumns) {
        ShowColumns showColumns = (ShowColumns) statement;
        showColumns(showColumns.getTable().getSuffix().toUpperCase());
        return;
      } else if (statement instanceof TerminateQuery) {
        terminateQuery((TerminateQuery) statement);
        return;
      } else if (statement instanceof PrintTopic) {
        PrintTopic printTopic = (PrintTopic) statement;
        KQLTopic
            KQLTopic =
            kqlEngine.getMetaStore().getTopic(printTopic.getTopic().getSuffix().toUpperCase());
        if (KQLTopic == null) {
          console.println("Topic does not exist: " + printTopic.getTopic().getSuffix());
          return;
        }
        String topicsName = KQLTopic.getTopicName();
        long interval;
        if (printTopic.getIntervalValue() == null) {
          interval = -1;
        } else {
          interval = printTopic.getIntervalValue().getValue();
        }
        printTopic(KQLTopic, interval);
        return;
      } else if (statement instanceof SetProperty) {

      } else if (statement instanceof LoadProperties) {

      }
      console.println("Command/Statement is incorrect or not supported.");

    } catch (Exception e) {
      console.println("Exception: " + e.getMessage());
    }

  }

  /**
   * Given a kql command string, parse the command and return the parse tree.
   */
  public Statement getSingleStatement(String statementString) throws IOException {
    if (!statementString.endsWith(";")) {
      statementString = statementString + ";";
    }
    List<Statement> statements = null;
    try {
      statements = kqlEngine.getStatements(statementString);
    } catch (Exception ex) {
      // Do nothing
      ex.printStackTrace();
      System.err.println(ex.getMessage());
    }

    if (statements == null) {
      console.println();
      console.println("Oops! Something went wrong!!!...");
      console.println();

    } else if ((statements.size() != 1)) {
      console.println();
      console.println("KQL CLI Processes one statement/command at a time.");
      console.println();
    } else {
      return statements.get(0);
    }
    console.flush();
    return null;
  }

  private void printCommandList() throws IOException {
    console.println("KQL cli commands: ");
    console.println(
        "------------------------------------------------------------------------------------ ");
    console.println(
        "list topics           .................... Show the list of available topics.");
    console.println(
            "list streams           .................... Show the list of available streams/tables.");
    console.println(
        "describe <stream/table name> .................... Show the schema of the given stream/table.");
    console.println("show queries          .................... Show the list of running queries.");
    console.println(
        "print <topic name>    .................... Print the content of a given topic/stream.");
    console.println(
        "terminate <query id>  .................... Terminate the running query with the given id.");
    console.println();
    console.println("For more information refer to www.kql.confluent.io");
    console.println();
    console.flush();
  }

    /**
     * Given a KQL query, start the continious query with QueryEngine.
     *
     * @param queryString
     * @param query
     * @throws Exception
     */
  private void startQuery(String queryString, Query query) throws Exception {
    if (query.getQueryBody() instanceof QuerySpecification) {
      QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
      if (querySpecification.getInto().get() instanceof Table) {
        Table table = (Table) querySpecification.getInto().get();
        if (kqlEngine.metaStore.getSource(table.getName().getSuffix().toUpperCase()) != null) {
          console.println(
              "Sink specified in INTO clause already exists: " + table.getName().getSuffix()
                  .toUpperCase());
          return;
        }
      }
    }

    String queryId = getNextQueryId();

    Triplet<String, KafkaStreams, OutputNode>
        queryPairInfo =
        kqlEngine.runSingleQuery(new Pair<>(queryId, query));

    if (queryPairInfo.getThird() instanceof KQLStructuredDataOutputNode) {
      Triplet<String, KafkaStreams, KQLStructuredDataOutputNode>
          queryInfo =
          new Triplet<>(queryString, queryPairInfo.getSecond(), (KQLStructuredDataOutputNode)
              queryPairInfo.getThird());
      liveQueries.put(queryId.toUpperCase(), queryInfo);
    } else if (queryPairInfo.getThird() instanceof KQLConsoleOutputNode) {
        if (cliCurrentQuery != null) {
            console.println("Terminating the currently running console query first. ");
            cliCurrentQuery.getSecond().close();
        }
      cliCurrentQuery = new Triplet<>(queryString, queryPairInfo.getSecond(), (KQLConsoleOutputNode) queryPairInfo.getThird());
    }
    if (isCLI) {
      console.println("");
    }
  }

  private void terminateQuery(TerminateQuery terminateQuery) throws IOException {
    String queryId = terminateQuery.getQueryId().toString();
    terminateQuery(queryId);
  }

  private void terminateQuery(String queryId) throws IOException {
    if (!liveQueries.containsKey(queryId.toUpperCase())) {
      console.println("No running query with id = " + queryId + " was found!");
      console.flush();
      return;
    }
    Triplet<String, KafkaStreams, KQLStructuredDataOutputNode>
        kafkaStreamsQuery =
        liveQueries.get(queryId);
    console.println("Query: " + kafkaStreamsQuery.getFirst());
    kafkaStreamsQuery.getSecond().close();
    liveQueries.remove(queryId);
    console.println("Terminated query with id = " + queryId);
  }


  private void listTopics() throws IOException {
    MetaStore metaStore = kqlEngine.getMetaStore();
    Map<String, KQLTopic> topicMap = metaStore.getAllKafkaTopics();
    if (topicMap.isEmpty()) {
      console.println("No topic has been defined yet.");
      return;
    }
    console.println(
        "                 KQL Topic                 |        Corresponding Kafka Topic           "
        + "|  "
        + "   "
        + "     Topic Format              |          Notes    "
        + "      ");
    console.println(
        "-------------------------------------------+--------------------------------------------+------------------------------------+--------------------");
    for (String topicName : topicMap.keySet()) {
      KQLTopic kqlTopic = topicMap.get(topicName);
      String formatStr = kqlTopic.getKqlTopicSerDe().getSerDe().toString();
      String notes = "";
      if (kqlTopic.getKqlTopicSerDe() instanceof KQLAvroTopicSerDe) {
        KQLAvroTopicSerDe kqlAvroTopicSerDe = (KQLAvroTopicSerDe) kqlTopic.getKqlTopicSerDe();
        notes = "Avro schema path: "+kqlAvroTopicSerDe.getSchemaFilePath();
      }
      console.println(
          " " + padRight(topicName, 42) + "|  " + padRight(kqlTopic.getKafkaTopicName(), 42) + "| "
          + padRight(kqlTopic.getKqlTopicSerDe().getSerDe().toString(), 35)+"| "+notes);
    }
    console.println(
        "-------------------------------------------+--------------------------------------------+------------------------------------+--------------------");
    console.println("( " + topicMap.size() + " rows)");
    console.flush();
  }

  private void listStreams() throws IOException {
    MetaStore metaStore = kqlEngine.getMetaStore();
    Map<String, StructuredDataSource> allDataSources = metaStore.getAllStructuredDataSource();
    if (allDataSources.isEmpty()) {
      console.println("No streams/tables has been defined yet.");
      return;
    }
    console.println(
        "         Name               |               KQL Topic                |             Topic"
        + " Key          |     Topic Type     |          Topic Format           "
        + "      ");
    console.println(
        "----------------------------+----------------------------------------+--------------------------------+--------------------+-------------------------------------");
    for (String datasourceName : allDataSources.keySet()) {
      StructuredDataSource dataSource = allDataSources.get(datasourceName);
      if (dataSource instanceof KQLStream) {
        KQLStream kqlStream = (KQLStream) dataSource;
        console.println(
            " " + padRight(datasourceName, 27) + "|  " + padRight(kqlStream.getKQLTopic()
                                                                      .getName().toUpperCase(), 38)
            + "|  " + padRight(kqlStream.getKeyField().name().toString(), 30) + "|    "
            + padRight(kqlStream.getDataSourceType().toString(), 16)+ "|          "
            + padRight(kqlStream.getKQLTopic().getKqlTopicSerDe().getSerDe().toString(), 30));
      } else if (dataSource instanceof KQLTable) {
        KQLTable kqlTable = (KQLTable) dataSource;
        console.println(
            " " + padRight(datasourceName, 27) + "|  " + padRight(kqlTable.getKQLTopic().getName(), 38)
            + "|  " + padRight(kqlTable.getKeyField().name().toString(), 30) + "|    "
            + padRight(kqlTable.getDataSourceType().toString(), 16)+ "|          "
            + padRight(kqlTable.getKQLTopic().getKqlTopicSerDe().getSerDe().toString(), 30));
      }

    }
    console.println(
        "----------------------------+----------------------------------------+--------------------------------+--------------------+-------------------------------------");
    console.println("( " + allDataSources.size() + " rows)");
    console.flush();
  }

  private void showColumns(String name) throws IOException {
    StructuredDataSource dataSource = kqlEngine.getMetaStore().getSource(name.toUpperCase());
    if (dataSource == null) {
      console.println("Could not find topic " + name + " in the metastore!");
      console.flush();
      return;
    }
    console.println("TOPIC: " + name.toUpperCase() + "    Key: " + dataSource.getKeyField().name()
                    + "    Type: " + dataSource.getDataSourceType());
    console.println("");
    console.println(
        "      Column       |         Type         |                   Comment                   ");
    console.println(
        "-------------------+----------------------+---------------------------------------------");
    for (Field schemaField : dataSource.getSchema().fields()) {
      console.println(padRight(schemaField.name(), 19) + "|  " + padRight(
          SchemaUtil.typeMap.get(schemaField.schema().type().getName().toUpperCase()).toUpperCase()
          + " (" + schemaField.schema().type().getName() + ")", 18) + "  |");
    }
    console.println(
        "-------------------+----------------------+---------------------------------------------");
    console.println("( " + dataSource.getSchema().fields().size() + " rows)");
    console.flush();
  }

  private void showQueries() throws IOException {
    console.println("Running queries: ");
    console.println(
        " Query ID   |         Query                                                                    |         Query Sink Topic");
    for (String queryId : liveQueries.keySet()) {
      console.println(
          "------------+----------------------------------------------------------------------------------+-----------------------------------");
      Triplet<String, KafkaStreams, KQLStructuredDataOutputNode> queryInfo = liveQueries.get(queryId);
      console.println(
          padRight(queryId, 12) + "|   " + padRight(queryInfo.getFirst(), 80) + "|   " + queryInfo
              .getThird().getKafkaTopicName().toUpperCase());
    }
    console.println(
        "------------+----------------------------------------------------------------------------------+-----------------------------------");
    console.println("( " + liveQueries.size() + " rows)");
    console.flush();
  }

  private void printTopic(KQLTopic KQLTopic, long interval) throws IOException {
    new TopicPrinter().printGenericRowTopic(KQLTopic, console, interval, this.cliProperties);
  }

  private void exportCatalog(String filePath) {

    MetaStoreImpl metaStore = (MetaStoreImpl)kqlEngine.getMetaStore();
    MetastoreUtil metastoreUtil = new MetastoreUtil();

    metastoreUtil.writeMetastoreToFile(filePath, metaStore);
    try {
      console.println("Wrote the catalog into "+filePath);
    } catch (IOException e) {
    }
  }

  private String getNextQueryId() {
    String queryId = queryIdPrefix + queryIdCounter;
    queryIdCounter++;
    return queryId.toUpperCase();
  }

  public static String padRight(String s, int n) {
    return String.format("%1$-" + n + "s", s);
  }

  private KQL(Map<String, String> cliProperties) throws IOException {
    this.cliProperties = cliProperties;

    kqlEngine = new KQLEngine(cliProperties);
  }

  public static void printUsageFromatMessage() {

    System.err.println("Incorrect format: ");
    System.err.println("Usage: ");
    System.err.println(
        " kql [" + KQLConfig.QUERY_FILE_PATH_CONFIG + "=<cli|path to query file> ] ["
        + KQLConfig.CATALOG_FILE_PATH_CONFIG + "=<path to catalog json file>] ["
        + KQLConfig.PROP_FILE_PATH_CONFIG + "=<path to the properties file>]");
  }

  private static void loadDefaultSettings(Map<String, String> cliProperties) {
    cliProperties.put(KQLConfig.QUERY_FILE_PATH_CONFIG, KQLConfig.DEFAULT_QUERY_FILE_PATH_CONFIG);
    cliProperties
        .put(KQLConfig.CATALOG_FILE_PATH_CONFIG, KQLConfig.DEFAULT_SCHEMA_FILE_PATH_CONFIG);
    cliProperties.put(KQLConfig.PROP_FILE_PATH_CONFIG, KQLConfig.DEFAULT_PROP_FILE_PATH_CONFIG);
    cliProperties.put(KQLConfig.AVRO_SCHEMA_FOLDER_PATH_CONFIG, KQLConfig.DEFAULT_AVRO_SCHEMA_FOLDER_PATH_CONFIG);

  }

  public static void main(String[] args)
      throws Exception {
    Map<String, String> cliProperties = new HashMap<>();
    loadDefaultSettings(cliProperties);
    // For now only pne parameter for CLI
    if (args.length > 3) {
      printUsageFromatMessage();
      System.exit(0);
    }

    for (String propertyStr : args) {
      if (!propertyStr.contains("=")) {
        printUsageFromatMessage();
        System.exit(0);
      }
      String[] property = propertyStr.split("=");
      if (property[0].equalsIgnoreCase(KQLConfig.QUERY_FILE_PATH_CONFIG) || property[0]
          .equalsIgnoreCase(KQLConfig.PROP_FILE_PATH_CONFIG)
          || property[0].equalsIgnoreCase(KQLConfig.CATALOG_FILE_PATH_CONFIG) || property[0]
              .equalsIgnoreCase(KQLConfig.QUERY_CONTENT_CONFIG)|| property[0]
              .equalsIgnoreCase(KQLConfig.QUERY_EXECUTION_TIME_CONFIG)) {
        cliProperties.put(property[0], property[1]);
      } else {
        printUsageFromatMessage();
      }
    }

    KQL kql = new KQL(cliProperties);

    // Run one query received as commandline parameter.
    if (cliProperties.get(KQLConfig.QUERY_CONTENT_CONFIG) != null) {
      String queryString = cliProperties.get(KQLConfig.QUERY_CONTENT_CONFIG);
      String terminateInStr = cliProperties.get(KQLConfig.QUERY_EXECUTION_TIME_CONFIG);
      long terminateIn;
      if (terminateInStr == null) {
        terminateIn = -1;
      } else {
        terminateIn = Long.parseLong(terminateInStr);
      }
      kql.kqlEngine.runCLIQuery(queryString, terminateIn);
    } else {
      // Start the kql cli?
      if (cliProperties.get(KQLConfig.QUERY_FILE_PATH_CONFIG).equalsIgnoreCase(KQLConfig.DEFAULT_QUERY_FILE_PATH_CONFIG)) {
        kql.startConsole();
      } else { // Use the query file to run the queries.
        kql.isCLI = false;
        kql.runQueries(cliProperties.get(KQLConfig.QUERY_FILE_PATH_CONFIG));
      }
    }
  }

  class LiveQueryMap {

    Map<String, Triplet<String, KafkaStreams, KQLStructuredDataOutputNode>> liveQueries = new HashMap<>();

    public Triplet<String, KafkaStreams, KQLStructuredDataOutputNode> get(String key) {
      return liveQueries.get(key.toUpperCase());
    }

    public void put(String key, Triplet<String, KafkaStreams, KQLStructuredDataOutputNode> value) {
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
