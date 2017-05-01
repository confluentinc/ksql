/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql;

import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.MetaStoreImpl;
import io.confluent.kql.metastore.MetastoreUtil;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.tree.CreateStream;
import io.confluent.kql.parser.tree.CreateStreamAsSelect;
import io.confluent.kql.parser.tree.CreateTable;
import io.confluent.kql.parser.tree.CreateTableAsSelect;
import io.confluent.kql.parser.tree.CreateTopic;
import io.confluent.kql.parser.tree.DropTable;
import io.confluent.kql.parser.tree.ExportCatalog;
import io.confluent.kql.parser.tree.ListProperties;
import io.confluent.kql.parser.tree.ListStreams;
import io.confluent.kql.parser.tree.ListTables;
import io.confluent.kql.parser.tree.ListTopics;
import io.confluent.kql.parser.tree.LoadProperties;
import io.confluent.kql.parser.tree.PrintTopic;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.QuerySpecification;
import io.confluent.kql.parser.tree.SetProperty;
import io.confluent.kql.parser.tree.ShowColumns;
import io.confluent.kql.parser.tree.ListQueries;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.parser.tree.Table;
import io.confluent.kql.parser.tree.TerminateQuery;
import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.util.KQLConfig;
import io.confluent.kql.util.KQLUtil;
import io.confluent.kql.util.QueryMetadata;
import io.confluent.kql.util.QueuedQueryMetadata;
import io.confluent.kql.util.SchemaUtil;
import io.confluent.kql.util.TopicPrinter;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.completer.AnsiStringsCompleter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;


public class KQL {

  KQLEngine kqlEngine;
  KQLConfig config;
  String currentQueryId;
  Thread currentQueryThread;

  boolean isCLI = true;

  ConsoleReader console;

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
                                   "list streams", "create topic", "create stream", "create table"));
      console.setExpandEvents(false); // Disable event expansion so things like '!=' are left alone by the console reader
      for (String line = console.readLine(); line != null; line = console.readLine()) {
        if (line.trim().isEmpty()) {
          continue;
        }
        if (line.trim().toLowerCase().startsWith("exit")) {
          break;
        } else if (line.trim().toLowerCase().startsWith("help")) {
          printCommandList();
          continue;
        } else if (line.trim().equalsIgnoreCase("close")) {
          if (stopCurrentQuery()) {
            console.println("Query terminated");
          } else {
            console.println("There is no currently-running console query to terminate");
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
        stopCurrentQuery();
        kqlEngine.close();
        console.println();
        console.println("Goodbye!");
        console.println();
        console.flush();
        console.close();
        TerminalFactory.get().restore();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void runQueriesFromFile(String queryFilePath) throws Exception {
    System.out.println(
        "********************************************************************************************************");
    System.out
        .println("Starting the KQL stream processing app with query file path : " + queryFilePath);
    System.out.println(
        "********************************************************************************************************");
    String queryString = KQLUtil.readQueryFile(queryFilePath);
    kqlEngine.runMultipleQueries(false, queryString);
  }


  private void processStatement(final Statement statement, final String statementStr) throws
                                                                                 IOException {
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
        CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
        QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery()
            .getQueryBody();
        Query query = kqlEngine.addInto(createStreamAsSelect.getQuery(), querySpecification,
                              createStreamAsSelect.getName().getSuffix(), createStreamAsSelect
                                  .getProperties());
        startQuery(statementStr, query);
        return;
      } else if (statement instanceof CreateTableAsSelect) {
        CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
        QuerySpecification querySpecification = (QuerySpecification) createTableAsSelect.getQuery()
            .getQueryBody();

        Query query = kqlEngine.addInto(createTableAsSelect.getQuery(), querySpecification,
                              createTableAsSelect.getName().getSuffix(), createTableAsSelect
                                  .getProperties());
        startQuery(statementStr, query);
        return;
      } else if (statement instanceof DropTable) {
        kqlEngine.getDdlEngine().dropTopic((DropTable) statement);
        return;
      } else if (statement instanceof ExportCatalog) {
        ExportCatalog exportCatalog = (ExportCatalog) statement;
        exportCatalog(exportCatalog.getCatalogFilePath());
        return;
      } else if (statement instanceof SetProperty) {
        setProperty((SetProperty) statement);
        return;
      } else if (statement instanceof ListProperties) {
        listProperties();
        return;
      } else if (statement instanceof ListQueries) {
        listQueries();
        return;
      } else if (statement instanceof ListTopics) {
        listTopics();
        return;
      } else if (statement instanceof ListStreams) {
        listStreams();
        return;
      } else if (statement instanceof ListTables) {
        listTables();
        return;
      } else if (statement instanceof ShowColumns) {
        ShowColumns showColumns = (ShowColumns) statement;
        showColumns(showColumns.getTable().getSuffix());
        return;
      } else if (statement instanceof TerminateQuery) {
        terminateQuery((TerminateQuery) statement);
        return;
      } else if (statement instanceof PrintTopic) {
        PrintTopic printTopic = (PrintTopic) statement;
        KQLTopic
            kqlTopic =
            kqlEngine.getMetaStore().getTopic(printTopic.getTopic().getSuffix());
        if (kqlTopic == null) {
          console.println("Topic does not exist: " + printTopic.getTopic().getSuffix());
          return;
        }
        long interval;
        if (printTopic.getIntervalValue() == null) {
          interval = -1;
        } else {
          interval = printTopic.getIntervalValue().getValue();
        }
        printTopic(kqlTopic, interval);
        return;
      } else if (statement instanceof SetProperty) {

      } else if (statement instanceof LoadProperties) {

      }
      console.println("Command/Statement is incorrect or not supported.");

    } catch (Exception e) {
//      e.printStackTrace();
      console.println("Exception: " + e.getMessage());
    }

  }

  /**
   * Given a kql command string, parse the command and return the parse tree.
   */
  public Statement getSingleStatement(final String statementString) throws IOException {
    String statementStr = statementString;
    if (!statementStr.endsWith(";")) {
      statementStr = statementStr + ";";
    }
    List<Statement> statements = null;
    try {
      statements = kqlEngine.getStatements(statementStr);
    } catch (Exception ex) {
      // Do nothing
//      ex.printStackTrace();
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
        "list streams           .................... Show the list of available streams.");
    console.println(
        "list tables           .................... Show the list of available tables.");
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
  private void startQuery(final String queryString, final Query query) throws Exception {
    if (query.getQueryBody() instanceof QuerySpecification) {
      QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
      if (querySpecification.getInto().get() instanceof Table) {
        Table table = (Table) querySpecification.getInto().get();
        if (kqlEngine.metaStore.getSource(table.getName().getSuffix()) != null) {
          console.println(
              "Sink specified in INTO clause already exists: " + table.getName().getSuffix()
          );
          return;
        }
      }
    }

    QueryMetadata queryMetadata =
        kqlEngine.runMultipleQueries(true, queryString).get(0);

    if (queryMetadata instanceof QueuedQueryMetadata) {
      if (currentQueryId != null) {
        console.println("Terminating the currently-running console query first");
        if (stopCurrentQuery()) {
          console.println("Query terminated");
        } else {
          console.println("Query may not have been successfully terminated");
        }
      }
      currentQueryId = queryMetadata.getQueryId();
      currentQueryThread = new Thread(new QueuedQueryPrinter(((QueuedQueryMetadata) queryMetadata).getRowQueue()));
      currentQueryThread.start();
    }
    if (isCLI) {
      console.println();
    }
  }

  private void terminateQuery(final TerminateQuery terminateQuery) throws IOException {
    String queryId = terminateQuery.getQueryId().toString();
    if (kqlEngine.terminateQuery(queryId)) {
      console.println("Terminated query with id = " + queryId);
    } else {
      console.println("No running query with id = " + queryId + " was found!");
    }
    console.flush();
  }

  private void listTopics() throws IOException {
    MetaStore metaStore = kqlEngine.getMetaStore();
    Map<String, KQLTopic> topicMap = metaStore.getAllKQLTopics();
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
      String notes = "";
      if (kqlTopic.getKqlTopicSerDe() instanceof KQLAvroTopicSerDe) {
        KQLAvroTopicSerDe kqlAvroTopicSerDe = (KQLAvroTopicSerDe) kqlTopic.getKqlTopicSerDe();
        notes = "Avro schema path: " + kqlAvroTopicSerDe.getSchemaFilePath();
      }
      console.println(
          " " + padRight(topicName, 42) + "|  " + padRight(kqlTopic.getKafkaTopicName(), 42) + "| "
          + padRight(kqlTopic.getKqlTopicSerDe().getSerDe().toString(), 35) + "| " + notes);
    }
    console.println(
        "-------------------------------------------+--------------------------------------------+------------------------------------+--------------------");
    console.println("( " + topicMap.size() + " rows)");
    console.flush();
  }

  private void listStreams() throws IOException {
    MetaStore metaStore = kqlEngine.getMetaStore();
    Map<String, StructuredDataSource> allDataSources = metaStore.getAllStructuredDataSources();
    List<String> streamsInfo = new LinkedList<>();
    for (String datasourceName : allDataSources.keySet()) {
      StructuredDataSource dataSource = allDataSources.get(datasourceName);
      if (dataSource instanceof KQLStream) {
        KQLStream kqlStream = (KQLStream) dataSource;
        streamsInfo.add(
            " " + padRight(datasourceName, 27) + "|  " + padRight(kqlStream.getKqlTopic()
                                                                         .getName(), 38)
            + "|  " + padRight(kqlStream.getKeyField().name(), 30) + "|    "
            + padRight(kqlStream.getKqlTopic().getKqlTopicSerDe().getSerDe().toString(), 28));
      }
    }
    if (streamsInfo.isEmpty()) {
      console.println("No streams have been defined yet.");
      return;
    }
    console.println(
            "         Name               |               KQL Topic                |             Topic"
                    + " Key          |          Topic Format           ");
    console.println(
            "----------------------------+----------------------------------------+--------------------------------+--------------------------------");
    for (String tableInfo : streamsInfo) {
      console.println(tableInfo);
    }
    console.println(
        "----------------------------+----------------------------------------+--------------------------------+--------------------------------");
    console.println("(" + streamsInfo.size() + " streams)");
    console.flush();
  }

  private void listTables() throws IOException {
    MetaStore metaStore = kqlEngine.getMetaStore();
    Map<String, StructuredDataSource> allDataSources = metaStore.getAllStructuredDataSources();
    List<String> tablesInfo = new LinkedList<>();
    for (String datasourceName : allDataSources.keySet()) {
      StructuredDataSource dataSource = allDataSources.get(datasourceName);
      if (dataSource instanceof KQLTable) {
        KQLTable kqlTable = (KQLTable) dataSource;
        tablesInfo.add(
            " " + padRight(datasourceName, 27) + "|  " + padRight(kqlTable.getKqlTopic().getName(), 38)
            + "|  " + padRight(kqlTable.getKeyField().name(), 30) + "|    "
            + padRight(kqlTable.getKqlTopic().getKqlTopicSerDe().getSerDe().toString(), 28) + "|    "
            + padRight(kqlTable.getStateStoreName(), 30));
      }
    }
    if (tablesInfo.isEmpty()) {
      console.println("No tables have been defined yet.");
      return;
    }
    console.println(
            "         Name               |               KQL Topic                |             Topic"
                    + " Key          |          Topic Format          |            Statestore            ");
    console.println(
        "----------------------------+----------------------------------------+--------------------------------+--------------------------------+----------------------------------");

    for (String tableInfo : tablesInfo) {
      console.println(tableInfo);
    }
    console.println(
            "----------------------------+----------------------------------------+--------------------------------+--------------------------------+----------------------------------");
    console.println("(" + tablesInfo.size() + " tables)");
    console.flush();
  }

  private void showColumns(final String name) throws IOException {
    StructuredDataSource dataSource = kqlEngine.getMetaStore().getSource(name);
    if (dataSource == null) {
      console.println("Could not find topic " + name + " in the metastore!");
      console.flush();
      return;
    }
    console.println("TOPIC: " + name + "    Key: " + dataSource.getKeyField().name()
                    + "    Type: " + dataSource.getDataSourceType());
    console.println();
    console.println(
        "      Column       |         Type         |                   Comment                   ");
    console.println(
        "-------------------+----------------------+---------------------------------------------");
    for (Field schemaField : dataSource.getSchema().fields()) {
      if (schemaField.schema().type() == Schema.Type.ARRAY) {
        console.println(padRight(schemaField.name(), 19) + "|  " + padRight(
            SchemaUtil.TYPE_MAP.get(schemaField.schema().type().getName().toUpperCase())
            + "[" + schemaField.schema()
                .valueSchema().schema().type() + "]", 18)
                        + "  |");
      } else if (schemaField.schema().type() == Schema.Type.MAP) {
        console.println(padRight(schemaField.name(), 19) + "|  " + padRight(
            SchemaUtil.TYPE_MAP.get(schemaField.schema().type().getName().toUpperCase())
            + "<" + schemaField.schema()
                .keySchema().schema().type() + " , " + schemaField.schema()
                .valueSchema().schema().type() + ">", 18)
                        + "  |");
      } else {
        console.println(padRight(schemaField.name(), 19) + "|  " + padRight(
            SchemaUtil.TYPE_MAP.get(schemaField.schema().type().getName().toUpperCase())
            + " (" + schemaField.schema().type().getName() + ")", 18) + "  |");
      }

    }
    console.println(
        "-------------------+----------------------+---------------------------------------------");
    console.println("( " + dataSource.getSchema().fields().size() + " rows)");
    console.flush();
  }

  private void listProperties() throws IOException {
    Map<String, Object> kqlConfigProperties = kqlEngine.kqlConfigProperties;
    KQLConfig kqlConfig = new KQLConfig(kqlConfigProperties);
    StreamsConfig streamsConfig = new StreamsConfig(kqlConfig.getResetStreamsProperties("Test"));
    Map<String, ?> props = streamsConfig.values();
    Map<String, ?> consumerProps = streamsConfig.getRestoreConsumerConfigs(props.get("client.id").toString());
    Map<String, ?> producerProps = streamsConfig.getProducerConfigs(props.get("client.id").toString());
    console.println("------------------------------------------------------------------------------------");
    console.println("Streams config properties: ");
    for (String propName: props.keySet()) {
      console.println(String.format(" %s = %s", propName, props.get(propName)));
    }
    console.println("Consumer config properties: ");
    for (String propName: consumerProps.keySet()) {
      console.println(String.format(" %s = %s", propName, consumerProps.get(propName)));
    }
    console.println("Producer config properties: ");
    for (String propName: producerProps.keySet()) {
      console.println(String.format(" %s = %s", propName, producerProps.get(propName)));
    }
    console.println("------------------------------------------------------------------------------------");
  }

//  private void setProperty(String propertyName, Object propertyValue) throws IOException {
private void setProperty(SetProperty setProperty) throws IOException {
    Map<String, Object> kqlConfigProperties = kqlEngine.kqlConfigProperties;
    KQLConfig kqlConfig = new KQLConfig(kqlConfigProperties);
    StreamsConfig streamsConfig = new StreamsConfig(kqlConfig.getResetStreamsProperties("Test"));
    Map<String, ?> consumerProps = streamsConfig.getRestoreConsumerConfigs(kqlConfig.values().get("client.id").toString());
    Map<String, ?> producerProps = streamsConfig.getProducerConfigs(kqlConfig.values().get("client.id").toString());
    Set<String> validConfigProprtyNames = new HashSet<>(streamsConfig.values().keySet());
    validConfigProprtyNames.addAll(consumerProps.keySet());
    validConfigProprtyNames.addAll(producerProps.keySet());
    String propertyName = setProperty.getPropertyName();
    String propertyValue = setProperty.getPropertyValue();
    if (!validConfigProprtyNames.contains(propertyName)) {
      console.println(String.format(" Property with name %s does not exist.", propertyName));
    } else {
      kqlEngine.kqlConfigProperties.put(propertyName, propertyValue);
      console.println(String.format(" %s = %s", propertyName, propertyValue));
    }

  }

  private void listQueries() throws IOException {
    Map<String, QueryMetadata> liveQueries = kqlEngine.getLiveQueries();
    console.println("Running queries: ");
    console.println(
        " Query ID   |         Query                                                                    |         Query Sink Topic");
    int numQueries = 0;
    for (String queryId : liveQueries.keySet()) {
      QueryMetadata queryInfo = liveQueries.get(queryId);
      if (queryInfo.getQueryOutputNode() instanceof KQLStructuredDataOutputNode) {
        numQueries++;
        console.println(
            "------------+----------------------------------------------------------------------------------+-----------------------------------");
        KQLStructuredDataOutputNode kqlStructuredDataOutputNode = (KQLStructuredDataOutputNode)
            queryInfo.getQueryOutputNode();
        console.println(
            padRight(queryId, 12) + "|   " + padRight(queryInfo.getQueryId(), 80) + "|   " + kqlStructuredDataOutputNode.getKafkaTopicName());
      }
    }
    console.println(
        "------------+----------------------------------------------------------------------------------+-----------------------------------");
    console.println("( " + numQueries + " rows)");
    console.flush();
  }

  private void printTopic(final KQLTopic kqlTopic, final long interval) throws IOException {
    Map<String, Object> kqlConfigProperties = kqlEngine.kqlConfigProperties;
    KQLConfig kqlConfig = new KQLConfig(kqlConfigProperties);
    new TopicPrinter().printGenericRowTopic(kqlTopic, console, interval, kqlConfig);
  }

  private void exportCatalog(final String filePath) {

    MetaStoreImpl metaStore = (MetaStoreImpl) kqlEngine.getMetaStore();
    MetastoreUtil metastoreUtil = new MetastoreUtil();

    metastoreUtil.writeMetastoreToFile(filePath, metaStore);
    try {
      console.println("Wrote the catalog into " + filePath);
    } catch (IOException e) {
    }
  }

  private boolean stopCurrentQuery() {
    if (currentQueryId == null) {
      return false;
    }
    boolean result;
    result = kqlEngine.terminateQuery(currentQueryId);
    currentQueryId = null;
    if (currentQueryThread == null) {
      return result;
    }
    currentQueryThread.interrupt();
    try {
      currentQueryThread.join();
    } catch (InterruptedException exception) {
      return result;
    } finally {
      currentQueryThread = null;
    }
    return result;
  }


  private static String padRight(String s, int n) {
    return String.format("%1$-" + n + "s", s);
  }

  private KQL(MetaStore metaStore, Map<String, Object> kqlProperties) throws IOException {
    kqlEngine = new KQLEngine(metaStore, kqlProperties);
  }

  private class QueuedQueryPrinter implements Runnable {
    private final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue;

    public QueuedQueryPrinter(SynchronousQueue<KeyValue<String, GenericRow>> rowQueue) {
      this.rowQueue = rowQueue;
    }

    @Override
    public void run() {
      try {
        while (true) {
          KeyValue<String, GenericRow> row = rowQueue.take();
          console.println(String.format("%s => %s", row.key, row.value));
          console.flush();
        }
      } catch (IOException exception) {
        throw new RuntimeException(exception);
      } catch (InterruptedException exception) {
        // Interrupt is used to signal the thread to stop
      }
    }
  }


  public static void main(String[] args)
      throws Exception {

    CLIOptions cliOptions = CLIOptions.parse(args);
    if (cliOptions == null) {
      return;
    }

    Map<String, Object> kqlProperties = CLIOptions.getPropertiesMap(cliOptions.getPropertiesFile());

    MetaStore metaStore;
    String catalogFile = cliOptions.getCatalogFile();
    if (catalogFile != null) {
      metaStore = new MetastoreUtil().loadMetaStoreFromJSONFile(catalogFile);
    } else {
      metaStore = new MetaStoreImpl();
    }

    KQL kql = new KQL(metaStore, kqlProperties);

    String queries = cliOptions.getQueries();
    if (queries != null) {
      kql.kqlEngine.runMultipleQueries(true, queries);
      Long queryTime = cliOptions.getQueryTime();
      if (queryTime != null) {
        Thread.sleep(queryTime);
      }
      return;
    }

    String queryFile = cliOptions.getQueryFile();
    if (queryFile != null) {
      kql.isCLI = false;
      kql.runQueriesFromFile(queryFile);
      Long queryTime = cliOptions.getQueryTime();
      if (queryTime != null) {
        Thread.sleep(queryTime);
      }
      return;
    }

    kql.startConsole();
  }
}
