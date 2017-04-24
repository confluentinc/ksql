/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql;

import io.confluent.kql.rest.client.KQLRestClient;
import jline.console.ConsoleReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;


public class Cli {

  private final ConsoleReader console;
  private final KQLRestClient restClient;

  public Cli(KQLRestClient restClient) throws IOException {
    this.console = new ConsoleReader();
    this.restClient = restClient;
  }

  public void repl() throws IOException {
    console.println("Entering REPL");
    console.flush();
    for (String line = console.readLine(); line != null; line = console.readLine()) {
      if (line.trim().isEmpty()) {
        continue;
      } else if (line.trim().startsWith(":")) {
        handleMetaCommand(line.trim());
      } else {
        handleStatements(line);
      }
      console.flush();
    }
    console.println("Exiting REPL");
    console.flush();
    console.close();
  }

  private void handleMetaCommand(String trimmedLine) throws IOException {
    String[] commandArgs = trimmedLine.split("\\s+", 2);
    String command = commandArgs[0];
    switch (command) {
      case ":":
        console.println("Lines beginning with ':' must contain exactly one meta-command");
        break;
      case ":status": {
        JsonStructure jsonResponse;
        if (commandArgs.length == 1) {
          jsonResponse = restClient.makeStatusRequest();
        } else {
          String statementId = commandArgs[1];
          jsonResponse = restClient.makeStatusRequest(statementId);
        }
        printJsonResponse(jsonResponse);
        break;
      }
      case ":query":
        if (commandArgs.length == 1) {
          throw new RuntimeException(String.format("'%s' meta-command must be followed by a query to stream", command));
        }
        String query = commandArgs[1];
        handleStreamedQuery(query);
        break;
      case ":help":
        printCommandList();
        break;
      default:
        throw new RuntimeException(String.format("Unknown meta-command: ':%s'", command));
    }
  }

  private void handleStatements(String line) throws IOException {
    JsonStructure jsonResponse = restClient.makeKQLRequest(line);
    printJsonResponse(jsonResponse);
  }

  private void handleStreamedQuery(String query) throws IOException {
    try (InputStream queryStream = restClient.makeQueryRequest(query)) {
      Scanner queryScanner = new Scanner(queryStream);
      while (queryScanner.hasNextLine()) {
        String line = queryScanner.nextLine();
        if (!line.trim().isEmpty()) {
          InputStream lineInputStream = new ByteArrayInputStream(line.getBytes());
          printJsonResponse(Json.createReader(lineInputStream).read());
          console.flush();
        }
      }
    }
    console.println("Query terminated");
  }

  private void printJsonResponse(JsonStructure jsonResponse) throws IOException {
    switch (jsonResponse.getValueType()) {
      case OBJECT:
        console.println(jsonResponse.toString());
        break;
      case ARRAY:
        JsonArray responseArray = (JsonArray) jsonResponse;
        for (JsonValue responseElement : responseArray) {
          console.println(responseElement.toString());
        }
        break;
      default:
        throw new RuntimeException(String.format(
            "Unexpected JSON response type: %s",
            jsonResponse.getValueType().toString()
        ));
    }
  }

  private void printCommandList() throws IOException {
    console.println("Cli cli commands: ");
    console.println(
        "------------------------------------------------------------------------------------ ");
    console.println(
        "list topics           .................... Show the list of available topics.");
    console.println(
        "list streams          .................... Show the list of available streams.");
    console.println(
        "list tables           .................... Show the list of available tables.");
    console.println(
        "describe <name>       .................... Show the schema of the given stream/table.");
    console.println(
        "show queries          .................... Show the list of running queries.");
    console.println(
        "print <topic name>    .................... Print the content of a given topic/stream.");
    console.println(
        "terminate <query id>  .................... Terminate the running query with the given id.");
    console.println();
    console.println("For more information refer to www.kql.confluent.io");
    console.println();
  }

  public static void main(String[] args)
      throws Exception {

//    KQL cliOptions = KQL.parse(args);
//    if (cliOptions == null) {
//      return;
//    }
//
//    Map<String, Object> kqlProperties = KQL.getPropertiesMap(cliOptions.getPropertiesFile());
//
//    MetaStore metaStore;
//    String catalogFile = cliOptions.getCatalogFile();
//    if (catalogFile != null) {
//      metaStore = new MetastoreUtil().loadMetaStoreFromJSONFile(catalogFile);
//    } else {
//      metaStore = new MetaStoreImpl();
//    }
//
//    Cli kql = new Cli(metaStore, new KQLConfig(kqlProperties));
//
//    String queries = cliOptions.getQueries();
//    if (queries != null) {
//      kql.kqlEngine.runMultipleQueries(true, queries);
//      Long queryTime = cliOptions.getQueryTime();
//      if (queryTime != null) {
//        Thread.sleep(queryTime);
//      }
//      return;
//    }
//
//    String queryFile = cliOptions.getQueryFile();
//    if (queryFile != null) {
//      kql.isCLI = false;
//      kql.runQueriesFromFile(queryFile);
//      Long queryTime = cliOptions.getQueryTime();
//      if (queryTime != null) {
//        Thread.sleep(queryTime);
//      }
//      return;
//    }
//
//    kql.startConsole();
  }

}
