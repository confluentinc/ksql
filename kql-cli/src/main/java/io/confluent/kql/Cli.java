/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql;

import io.confluent.kql.rest.client.KQLRestClient;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cli {

  private static final String DEFAULT_PROMPT = "kql> ";
  private static final Pattern QUOTED_PROMPT_PATTERN = Pattern.compile("'(''|[^'])*'");

  private final KQLRestClient restClient;
  private final ConsoleReader console;

  public Cli(KQLRestClient restClient) throws IOException {
    this.restClient = restClient;

    this.console = new ConsoleReader();
    console.setExpandEvents(false); // Otherwise, things like '!=' will cause things to break
    console.setPrompt(DEFAULT_PROMPT);
    console.setHandleUserInterrupt(true);
  }

  public void repl() throws IOException {
    console.flush();
    for (String line = readLine(); line != null && handleLine(line.trim()); line = readLine()) {
      console.flush();
    }
    console.println();
    console.flush();
    console.close();
  }

  private String readLine() throws IOException {
    while (true) {
      try {
        return console.readLine();
      } catch (UserInterruptException exception) {
        console.println("^C");
        console.flush();
      }
    }
  }

  // Return value is a hacky way of detecting EOF
  private boolean handleLine(String trimmedLine) throws IOException {
    if (trimmedLine.isEmpty()) {
      return true;
    }

    try {
      if (trimmedLine.startsWith(":")) {
        handleMetaCommand(trimmedLine);
      } else {
        StringBuilder multiLineBuffer = new StringBuilder();
        String currentPrompt = console.getPrompt();
        while (trimmedLine.endsWith("\\")) {
          multiLineBuffer.append(trimmedLine.substring(0, trimmedLine.length() - 1));
          try {
            trimmedLine = console.readLine("");
            if (trimmedLine == null) {
              return false;
            }
            trimmedLine = trimmedLine.trim();
          } catch (UserInterruptException exception) {
            console.println("^C");
            return true;
          }
        }
        console.setPrompt(currentPrompt);
        multiLineBuffer.append(trimmedLine);
        handleStatements(multiLineBuffer.toString());
      }
    } catch (RuntimeException exception) {
      if (exception.getMessage() != null) {
        console.println(exception.getMessage());
      } else {
        console.println("An unexpected exception with no message was encountered.");
      }
    }
    return true;
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
      case ":prompt":
        if (commandArgs.length == 1) {
          throw new RuntimeException(String.format(
              "'%s' meta-command must be followed by a new prompt value, optionally enclosed in single quotes",
              command
          ));
        }
        String prompt = commandArgs[1];
        if (prompt.startsWith("'")) {
          Matcher quotedPromptMatcher = QUOTED_PROMPT_PATTERN.matcher(prompt);
          if (quotedPromptMatcher.matches()) {
            console.setPrompt(prompt.substring(1, prompt.length() - 1).replace("''", "'"));
          } else {
            throw new RuntimeException(
                "Failed to parse prompt string. All non-enclosing single quotes must be doubled."
            );
          }
        } else {
          console.setPrompt(prompt);
        }
        break;
      case ":help":
        printHelpMessage();
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
    AtomicBoolean continueStreaming = new AtomicBoolean(true);
    Thread queryPrintThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try (Scanner queryScanner = new Scanner(restClient.makeQueryRequest(query))) {
          while (continueStreaming.get() && queryScanner.hasNextLine()) {
            String line = queryScanner.nextLine();
            if (!line.trim().isEmpty()) {
              InputStream lineInputStream = new ByteArrayInputStream(line.getBytes());
              try {
                printJsonResponse(Json.createReader(lineInputStream).read());
                console.flush();
              } catch (IOException exception) {
                throw new RuntimeException(exception);
              }
            }
          }
        }
      }
    });
    queryPrintThread.start();

    String currentPrompt = console.getPrompt();
    // Have to find some way to call console.readLine(), since that's the only way to cause a UserInterruptException to
    // be thrown; in this case, a separate thread is dispatched to print out the contents of the streamed query, while
    // the current thread just continuously calls console.readLine() and ignores all user input until they press ^C,
    // triggering the UserInterruptException and causing everything to go back to normal.
    try {
      while (true) {
        console.readLine("");
      }
    } catch (UserInterruptException exception) {
      continueStreaming.set(false);
    }
    try {
      queryPrintThread.join();
    } catch (InterruptedException exception) {
      throw new RuntimeException(exception);
    }
    console.println("Query terminated");
    console.setPrompt(currentPrompt);
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

  private void printHelpMessage() throws IOException {
    console.println("meta-commands (one per line, must be on their own line):");
    console.println();
    console.println("    :exit               - Exit the REPL (^D is also fine)");
    console.println("    :help               - Show this message");
    console.println("    :prompt <prompt>    - Change the prompt to <prompt>");
    console.println("                          Example: :prompt 'my ''special'' prompt> '");
//    console.println("    :output             - Show the current output format");
//    console.println("                          Example: :output");
//    console.println("    :output <format>    - Change the output format to <format> (case-insensitive), "
//        + "currently only 'json' and 'cli' are supported)"
//    );
//    console.println("                          Example: :output JSON");
//    console.println("    :output CLI <width> - Change the output format to CLI, with a minimum column width of <width>");
//    console.println("                          Example: :output CLI 69");
//    console.println("    :server             - Show the current server");
//    console.println("    :server <server>    - Change the current server to <server>");
//    console.println("                          Example: :server confluent.io:6969'");
    console.println("    :status             - Check on the statuses of all KQL statements processed by the server");
    console.println("                          Example: :status");
    console.println("    :status <id>        - Check on the status of the statement with ID <id>");
    console.println("                          Example: :status KQL_NODE_69_STATEMENT_69");
    console.println("    :query <query>      - Stream the results of <query> to the console");
    console.println("                          Example: :query SELECT * FROM movies WHERE imdb_score = 69;");
    console.println();
    console.println("default behavior:");
    console.println();
    console.println("    Lines are read one at a time and are sent to the server as KQL unless one of "
        + "the following is true:"
    );
    console.println();
    console.println("    1. The line is empty or entirely whitespace. In this case, no request is made to the server.");
    console.println();
    console.println("    2. The line begins with ':'. In this case, the line is parsed as a meta-command "
        + "(as detailed above)."
    );
    console.println();
    console.println("    3. The line ends with '\\'. In this case, lines are continuously read and stripped of their "
        + "trailing newline and '\\' until one is encountered that does not end with '\\'; then, the concatenation of "
        + "all lines read during this time is sent to the server as KQL."
    );
  }
}
