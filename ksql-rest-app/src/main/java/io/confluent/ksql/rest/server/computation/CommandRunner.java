/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server.computation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.ksql.util.Pair;

/**
 * Handles the logic of reading distributed commands, including pre-existing commands that were
 * issued before being initialized, and then delegating their execution to a
 * {@link StatementExecutor}. Also responsible for taking care of any exceptions that occur in the
 * process.
 */
public class CommandRunner implements Runnable, Closeable {

  private static final Logger log = LoggerFactory.getLogger(CommandRunner.class);

  private final StatementExecutor statementExecutor;
  private final CommandStore commandStore;
  private final AtomicBoolean closed;

  public CommandRunner(
      StatementExecutor statementExecutor,
      CommandStore commandStore
  ) {
    this.statementExecutor = statementExecutor;
    this.commandStore = commandStore;

    closed = new AtomicBoolean(false);
  }

  /**
   * Begin a continuous poll-execute loop for the command topic, stopping only if either a
   * {@link WakeupException} is thrown or the {@link #close()} method is called.
   */
  @Override
  public void run() {
    try {
      while (!closed.get()) {
        log.debug("Polling for new writes to command topic");
        ConsumerRecords<CommandId, Command> records = commandStore.getNewCommands();
        log.debug("Found {} new writes to command topic", records.count());
        for (ConsumerRecord<CommandId, Command> record : records) {
          CommandId commandId = record.key();
          Command command = record.value();
          if (command.getStatement() != null) {
            executeStatement(command, commandId);
          } else {
            log.debug("Skipping null statement for ID {}", commandId);
          }
        }
      }
    } catch (WakeupException wue) {
      if (!closed.get()) {
        throw wue;
      }
    }
  }

  /**
   * Halt the poll-execute loop.
   */
  @Override
  public void close() {
    closed.set(true);
    commandStore.close();
  }

  /**
   * Read and execute all commands on the command topic, starting at the earliest offset.
   * @throws Exception TODO: Refine this.
   */
  public void processPriorCommands() throws Exception {
    List<Pair<CommandId, Command>> priorCommands = commandStore.getPriorCommands();
    statementExecutor.handleStatements(priorCommands);
  }

  private void executeStatement(Command command, CommandId commandId) {
    log.info("Executing statement: " + command.getStatement());
    try {
      statementExecutor.handleStatement(command, commandId);
    } catch (WakeupException wue) {
      throw wue;
    } catch (Exception exception) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      exception.printStackTrace(printWriter);
      log.error("Exception encountered during poll-parse-execute loop: " + stringWriter.toString());
    }
  }
}
