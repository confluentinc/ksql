/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest.computation;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.QueryEngine;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.tree.CreateStreamAsSelect;
import io.confluent.kql.parser.tree.CreateTableAsSelect;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.QuerySpecification;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.parser.tree.TerminateQuery;
import io.confluent.kql.physical.PhysicalPlanBuilder;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.planner.plan.OutputNode;
import io.confluent.kql.planner.plan.PlanNode;
import io.confluent.kql.rest.StatementParser;
import io.confluent.kql.structured.SchemaKStream;
import io.confluent.kql.structured.SchemaKTable;
import io.confluent.kql.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles the logic of polling for new queries, assigning them an ID, and then delegating their execution to a
 * {@link QueryHandler}. Also responsible for taking care of any exceptions that occur in the process.
 */
public class QueryComputer implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(QueryComputer.class);

  private final QueryHandler queryHandler;
  private final String commandTopic;
  private final long pollTimeout;
  private final KafkaConsumer<String, String> commandConsumer;
  private final Map<String, StatementStatus> statusStore;
  private final StatementParser statementParser;
  private final String statementPrefix;
  private final QueryEngine queryEngine;
  private final MetaStore metaStore;
  private final KQLEngine kqlEngine;
  private final AtomicBoolean closed;

  public QueryComputer(
      QueryHandler queryHandler,
      String commandTopic,
      long pollTimeout,
      KafkaConsumer<String, String> commandConsumer,
      Map<String, StatementStatus> statusStore,
      StatementParser statementParser,
      String statementPrefix,
      KQLEngine kqlEngine
  ) {
    this.queryHandler = queryHandler;
    this.commandTopic = commandTopic;
    this.pollTimeout = pollTimeout;
    this.commandConsumer = commandConsumer;
    this.statusStore = statusStore;
    this.statementParser = statementParser;
    this.statementPrefix = statementPrefix;
    this.kqlEngine = kqlEngine;

    this.queryEngine = kqlEngine.getQueryEngine();
    this.metaStore = kqlEngine.getMetaStore();

    commandConsumer.subscribe(Collections.singleton(commandTopic));

    closed = new AtomicBoolean(false);
  }

  // Returns the number of statements that this specific node has processed
  public int processPriorCommands() throws Exception {
    int result = 0;
    LinkedHashMap<String, String> priorCommands = getPriorCommands();
    Map<String, String> terminatedQueries = getTerminatedQueries(priorCommands);
    for (Map.Entry<String, String> commandEntry : priorCommands.entrySet()) {
      if (commandEntry.getKey().startsWith(statementPrefix)) {
        int commandNumber = Integer.parseInt(commandEntry.getKey().substring(statementPrefix.length()));
        result = Math.max(result, commandNumber);
      }
      Statement statement = statementParser.parseSingleStatement(commandEntry.getValue());
      if ((statement instanceof CreateStreamAsSelect || statement instanceof CreateTableAsSelect) &&
          terminatedQueries.containsKey(commandEntry.getKey() + "_QUERY")
      ) {
        Query query;
        if (statement instanceof CreateStreamAsSelect) {
          CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
          QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
          query = kqlEngine.addInto(
              createStreamAsSelect.getQuery(),
              querySpecification,
              createStreamAsSelect.getName().getSuffix(),
              createStreamAsSelect.getProperties()
          );
        } else {
          CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
          QuerySpecification querySpecification = (QuerySpecification) createTableAsSelect.getQuery().getQueryBody();
          query = kqlEngine.addInto(
              createTableAsSelect.getQuery(),
              querySpecification,
              createTableAsSelect.getName().getSuffix(),
              createTableAsSelect.getProperties()
          );
        }
        String queryId = (commandEntry.getKey() + "_query").toUpperCase();
        List<Pair<String, Query>> queryList = Collections.singletonList(new Pair<>(queryId, query));
        PlanNode logicalPlan = queryEngine.buildLogicalPlans(metaStore, queryList).get(0).getRight();
        KStreamBuilder builder = new KStreamBuilder();
        PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
        SchemaKStream schemaKStream = physicalPlanBuilder.buildPhysicalPlan(logicalPlan);
        OutputNode outputNode = physicalPlanBuilder.getPlanSink();
        if (outputNode instanceof KQLStructuredDataOutputNode) {
          KQLStructuredDataOutputNode outputKafkaTopicNode = (KQLStructuredDataOutputNode) outputNode;
          if (metaStore.getTopic(outputKafkaTopicNode.getKafkaTopicName()) == null) {
            metaStore.putTopic(outputKafkaTopicNode.getKqlTopic());
          }
          StructuredDataSource sinkDataSource;
          if (schemaKStream instanceof SchemaKTable) {
            sinkDataSource =
                new KQLTable(outputKafkaTopicNode.getId().toString(),
                    outputKafkaTopicNode.getSchema(),
                    outputKafkaTopicNode.getKeyField(),
                    outputKafkaTopicNode.getKqlTopic(), outputKafkaTopicNode.getId()
                    .toString() + "_statestore");
          } else {
            sinkDataSource =
                new KQLStream(outputKafkaTopicNode.getId().toString(),
                    outputKafkaTopicNode.getSchema(),
                    outputKafkaTopicNode.getKeyField(),
                    outputKafkaTopicNode.getKqlTopic());
          }

          metaStore.putSource(sinkDataSource);
        } else {
          throw new Exception(String.format(
              "Unexpected output node type: '%s'",
              outputNode.getClass().getCanonicalName()
          ));
        }
        statusStore.put(
            commandEntry.getKey(),
            new StatementStatus(StatementStatus.Status.TERMINATED, "Query terminated")
        );
        statusStore.put(
            terminatedQueries.get(commandEntry.getKey() + "_QUERY"),
            new StatementStatus(StatementStatus.Status.SUCCESS, "Termination request granted")
        );
      } else if (!(statement instanceof TerminateQuery)) {
        executeStatement(commandEntry.getValue(), commandEntry.getKey());
      }
    }

    for (String terminateCommand : terminatedQueries.values()) {
      if (!statusStore.containsKey(terminateCommand)) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        new Exception("Query not found").printStackTrace(printWriter);
        statusStore.put(terminateCommand, new StatementStatus(StatementStatus.Status.ERROR, stringWriter.toString()));
      }
    }

    return result;
  }

  private LinkedHashMap<String, String> getPriorCommands() {
    LinkedHashMap<String, String> result = new LinkedHashMap<>();
    for (ConsumerRecord<String, String> commandRecord : getAllPriorCommandRecords()) {
      String commandId = commandRecord.key();
      String command = commandRecord.value();
      if (command != null) {
        result.put(commandId, command);
      } else {
        // Handle potential log compaction by ignoring commands with null values
        result.remove(commandId);
      }
    }
    return result;
  }

  private List<ConsumerRecord<String, String>> getAllPriorCommandRecords() {
    Collection<TopicPartition> commandTopicPartitions = getTopicPartitionsForTopic(commandTopic);

    commandConsumer.poll(0); // Have to poll to make sure subscription has taken effect (subscribe() is lazy)
    commandConsumer.seekToBeginning(commandTopicPartitions);

    Map<TopicPartition, Long> currentOffsets = new HashMap<>();

    List<ConsumerRecord<String, String>> result = new ArrayList<>();
    log.info("Polling end offset(s) for command topic");
    Map<TopicPartition, Long> endOffsets = commandConsumer.endOffsets(commandTopicPartitions);
    // Only want to poll for end offsets at the very beginning, and when we think we may be caught up.
    // So, this outer loop tries to catch up (via the inner loop), then when it believes it has (signalled by having
    // exited the inner loop), end offsets are polled again and another check is performed to see if anything new has
    // been written (which would be signalled by the end offsets having changed). If something new has been written,
    // the outer loop is repeated; if not, we're caught up to the end offsets we just polled and can
    // continue.
    do {
      while (!offsetsCaughtUp(currentOffsets, endOffsets)) {
        log.info("Polling for prior command records");
        ConsumerRecords<String, String> records = commandConsumer.poll(pollTimeout);
        log.info(String.format("Received %d records from poll", records.count()));
        for (ConsumerRecord<String, String> record : records) {
          result.add(record);
          TopicPartition recordTopicPartition = new TopicPartition(record.topic(), record.partition());
          Long currentOffset = currentOffsets.get(recordTopicPartition);
          if (currentOffset == null || currentOffset < record.offset()) {
            currentOffsets.put(recordTopicPartition, record.offset());
          }
        }
      }
      log.info("Polling end offset(s) for command topic");
      endOffsets = commandConsumer.endOffsets(commandTopicPartitions);
    } while (!offsetsCaughtUp(currentOffsets, endOffsets));

    return result;
  }

  private Collection<TopicPartition> getTopicPartitionsForTopic(String topic) {
    List<PartitionInfo> partitionInfoList = commandConsumer.partitionsFor(topic);

    Collection<TopicPartition> result = new HashSet<>();
    for (PartitionInfo partitionInfo : partitionInfoList) {
      result.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }

    return result;
  }

  private boolean offsetsCaughtUp(Map<TopicPartition, Long> offsets, Map<TopicPartition, Long> endOffsets) {
    log.info("Checking to see if consumed command records are caught up with end offset(s)");
    for (Map.Entry<TopicPartition, Long> endOffset : endOffsets.entrySet()) {
      long offset = offsets.getOrDefault(endOffset.getKey(), 0L);
      /*
          From https://kafka.apache.org/0101/javadoc/index.html?org/apache/kafka/streams/kstream/KTable.html
          "The last offset of a partition is the offset of the upcoming message,
          i.e. the offset of the last available message + 1"
          Hence, "offset + 1" instead of just "offset"
       */
      if (offset + 1 < endOffset.getValue()) {
        log.info(String.format(
            "Consumed command records are not yet caught up with offset for partition %d; end offset is %d, but last "
            + "consumed offset is %d",
            endOffset.getKey().partition(),
            endOffset.getValue(),
            offset
        ));
        return false;
      }
    }
    log.info("Consumed command records are caught up with end offset(s)");
    return true;
  }

  private Map<String, String> getTerminatedQueries(Map<String, String> commands) throws Exception {
    Map<String, String> result = new HashMap<>();

    Pattern unquotedTerminatePattern =
        Pattern.compile("\\s*TERMINATE\\s+([A-Z][A-Z0-9_@:]*)\\s*;?\\s*", Pattern.CASE_INSENSITIVE);
    Pattern doubleQuotedTerminatePattern =
        Pattern.compile("\\s*TERMINATE\\s+\"((\"\"|[^\"])*)\"\\s*;?\\s*", Pattern.CASE_INSENSITIVE);
    Pattern backQuotedTerminatePattern =
        Pattern.compile("\\s*TERMINATE\\s+`((``|[^`])*)`\\s*;?\\s*", Pattern.CASE_INSENSITIVE);
    for (Map.Entry<String, String> commandEntry : commands.entrySet()) {
      String commandId = commandEntry.getKey();
      String command = commandEntry.getValue();
      Matcher unquotedMatcher = unquotedTerminatePattern.matcher(command);
      if (unquotedMatcher.matches()) {
        result.put(unquotedMatcher.group(1).toUpperCase(), commandId);
        continue;
      }

      Matcher doubleQuotedMatcher = doubleQuotedTerminatePattern.matcher(command);
      if (doubleQuotedMatcher.matches()) {
        result.put(doubleQuotedMatcher.group(1).replace("\"\"", "\""), commandId);
        continue;
      }

      Matcher backQuotedMatcher = backQuotedTerminatePattern.matcher(command);
      if (backQuotedMatcher.matches()) {
        result.put(backQuotedMatcher.group(1).replace("``", "`"), commandId);
        continue;
      }
    }

    return result;
  }

  @Override
  public void run() {
    try {
      while (!closed.get()) {
        log.info("Polling for new writes to command topic");
        ConsumerRecords<String, String> records = commandConsumer.poll(pollTimeout);
        log.info(String.format("Found %d new writes to command topic", records.count()));
        for (ConsumerRecord<String, String> record : records) {
          String statementId = record.key();
          String statementStr = record.value();
          if (statementStr != null) {
            executeStatement(statementStr, statementId);
          } else {
            log.info(String.format("Skipping null statement for ID %s", statementId));
          }
        }
      }
    } catch (WakeupException wue) {
      if (!closed.get()) {
        throw wue;
      }
    } finally {
      commandConsumer.close();
    }
  }

  private void executeStatement(String statementStr, String statementId) {
    log.info("Executing statement: " + statementStr);
    statusStore.put(statementId, new StatementStatus(StatementStatus.Status.PARSING, "Parsing statement"));
    try {
      queryHandler.handleStatement(statementStr, statementId);
    } catch (WakeupException wue) {
      throw wue;
    } catch (Exception exception) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      exception.printStackTrace(printWriter);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.ERROR, stringWriter.toString()));
      log.error("Exception encountered during poll-parse-execute loop: " + stringWriter.toString());
    }
  }

  public void shutdown() {
    closed.set(true);
    commandConsumer.wakeup();
  }
}
