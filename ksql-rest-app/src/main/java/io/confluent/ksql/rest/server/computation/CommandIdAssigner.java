/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.CreateTopic;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;

public class CommandIdAssigner {

  private final MetaStore metaStore;

  public CommandIdAssigner(MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public CommandId getCommandId(Statement command) {
    if (command instanceof CreateTopic) {
      return getTopicCommandId((CreateTopic) command);
    } else if (command instanceof CreateStream) {
      return getTopicStreamCommandId((CreateStream) command);
    } else if (command instanceof CreateTable) {
      return getTopicTableCommandId((CreateTable) command);
    } else if (command instanceof CreateStreamAsSelect) {
      return getSelectStreamCommandId((CreateStreamAsSelect) command);
    } else if (command instanceof CreateTableAsSelect) {
      return getSelectTableCommandId((CreateTableAsSelect) command);
    } else if (command instanceof TerminateQuery) {
      return getTerminateCommandId((TerminateQuery) command);
    } else if (command instanceof DropTopic) {
      return getDropTopicCommandId((DropTopic) command);
    } else if (command instanceof DropStream) {
      return getDropStreamCommandId((DropStream) command);
    } else if (command instanceof DropTable) {
      return getDropTableCommandId((DropTable) command);
    } else {
      throw new RuntimeException(String.format(
          "Cannot assign command ID to statement of type %s",
          command.getClass().getCanonicalName()
      ));
    }
  }

  public CommandId getTopicCommandId(CreateTopic createTopic) {
    String topicName = createTopic.getName().toString();
    if (metaStore.getAllTopicNames().contains(topicName)) {
      throw new RuntimeException(String.format("Topic %s already exists", topicName));
    }
    return new CommandId(CommandId.Type.TOPIC, topicName);
  }

  public CommandId getTopicStreamCommandId(CreateStream createStream) {
    return getStreamCommandId(createStream.getName().toString());
  }

  public CommandId getSelectStreamCommandId(CreateStreamAsSelect createStreamAsSelect) {
    return getStreamCommandId(createStreamAsSelect.getName().toString());
  }

  public CommandId getTopicTableCommandId(CreateTable createTable) {
    return getTableCommandId(createTable.getName().toString());
  }

  public CommandId getSelectTableCommandId(CreateTableAsSelect createTableAsSelect) {
    return getTableCommandId(createTableAsSelect.getName().toString());
  }

  public CommandId getTerminateCommandId(TerminateQuery terminateQuery) {
    return new CommandId(CommandId.Type.TERMINATE, Long.toString(terminateQuery.getQueryId()));
  }
  public CommandId getDropTopicCommandId(DropTopic dropTopicQuery) {
    return new CommandId(CommandId.Type.TOPIC,
                         dropTopicQuery.getTopicName().getSuffix() + "_DROP");
  }

  public CommandId getDropStreamCommandId(DropStream dropStreamQuery) {
    return new CommandId(CommandId.Type.STREAM,
                         dropStreamQuery.getStreamName().getSuffix() + "_DROP");
  }

  public CommandId getDropTableCommandId(DropTable dropTableQuery) {
    return new CommandId(CommandId.Type.TABLE,
                         dropTableQuery.getTableName().getSuffix() + "_DROP");
  }

  private CommandId getStreamCommandId(String streamName) {
    return getSourceCommandId(CommandId.Type.STREAM, streamName);
  }

  private CommandId getTableCommandId(String tableName) {
    return getSourceCommandId(CommandId.Type.TABLE, tableName);
  }

  private CommandId getSourceCommandId(CommandId.Type type, String sourceName) {
    if (metaStore.getAllStructuredDataSourceNames().contains(sourceName)) {
      throw new RuntimeException(String.format("Source %s already exists", sourceName));
    }
    return new CommandId(type, sourceName);
  }
}
