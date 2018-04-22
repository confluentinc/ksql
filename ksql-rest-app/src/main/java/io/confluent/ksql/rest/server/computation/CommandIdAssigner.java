/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.computation;

import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;

public class CommandIdAssigner {

  interface CommandIdSupplier {
    CommandId apply(final Statement command);
  }

  private final MetaStore metaStore;
  private final Map<Class<? extends Statement>, CommandIdSupplier> suppliers = new HashMap<>();

  public CommandIdAssigner(MetaStore metaStore) {
    this.metaStore = metaStore;
    suppliers.put(RegisterTopic.class,
        command -> getTopicCommandId((RegisterTopic) command));
    suppliers.put(CreateStream.class,
        command -> getTopicStreamCommandId((CreateStream) command));
    suppliers.put(CreateTable.class,
        command -> getTopicTableCommandId((CreateTable) command));
    suppliers.put(CreateStreamAsSelect.class,
        command -> getSelectStreamCommandId((CreateStreamAsSelect) command));
    suppliers.put(CreateTableAsSelect.class,
        command -> getSelectTableCommandId((CreateTableAsSelect) command));
    suppliers.put(TerminateQuery.class,
        command -> getTerminateCommandId((TerminateQuery) command));
    suppliers.put(DropTopic.class,
        command -> getDropTopicCommandId((DropTopic) command));
    suppliers.put(DropStream.class,
        command -> getDropStreamCommandId((DropStream) command));
    suppliers.put(DropTable.class,
        command -> getDropTableCommandId((DropTable) command));
    suppliers.put(RunScript.class,
        command -> new CommandId(CommandId.Type.STREAM, "RunScript", CommandId.Action.EXECUTE));
  }

  public CommandId getCommandId(Statement command) {
    final CommandIdSupplier supplier = suppliers.get(command.getClass());
    if (supplier == null) {
      throw new RuntimeException(String.format(
          "Cannot assign command ID to statement of type %s",
          command.getClass().getCanonicalName()
      ));
    }
    return supplier.apply(command);
  }

  private CommandId getTopicCommandId(RegisterTopic registerTopic) {
    String topicName = registerTopic.getName().toString();
    if (metaStore.getAllTopicNames().contains(topicName)) {
      throw new RuntimeException(String.format("Topic %s already exists", topicName));
    }
    return new CommandId(CommandId.Type.TOPIC, topicName, CommandId.Action.CREATE);
  }

  private CommandId getTopicStreamCommandId(CreateStream createStream) {
    return getStreamCommandId(createStream.getName().toString());
  }

  private CommandId getSelectStreamCommandId(CreateStreamAsSelect createStreamAsSelect) {
    return getStreamCommandId(createStreamAsSelect.getName().toString());
  }

  private CommandId getTopicTableCommandId(CreateTable createTable) {
    return getTableCommandId(createTable.getName().toString());
  }

  private CommandId getSelectTableCommandId(CreateTableAsSelect createTableAsSelect) {
    return getTableCommandId(createTableAsSelect.getName().toString());
  }

  private CommandId getTerminateCommandId(TerminateQuery terminateQuery) {
    return new CommandId(
        CommandId.Type.TERMINATE,
        terminateQuery.getQueryId().toString(),
        CommandId.Action.EXECUTE
    );
  }

  private CommandId getDropTopicCommandId(DropTopic dropTopicQuery) {
    return new CommandId(
        CommandId.Type.TOPIC,
        dropTopicQuery.getTopicName().getSuffix(),
        CommandId.Action.DROP
    );
  }

  private CommandId getDropStreamCommandId(DropStream dropStreamQuery) {
    return new CommandId(
        CommandId.Type.STREAM,
        dropStreamQuery.getName().getSuffix(),
        CommandId.Action.DROP
    );
  }

  private CommandId getDropTableCommandId(DropTable dropTableQuery) {
    return new CommandId(
        CommandId.Type.TABLE,
        dropTableQuery.getName().getSuffix(),
        CommandId.Action.DROP
    );
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
    return new CommandId(type, sourceName, CommandId.Action.CREATE);
  }
}
