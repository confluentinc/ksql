/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.rest.server.computation.CommandId.Action;
import io.confluent.ksql.rest.server.computation.CommandId.Type;
import io.confluent.ksql.rest.util.TerminateCluster;
import java.util.Map;

public class CommandIdAssigner {

  interface CommandIdSupplier {
    CommandId apply(Statement command);
  }

  private static final Map<Class<? extends Statement>, CommandIdSupplier> SUPPLIERS =
      ImmutableMap.<Class<? extends Statement>, CommandIdSupplier>builder()
          .put(RegisterTopic.class,
            command -> getTopicCommandId((RegisterTopic) command))
          .put(CreateStream.class,
            command -> getTopicStreamCommandId((CreateStream) command))
          .put(CreateTable.class,
            command -> getTopicTableCommandId((CreateTable) command))
          .put(CreateStreamAsSelect.class,
            command -> getSelectStreamCommandId((CreateStreamAsSelect) command))
          .put(CreateTableAsSelect.class,
            command -> getSelectTableCommandId((CreateTableAsSelect) command))
          .put(InsertInto.class,
            command -> getInsertIntoCommandId((InsertInto) command))
          .put(TerminateQuery.class,
            command -> getTerminateCommandId((TerminateQuery) command))
          .put(DropTopic.class,
            command -> getDropTopicCommandId((DropTopic) command))
          .put(DropStream.class,
            command -> getDropStreamCommandId((DropStream) command))
          .put(DropTable.class,
            command -> getDropTableCommandId((DropTable) command))
          .put(RunScript.class,
            command -> new CommandId(CommandId.Type.STREAM, "RunScript", CommandId.Action.EXECUTE))
          .put(TerminateCluster.class,
            command -> new CommandId(Type.CLUSTER, "TerminateCluster", Action.TERMINATE))
          .build();

  public CommandIdAssigner() { }

  public CommandId getCommandId(final Statement command) {
    final CommandIdSupplier supplier = SUPPLIERS.get(command.getClass());
    if (supplier == null) {
      throw new RuntimeException(String.format(
          "Cannot assign command ID to statement of type %s",
          command.getClass().getCanonicalName()
      ));
    }
    return supplier.apply(command);
  }

  private static CommandId getTopicCommandId(final RegisterTopic registerTopic) {
    final String topicName = registerTopic.getName().toString();
    return new CommandId(CommandId.Type.TOPIC, topicName, CommandId.Action.CREATE);
  }

  private static CommandId getTopicStreamCommandId(final CreateStream createStream) {
    return getStreamCommandId(createStream.getName().toString());
  }

  private static CommandId getSelectStreamCommandId(
      final CreateStreamAsSelect createStreamAsSelect) {
    return getStreamCommandId(createStreamAsSelect.getName().toString());
  }

  private static CommandId getTopicTableCommandId(final CreateTable createTable) {
    return getTableCommandId(createTable.getName().toString());
  }

  private static CommandId getSelectTableCommandId(final CreateTableAsSelect createTableAsSelect) {
    return getTableCommandId(createTableAsSelect.getName().toString());
  }

  private static CommandId getInsertIntoCommandId(final InsertInto insertInto) {
    return  new CommandId(CommandId.Type.STREAM, insertInto.getTarget().toString(), CommandId.Action
        .CREATE);
  }

  private static CommandId getTerminateCommandId(final TerminateQuery terminateQuery) {
    return new CommandId(
        CommandId.Type.TERMINATE,
        terminateQuery.getQueryId().toString(),
        CommandId.Action.EXECUTE
    );
  }

  private static CommandId getDropTopicCommandId(final DropTopic dropTopicQuery) {
    return new CommandId(
        CommandId.Type.TOPIC,
        dropTopicQuery.getTopicName().getSuffix(),
        CommandId.Action.DROP
    );
  }

  private static CommandId getDropStreamCommandId(final DropStream dropStreamQuery) {
    return new CommandId(
        CommandId.Type.STREAM,
        dropStreamQuery.getName().getSuffix(),
        CommandId.Action.DROP
    );
  }

  private static CommandId getDropTableCommandId(final DropTable dropTableQuery) {
    return new CommandId(
        CommandId.Type.TABLE,
        dropTableQuery.getName().getSuffix(),
        CommandId.Action.DROP
    );
  }

  private static CommandId getStreamCommandId(final String streamName) {
    return getSourceCommandId(CommandId.Type.STREAM, streamName);
  }

  private static CommandId getTableCommandId(final String tableName) {
    return getSourceCommandId(CommandId.Type.TABLE, tableName);
  }

  private static CommandId getSourceCommandId(final CommandId.Type type, final String sourceName) {
    return new CommandId(type, sourceName, CommandId.Action.CREATE);
  }
}
