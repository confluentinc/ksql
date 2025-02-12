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
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.parser.DropType;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.parser.tree.AlterSystemProperty;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.PauseQuery;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.ResumeQuery;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandId.Action;
import io.confluent.ksql.rest.entity.CommandId.Type;
import io.confluent.ksql.rest.util.TerminateCluster;
import java.util.Map;
import java.util.UUID;

public class CommandIdAssigner {

  interface CommandIdSupplier {
    CommandId apply(Statement command);
  }

  private static final Map<Class<? extends Statement>, CommandIdSupplier> SUPPLIERS =
      ImmutableMap.<Class<? extends Statement>, CommandIdSupplier>builder()
          .put(CreateStream.class,
              command -> getTopicStreamCommandId((CreateStream) command))
          .put(CreateTable.class,
              command -> getTopicTableCommandId((CreateTable) command))
          .put(CreateStreamAsSelect.class,
              command -> getSelectStreamCommandId((CreateStreamAsSelect) command))
          .put(CreateTableAsSelect.class,
              command -> getSelectTableCommandId((CreateTableAsSelect) command))
          .put(RegisterType.class,
              command -> getRegisterTypeCommandId((RegisterType) command))
          .put(DropType.class,
              command -> getDropTypeCommandId((DropType) command))
          .put(InsertInto.class,
              command -> getInsertIntoCommandId((InsertInto) command))
          .put(PauseQuery.class,
              command -> getPauseCommandId((PauseQuery) command))
          .put(ResumeQuery.class,
              command -> getResumeCommandId((ResumeQuery) command))
          .put(TerminateQuery.class,
              command -> getTerminateCommandId((TerminateQuery) command))
          .put(DropStream.class,
              command -> getDropStreamCommandId((DropStream) command))
          .put(DropTable.class,
              command -> getDropTableCommandId((DropTable) command))
          .put(TerminateCluster.class,
              command -> new CommandId(Type.CLUSTER, "TerminateCluster", Action.TERMINATE))
          .put(AlterSource.class, command -> getAlterSourceCommandId((AlterSource) command))
          .put(AlterSystemProperty.class, command
              -> getAlterSystemCommandId((AlterSystemProperty) command))
          .build();

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
    return new CommandId(CommandId.Type.STREAM, insertInto.getTarget().toString(),
        CommandId.Action.CREATE);
  }

  private static CommandId getRegisterTypeCommandId(final RegisterType registerType) {
    return new CommandId(CommandId.Type.TYPE, registerType.getName(), Action.CREATE);
  }

  private static CommandId getDropTypeCommandId(final DropType dropType) {
    return new CommandId(CommandId.Type.TYPE, dropType.getTypeName(), Action.DROP);
  }

  private static CommandId getPauseCommandId(final PauseQuery pauseQuery) {
    return new CommandId(
        CommandId.Type.PAUSE,
        pauseQuery.getQueryId().map(QueryId::toString).orElse(PauseQuery.ALL_QUERIES),
        CommandId.Action.EXECUTE
    );
  }

  private static CommandId getResumeCommandId(final ResumeQuery resumeQuery) {
    return new CommandId(
        CommandId.Type.RESUME,
        resumeQuery.getQueryId().map(QueryId::toString).orElse(ResumeQuery.ALL_QUERIES),
        CommandId.Action.EXECUTE
    );
  }

  private static CommandId getTerminateCommandId(final TerminateQuery terminateQuery) {
    return new CommandId(
        CommandId.Type.TERMINATE,
        terminateQuery.getQueryId().map(QueryId::toString).orElse(TerminateQuery.ALL_QUERIES),
        CommandId.Action.EXECUTE
    );
  }

  private static CommandId getDropStreamCommandId(final DropStream dropStreamQuery) {
    return new CommandId(
        CommandId.Type.STREAM,
        dropStreamQuery.getName().text(),
        CommandId.Action.DROP
    );
  }

  private static CommandId getDropTableCommandId(final DropTable dropTableQuery) {
    return new CommandId(
        CommandId.Type.TABLE,
        dropTableQuery.getName().text(),
        CommandId.Action.DROP
    );
  }

  private static CommandId getAlterSourceCommandId(final AlterSource alterSource) {
    return new CommandId(
        alterSource.getDataSourceType() == DataSourceType.KSTREAM ? Type.STREAM : Type.TABLE,
        alterSource.getName().text(),
        Action.ALTER
    );
  }

  private static CommandId getAlterSystemCommandId(final AlterSystemProperty alterSystemProperty) {
    return new CommandId(
        Type.CLUSTER,
        UUID.randomUUID().toString(),
        Action.ALTER
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
