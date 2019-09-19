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

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommandResult;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.execution.ddl.commands.RegisterTypeCommand;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Optional;

/**
 * Execute DDL Commands
 */
public class DdlCommandExec {

  private final MutableMetaStore metaStore;

  public DdlCommandExec(final MutableMetaStore metaStore) {
    this.metaStore = metaStore;
  }

  /**
   * execute on metaStore
   */
  public DdlCommandResult execute(final DdlCommand ddlCommand) {
    return new Executor().execute(ddlCommand);
  }

  private class Executor implements io.confluent.ksql.execution.ddl.commands.Executor {
    @Override
    public DdlCommandResult executeCreateStream(final CreateStreamCommand createStream) {
      final KsqlStream<?> ksqlStream = new KsqlStream<>(
          createStream.getSqlExpression(),
          createStream.getSourceName(),
          createStream.getSchema(),
          createStream.getSerdeOptions(),
          getKeyField(createStream.getKeyField(), createStream.getSchema()),
          createStream.getTimestampExtractionPolicy(),
          createStream.getTopic()
      );
      metaStore.putSource(ksqlStream);
      return new DdlCommandResult(true, "Stream created");
    }

    @Override
    public DdlCommandResult executeCreateTable(final CreateTableCommand createTable) {
      final KsqlTable<?> ksqlTable = new KsqlTable<>(
          createTable.getSqlExpression(),
          createTable.getSourceName(),
          createTable.getSchema(),
          createTable.getSerdeOptions(),
          getKeyField(createTable.getKeyField(), createTable.getSchema()),
          createTable.getTimestampExtractionPolicy(),
          createTable.getTopic()
      );
      metaStore.putSource(ksqlTable);
      return new DdlCommandResult(true, "Table created");
    }

    @Override
    public DdlCommandResult executeDropSource(final DropSourceCommand dropSource) {
      final SourceName sourceName = dropSource.getSourceName();
      final DataSource<?> dataSource = metaStore.getSource(sourceName);
      if (dataSource == null) {
        return new DdlCommandResult(true, "Source " + sourceName + " does not exist.");
      }
      metaStore.deleteSource(sourceName);
      return new DdlCommandResult(true,
          "Source " + sourceName + " (topic: " + dataSource.getKafkaTopicName() + ") was dropped.");
    }

    @Override
    public DdlCommandResult executeRegisterType(final RegisterTypeCommand registerType) {
      final String name = registerType.getName();
      final SqlType type = registerType.getType();
      metaStore.registerType(name, type);
      return new DdlCommandResult(
          true,
          "Registered custom type with name '" + name + "' and SQL type " + type
      );
    }

    @Override
    public DdlCommandResult executeDropType(final DropTypeCommand dropType) {
      final String typeName = dropType.getTypeName();
      final boolean wasDeleted = metaStore.deleteType(typeName);
      return wasDeleted
          ? new DdlCommandResult(true, "Dropped type '" + typeName + "'")
          : new DdlCommandResult(true, "Type '" + typeName + "' does not exist");
    }

    private KeyField getKeyField(
        final Optional<ColumnName> keyFieldName,
        final LogicalSchema schema
    ) {
      if (keyFieldName.isPresent()) {
        final Column keyColumn = schema.findValueColumn(keyFieldName.get())
            .orElseThrow(() -> new IllegalStateException(
                "The KEY column set in the WITH clause does not exist in the schema: '"
                    + keyFieldName + "'"
            ));
        return KeyField.of(keyFieldName.get(), keyColumn);
      } else {
        return KeyField.none();
      }
    }
  }
}
