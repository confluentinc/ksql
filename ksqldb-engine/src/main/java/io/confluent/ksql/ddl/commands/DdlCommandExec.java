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

import io.confluent.ksql.execution.ddl.commands.AlterSourceCommand;
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommandResult;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.ddl.commands.RegisterTypeCommand;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;

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
  public DdlCommandResult execute(
      final String sql,
      final DdlCommand ddlCommand,
      final boolean withQuery) {
    return new Executor(sql, withQuery).execute(ddlCommand);
  }

  private final class Executor implements io.confluent.ksql.execution.ddl.commands.Executor {
    private final String sql;
    private final boolean withQuery;

    private Executor(final String sql, final boolean withQuery) {
      this.sql = Objects.requireNonNull(sql, "sql");
      this.withQuery = withQuery;
    }

    @Override
    public DdlCommandResult executeCreateStream(final CreateStreamCommand createStream) {
      final KsqlStream<?> ksqlStream = new KsqlStream<>(
          sql,
          createStream.getSourceName(),
          createStream.getSchema(),
          createStream.getFormats().getOptions(),
          createStream.getTimestampColumn(),
          withQuery,
          getKsqlTopic(createStream)
      );
      metaStore.putSource(ksqlStream, createStream.isOrReplace());
      return new DdlCommandResult(true, "Stream created");
    }

    @Override
    public DdlCommandResult executeCreateTable(final CreateTableCommand createTable) {
      final KsqlTable<?> ksqlTable = new KsqlTable<>(
          sql,
          createTable.getSourceName(),
          createTable.getSchema(),
          createTable.getFormats().getOptions(),
          createTable.getTimestampColumn(),
          withQuery,
          getKsqlTopic(createTable)
      );
      metaStore.putSource(ksqlTable, createTable.isOrReplace());
      return new DdlCommandResult(true, "Table created");
    }

    @Override
    public DdlCommandResult executeDropSource(final DropSourceCommand dropSource) {
      final SourceName sourceName = dropSource.getSourceName();
      final DataSource dataSource = metaStore.getSource(sourceName);
      if (dataSource == null) {
        return new DdlCommandResult(true, "Source " + sourceName + " does not exist.");
      }
      metaStore.deleteSource(sourceName);
      return new DdlCommandResult(true,
          "Source " + sourceName + " (topic: " + dataSource.getKafkaTopicName() + ") was dropped.");
    }

    @Override
    public DdlCommandResult executeRegisterType(final RegisterTypeCommand registerType) {
      final String name = registerType.getTypeName();
      final SqlType type = registerType.getType();
      final boolean wasRegistered = metaStore.registerType(name, type);
      return wasRegistered
          ? new DdlCommandResult(
              true,
              "Registered custom type with name '" + name + "' and SQL type " + type)
          : new DdlCommandResult(
              true,
              name + " is already registered with type " + metaStore.resolveType(name).get());
    }

    @Override
    public DdlCommandResult executeDropType(final DropTypeCommand dropType) {
      final String typeName = dropType.getTypeName();
      final boolean wasDeleted = metaStore.deleteType(typeName);
      return wasDeleted
          ? new DdlCommandResult(true, "Dropped type '" + typeName + "'")
          : new DdlCommandResult(true, "Type '" + typeName + "' does not exist");
    }

    @Override
    public DdlCommandResult executeAlterSource(final AlterSourceCommand alterSource) {
      final DataSource dataSource = metaStore.getSource(alterSource.getSourceName());

      if (dataSource == null) {
        throw new KsqlException(
            "Source " + alterSource.getSourceName().text()
                + " does not exist."
        );
      }

      if (!dataSource.getDataSourceType().getKsqlType().equals(alterSource.getKsqlType())) {
        throw new KsqlException(String.format(
            "Incompatible data source type is %s, but statement was ALTER %s",
            dataSource.getDataSourceType().getKsqlType(),
            alterSource.getKsqlType()
        ));
      }

      if (dataSource.isCasTarget()) {
        throw new KsqlException(String.format(
            "ALTER command is not supported for CREATE ... AS statements."
        ));
      }

      final LogicalSchema newSchema = dataSource.getSchema()
          .asBuilder()
          .valueColumns(alterSource.getNewColumns().columns())
          .build();

      if (alterSource.getKsqlType().equals(DataSourceType.KSTREAM.getKsqlType())) {
        return alterStream(dataSource, newSchema);
      } else {
        return alterTable(dataSource, newSchema);
      }
    }

    private DdlCommandResult alterStream(
        final DataSource dataSource,
        final LogicalSchema newSchema
    ) {
      final KsqlStream<?> ksqlStream = new KsqlStream<>(
          dataSource.getSqlExpression().concat(sql),
          dataSource.getName(),
          newSchema,
          dataSource.getSerdeOptions(),
          dataSource.getTimestampColumn(),
          dataSource.isCasTarget(),
          dataSource.getKsqlTopic()
      );
      metaStore.putSource(ksqlStream, true);
      return new DdlCommandResult(
          true,
          "Stream " + dataSource.getName().text() + " altered"
      );
    }

    private DdlCommandResult alterTable(
        final DataSource dataSource,
        final LogicalSchema newSchema
    ) {
      final KsqlTable<?> ksqlTable = new KsqlTable<>(
          dataSource.getSqlExpression().concat(sql),
          dataSource.getName(),
          newSchema,
          dataSource.getSerdeOptions(),
          dataSource.getTimestampColumn(),
          dataSource.isCasTarget(),
          dataSource.getKsqlTopic()
      );
      metaStore.putSource(ksqlTable, true);
      return new DdlCommandResult(
          true,
          "Stream " + dataSource.getName().text() + " altered"
      );
    }
  }

  private static KsqlTopic getKsqlTopic(final CreateSourceCommand createSource) {
    return new KsqlTopic(
        createSource.getTopicName(),
        KeyFormat.of(createSource.getFormats().getKeyFormat(), createSource.getWindowInfo()),
        ValueFormat.of(createSource.getFormats().getValueFormat())
    );
  }
}
