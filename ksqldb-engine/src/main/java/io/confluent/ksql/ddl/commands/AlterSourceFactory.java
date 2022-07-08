/*
 * Copyright 2020 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.execution.ddl.commands.AlterSourceCommand;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.stream.Collectors;

public class AlterSourceFactory {
  private final MetaStore metaStore;

  @VisibleForTesting
  public AlterSourceFactory(final MetaStore metaStore) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
  }

  public AlterSourceCommand create(final AlterSource statement) {
    final DataSource dataSource = metaStore.getSource(statement.getName());
    final String dataSourceType = statement.getDataSourceType().getKsqlType();

    if (dataSource != null && dataSource.isSource()) {
      throw new KsqlException(
          String.format("Cannot alter %s '%s': ALTER operations are not supported on source %s.",
              dataSourceType.toLowerCase(),
              statement.getName().text(),
              dataSourceType.toLowerCase() + "s"));
    }

    final List<Column> newColumns = statement
        .getAlterOptions()
        .stream()
        .map(
            alterOption -> Column.of(
                ColumnName.of(alterOption.getColumnName()),
                alterOption.getType().getSqlType(),
                Namespace.VALUE,
                0))
        .collect(Collectors.toList());

    return new AlterSourceCommand(
        statement.getName(),
        dataSourceType,
        newColumns
    );
  }
}
