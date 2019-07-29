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

import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.parser.tree.DropStatement;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;

public class DropSourceCommand implements DdlCommand {

  private final String sourceName;
  private final boolean ifExists;
  private final DataSource.DataSourceType dataSourceType;

  public DropSourceCommand(
      final DropStatement statement,
      final DataSourceType dataSourceType
  ) {
    Objects.requireNonNull(statement);

    this.sourceName = statement.getName().getSuffix();
    this.ifExists = statement.getIfExists();
    this.dataSourceType = Objects.requireNonNull(dataSourceType, "dataSourceType");
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    final DataSource<?> dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      if (ifExists) {
        return new DdlCommandResult(true, "Source " + sourceName + " does not exist.");
      }
      throw new KsqlException("Source " + sourceName + " does not exist.");
    }

    if (dataSource.getDataSourceType() != dataSourceType) {
      throw new KsqlException(String.format(
          "Incompatible data source type is %s, but statement was DROP %s",
          dataSource.getDataSourceType() == DataSourceType.KSTREAM ? "STREAM" : "TABLE",
          dataSourceType == DataSourceType.KSTREAM ? "STREAM" : "TABLE"
      ));
    }

    metaStore.deleteSource(sourceName);

    return new DdlCommandResult(true,
        "Source " + sourceName + " (topic: " + dataSource.getKafkaTopicName() + ") was dropped.");
  }
}