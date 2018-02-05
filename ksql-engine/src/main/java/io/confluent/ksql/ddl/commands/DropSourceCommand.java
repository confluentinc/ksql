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

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.AbstractStreamDropStatement;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KsqlException;


public class DropSourceCommand implements DDLCommand {

  private final String sourceName;
  private final DataSource.DataSourceType dataSourceType;
  private final MetaStore metaStore;

  public DropSourceCommand(
      final AbstractStreamDropStatement statement,
      final DataSource.DataSourceType dataSourceType,
      final MetaStore metaStore) {

    this.sourceName = statement.getName().getSuffix();
    this.dataSourceType = dataSourceType;
    this.metaStore = metaStore;
  }

  @Override
  public DDLCommandResult run(MetaStore metaStore) {
    StructuredDataSource dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      throw new KsqlException("Source " + sourceName + " does not exist.");
    }
    if (dataSource.getDataSourceType() != dataSourceType) {
      throw new KsqlException(String.format(
          "Incompatible data source type is %s, but statement was DROP %s",
          dataSource.getDataSourceType() == DataSource.DataSourceType.KSTREAM ? "STREAM" : "TABLE",
          dataSourceType == DataSource.DataSourceType.KSTREAM ? "STREAM" : "TABLE"
      ));
    }
    metaStore.deleteSource(sourceName);
    DropTopicCommand dropTopicCommand = new DropTopicCommand(
        dataSource.getKsqlTopic().getTopicName());
    dropTopicCommand.run(metaStore);
    return new DDLCommandResult(true, "Source " + sourceName +  " was dropped");
  }
}
