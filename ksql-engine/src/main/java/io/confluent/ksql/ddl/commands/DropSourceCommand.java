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
import io.confluent.ksql.util.KsqlException;


public class DropSourceCommand implements DDLCommand {

  private final String sourceName;

  public DropSourceCommand(AbstractStreamDropStatement statement) {
    this.sourceName = statement.getName().getSuffix();
  }

  @Override
  public DDLCommandResult run(MetaStore metaStore) {
    StructuredDataSource dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      throw new KsqlException("Source " + sourceName + " does not exist.");
    }
    DropTopicCommand dropTopicCommand = new DropTopicCommand(
        dataSource.getKsqlTopic().getTopicName());
    dropTopicCommand.run(metaStore);
    metaStore.deleteSource(sourceName);
    return new DDLCommandResult(true, "Source " + sourceName +  " was dropped");
  }
}
