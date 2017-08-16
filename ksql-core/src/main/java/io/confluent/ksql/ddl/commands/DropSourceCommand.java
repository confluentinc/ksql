/**
 * Copyright 2017 Confluent Inc.
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
