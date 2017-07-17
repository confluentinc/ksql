/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DropTopic;


public class DropTopicCommand implements DDLCommand {

  private final String topicName;

  public DropTopicCommand(DropTopic dropTopic) {
    this.topicName = dropTopic.getTopicName().getSuffix();
  }

  @Override
  public DDLCommandResult run(MetaStore metaStore) {
    metaStore.deleteTopic(topicName);
    return new DDLCommandResult(true, "Topic " + topicName + " was dropped");
  }
}
