/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.physical.GenericRow;

import java.util.ArrayList;
import java.util.List;

public class DropTopicCommand implements DDLCommand {

  private final String topicName;

  public DropTopicCommand(DropTopic dropTopic) {
    this.topicName = dropTopic.getTopicName().getSuffix();
  }

  @Override
  public List<GenericRow> run(MetaStore metaStore) {
    metaStore.deleteTopic(topicName);
    return new ArrayList<>();
  }
}
