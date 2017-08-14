/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import java.util.Map;

import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.util.KafkaTopicClient;


public class CreateStreamCommand extends AbstractCreateStreamCommand {
  public CreateStreamCommand(CreateStream createStream, Map<String, Object> overriddenProperties,
                             KafkaTopicClient kafkaTopicClient) {
    super(createStream, overriddenProperties, kafkaTopicClient);
  }

  @Override
  public DDLCommandResult run(MetaStore metaStore) {
    if (registerTopicCommand != null) {
      registerTopicCommand.run(metaStore);
    }
    checkMetaData(metaStore, sourceName, topicName);
    KsqlStream ksqlStream = new KsqlStream(sourceName, schema,
        (keyColumnName.length() == 0) ? null :
            schema.field(keyColumnName),
        (timestampColumnName.length() == 0) ? null :
            schema.field(timestampColumnName),
        metaStore.getTopic(topicName));

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    metaStore.putSource(ksqlStream.cloneWithTimeKeyColumns());
    return new DDLCommandResult(true, "Stream created");
  }
}
