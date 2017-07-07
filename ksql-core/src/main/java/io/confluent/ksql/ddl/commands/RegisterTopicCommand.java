/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.delimited.KsqlDelimitedTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.StringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RegisterTopicCommand implements DDLCommand {
  private final String topicName;
  private final String kafkaTopicName;
  private final KsqlTopicSerDe topicSerDe;
  private final boolean notExists;

  public RegisterTopicCommand(RegisterTopic registerTopic) {
    this(registerTopic, Collections.emptyMap());
  }

  public RegisterTopicCommand(RegisterTopic registerTopic,
                              Map<String, Object> overriddenProperties) {
    // TODO: find a way to merge overriddenProperties
    enforceTopicProperties(registerTopic.getProperties());

    final String serde = StringUtil.cleanQuotes(
        registerTopic.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY).toString());
    this.topicSerDe = extractTopicSerDe(overriddenProperties, serde);

    this.topicName = registerTopic.getName().getSuffix();
    this.kafkaTopicName = StringUtil.cleanQuotes(
        registerTopic.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString());
    this.notExists = registerTopic.isNotExists();
  }

  private KsqlTopicSerDe extractTopicSerDe(Map<String, Object> overriddenProperties, String serde) {
    // TODO: Find a way to avoid calling toUpperCase() here;
    // if the property can be an unquoted identifier, then capitalization will have already happened
    switch (serde.toUpperCase()) {
      case DataSource.AVRO_SERDE_NAME:
        if (!overriddenProperties.containsKey(DdlConfig.AVRO_SCHEMA)) {
          throw new KsqlException("Avro schema file path should be set for avro topics.");
        }
        String avroSchema = overriddenProperties.get(DdlConfig.AVRO_SCHEMA).toString();
        return new KsqlAvroTopicSerDe(avroSchema);
      case DataSource.JSON_SERDE_NAME:
        return new KsqlJsonTopicSerDe(null);
      case DataSource.DELIMITED_SERDE_NAME:
        return new KsqlDelimitedTopicSerDe();
      default:
        throw new KsqlException("The specified topic serde is not supported.");
    }
  }

  private void enforceTopicProperties(final Map<String, Expression> properties) {
    if (properties.size() == 0) {
      throw new KsqlException("Create topic statement needs WITH clause.");
    }

    if (properties.get(DdlConfig.VALUE_FORMAT_PROPERTY) == null) {
      throw new KsqlException("Topic format(format) should be set in WITH clause.");
    }

    if (properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY) == null) {
      throw new KsqlException("Corresponding kafka topic should be set in WITH clause.");
    }
  }

  @Override
  public List<GenericRow> run(MetaStore metaStore) {
    if (metaStore.getTopic(topicName) != null) {
      if (notExists) {
        System.out.println("Topic already exists."); // TODO: remove syso
      } else {
        throw new KsqlException("Topic already exists.");
      }
      return null;
    }

    KsqlTopic ksqlTopic = new KsqlTopic(topicName, kafkaTopicName, topicSerDe);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    metaStore.putTopic(ksqlTopic);

    return new ArrayList<>();
  }
}
