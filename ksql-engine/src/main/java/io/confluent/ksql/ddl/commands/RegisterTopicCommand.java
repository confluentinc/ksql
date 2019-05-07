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

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.delimited.KsqlDelimitedTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.StringUtil;
import java.util.Map;

public class RegisterTopicCommand implements DdlCommand {
  private final String topicName;
  private final String kafkaTopicName;
  private final KsqlTopicSerDe topicSerDe;
  private final boolean notExists;

  public RegisterTopicCommand(final RegisterTopic registerTopic) {
    this(registerTopic.getName().getSuffix(),
         registerTopic.isNotExists(),
         registerTopic.getProperties()
    );
  }

  RegisterTopicCommand(final String topicName, final boolean notExist,
                       final Map<String, Literal> properties) {
    this.topicName = topicName;
    enforceTopicProperties(properties);
    this.kafkaTopicName = StringUtil.cleanQuotes(
        properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString());
    final Format format = Format
        .of(StringUtil.cleanQuotes(properties.get(DdlConfig.VALUE_FORMAT_PROPERTY).toString()));
    this.topicSerDe = extractTopicSerDe(format, properties);
    this.notExists = notExist;
  }

  private static KsqlTopicSerDe extractTopicSerDe(
      final Format format,
      final Map<String, Literal> properties
  ) {
    if (properties.containsKey(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME)
        && format != Format.AVRO
    ) {
      throw new KsqlException(
              DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME + " is only valid for AVRO topics.");
    }

    switch (format) {
      case AVRO:
        final Expression schemaFullNameExp =
                properties.get(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME);
        final String schemaFullName = schemaFullNameExp == null
                ? KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME :
                  StringUtil.cleanQuotes(schemaFullNameExp.toString());
        return new KsqlAvroTopicSerDe(schemaFullName);

      case JSON:
        return new KsqlJsonTopicSerDe();

      case DELIMITED:
        return new KsqlDelimitedTopicSerDe();

      default:
        throw new KsqlException("The specified topic serde is not supported.");
    }
  }

  private static void enforceTopicProperties(final Map<String, ?> properties) {
    if (!properties.containsKey(DdlConfig.VALUE_FORMAT_PROPERTY)) {
      throw new KsqlException("Topic format("
          + DdlConfig.VALUE_FORMAT_PROPERTY + ") should be set in WITH clause.");
    }

    if (!properties.containsKey(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY)) {
      throw new KsqlException("Corresponding Kafka topic ("
          + DdlConfig.KAFKA_TOPIC_NAME_PROPERTY + ") should be set in WITH clause.");
    }
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    if (metaStore.getTopic(topicName) != null) {
      // Check IF NOT EXIST is set, if set, do not create topic if one exists.
      if (notExists) {
        return new DdlCommandResult(true, "Topic already registered.");
      } else {
        final String sourceType = getSourceType(metaStore);
        final String errorMessage =
            String.format("%s with name '%s' already exists", sourceType, topicName);
        throw new KsqlException(errorMessage);
      }
    }

    final KsqlTopic ksqlTopic = new KsqlTopic(topicName, kafkaTopicName, topicSerDe, false);

    metaStore.putTopic(ksqlTopic);

    return new DdlCommandResult(true, "Topic registered");
  }

  private String getSourceType(final MetaStore metaStore) {
    final DataSource<?> source = metaStore.getSource(topicName);
    if (source == null) {
      return "A topic";
    }

    switch (source.getDataSourceType()) {
      case KSTREAM:
        return "A stream";

      case KTABLE:
        return "A table";

      default:
        return "An entity";
    }
  }
}
