/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.serde.DataSource;
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
    // TODO: find a way to merge overriddenProperties
    this(registerTopic.getName().getSuffix(),
         registerTopic.isNotExists(),
         registerTopic.getProperties()
    );
  }

  RegisterTopicCommand(final String topicName, final boolean notExist,
                       final Map<String, Expression> properties) {
    this.topicName = topicName;
    // TODO: find a way to merge overriddenProperties
    enforceTopicProperties(properties);
    this.kafkaTopicName = StringUtil.cleanQuotes(
        properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString());
    final String serde = StringUtil.cleanQuotes(
        properties.get(DdlConfig.VALUE_FORMAT_PROPERTY).toString());
    this.topicSerDe = extractTopicSerDe(serde, properties);
    this.notExists = notExist;
  }

  private KsqlTopicSerDe extractTopicSerDe(
      final String serde, final Map<String, Expression> properties) {
    // TODO: Find a way to avoid calling toUpperCase() here;
    // if the property can be an unquoted identifier, then capitalization will have already happened
    if (!serde.equalsIgnoreCase(DataSource.AVRO_SERDE_NAME)
        && properties.containsKey(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME)) {
      throw new KsqlException(
              DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME + " is only valid for AVRO topics.");
    }
    switch (serde.toUpperCase()) {
      case DataSource.AVRO_SERDE_NAME:
        final Expression schemaFullNameExp =
                properties.get(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME);
        final String schemaFullName = schemaFullNameExp == null
                ? KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME :
                  StringUtil.cleanQuotes(schemaFullNameExp.toString());
        return new KsqlAvroTopicSerDe(schemaFullName);
      case DataSource.JSON_SERDE_NAME:
        return new KsqlJsonTopicSerDe();
      case DataSource.DELIMITED_SERDE_NAME:
        return new KsqlDelimitedTopicSerDe();
      default:
        throw new KsqlException("The specified topic serde is not supported.");
    }
  }

  private void enforceTopicProperties(final Map<String, Expression> properties) {
    if (properties.size() == 0) {
      throw new KsqlException("Register topic statement needs WITH clause.");
    }

    if (!properties.containsKey(DdlConfig.VALUE_FORMAT_PROPERTY)) {
      throw new KsqlException("Topic format(format) should be set in WITH clause.");
    }

    if (!properties.containsKey(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY)) {
      throw new KsqlException("Corresponding kafka topic should be set in WITH clause.");
    }
  }

  @Override
  public DdlCommandResult run(final MetaStore metaStore) {
    if (metaStore.getTopic(topicName) != null) {
      // Check IF NOT EXIST is set, if set, do not create topic if one exists.
      if (notExists) {
        return new DdlCommandResult(true,
                                    "Topic is not registered because it already registered"
                                          + ".");
      } else {
        throw new KsqlException("Topic already registered: " + topicName);
      }
    }

    final KsqlTopic ksqlTopic = new KsqlTopic(topicName, kafkaTopicName, topicSerDe, false);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    metaStore.putTopic(ksqlTopic);

    return new DdlCommandResult(true, "Topic registered");
  }
}
