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

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.delimited.KsqlDelimitedTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.StringUtil;

import java.util.Map;

public class RegisterTopicCommand implements DdlCommand {
  private final String topicName;
  private final String kafkaTopicName;
  private final KsqlTopicSerDe topicSerDe;
  private final boolean notExists;

  public RegisterTopicCommand(RegisterTopic registerTopic) {
    // TODO: find a way to merge overriddenProperties
    this(registerTopic.getName().getSuffix(),
         registerTopic.isNotExists(),
         registerTopic.getProperties()
    );
  }

  RegisterTopicCommand(String topicName, boolean notExist,
                       Map<String, Expression> properties) {
    this.topicName = topicName;
    // TODO: find a way to merge overriddenProperties
    enforceTopicProperties(properties);
    this.kafkaTopicName = StringUtil.cleanQuotes(
        properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString());
    final String serde = StringUtil.cleanQuotes(
        properties.get(DdlConfig.VALUE_FORMAT_PROPERTY).toString());
    this.topicSerDe = extractTopicSerDe(serde);
    this.notExists = notExist;
  }

  private KsqlTopicSerDe extractTopicSerDe(String serde) {
    // TODO: Find a way to avoid calling toUpperCase() here;
    // if the property can be an unquoted identifier, then capitalization will have already happened
    switch (serde.toUpperCase()) {
      case DataSource.AVRO_SERDE_NAME:
        return new KsqlAvroTopicSerDe();
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
  public DdlCommandResult run(MetaStore metaStore, boolean isValidatePhase) {
    if (metaStore.getTopic(topicName) != null) {
      // Check IF NOT EXIST is set, if set, do not create topic if one exists.
      if (notExists) {
        return new DdlCommandResult(true,
                                    "Topic is not registered because it already registered"
                                          + ".");
      } else {
        throw new KsqlException("Topic already registered.");
      }
    }

    KsqlTopic ksqlTopic = new KsqlTopic(topicName, kafkaTopicName, topicSerDe);

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    metaStore.putTopic(ksqlTopic);

    return new DdlCommandResult(true, "Topic registered");
  }
}
