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
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlPreconditions;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.StringUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Base class of create table/stream command
 */
public abstract class AbstractCreateStreamCommand implements DDLCommand {

  String sourceName;
  String topicName;
  Schema schema;
  String keyColumnName;
  String timestampColumnName;
  boolean isWindowed;
  RegisterTopicCommand registerTopicCommand;
  KafkaTopicClient kafkaTopicClient;

  public AbstractCreateStreamCommand(final AbstractStreamCreateStatement statement,
                                     Map<String, Object> overriddenProperties,
                                     KafkaTopicClient kafkaTopicClient) {
    this.sourceName = statement.getName().getSuffix();
    this.topicName = this.sourceName;
    this.kafkaTopicClient = kafkaTopicClient;

    // TODO: get rid of toUpperCase in following code
    Map<String, Expression> properties = statement.getProperties();
    validateWithClause(properties.keySet());

    if (properties.containsKey(DdlConfig.TOPIC_NAME_PROPERTY) &&
        !properties.containsKey(DdlConfig.VALUE_FORMAT_PROPERTY)) {
      this.topicName = StringUtil.cleanQuotes(
          properties.get(DdlConfig.TOPIC_NAME_PROPERTY).toString().toUpperCase());

      checkTopicNameNotNull(properties);
    } else {
      this.registerTopicCommand = registerTopicFirst(properties, overriddenProperties);
    }


    this.schema = getStreamTableSchema(statement.getElements());


    this.keyColumnName = "";

    if (properties.containsKey(DdlConfig.KEY_NAME_PROPERTY)) {
      keyColumnName = properties.get(DdlConfig.KEY_NAME_PROPERTY)
          .toString().toUpperCase();

      keyColumnName = StringUtil.cleanQuotes(keyColumnName);
    }

    this.timestampColumnName = "";
    if (properties.containsKey(DdlConfig.TIMESTAMP_NAME_PROPERTY)) {
      timestampColumnName = properties.get(DdlConfig.TIMESTAMP_NAME_PROPERTY)
          .toString().toUpperCase();
      timestampColumnName = StringUtil.cleanQuotes(timestampColumnName);
      if (SchemaUtil.getFieldByName(schema, timestampColumnName)
          .get().schema().type() != Schema
          .Type.INT64) {
        throw new KsqlException("Timestamp column, " + timestampColumnName + ", should be LONG"
            + "(INT64).");
      }
    }


    this.isWindowed = false;
    if (properties.containsKey(DdlConfig.IS_WINDOWED_PROPERTY)) {
      String isWindowedProp = properties.get(DdlConfig.IS_WINDOWED_PROPERTY)
          .toString().toUpperCase();
      try {
        isWindowed = Boolean.parseBoolean(isWindowedProp);
      } catch (Exception e) {
        throw new KsqlException("isWindowed property is not set correctly: " + isWindowedProp);
      }
    }
  }

  private void checkTopicNameNotNull(Map<String, Expression> properties) {
    // TODO: move the check to grammar
    KsqlPreconditions.checkNotNull(
        properties.get(DdlConfig.TOPIC_NAME_PROPERTY),
        "Topic name should be set in WITH clause.");
  }

  private SchemaBuilder getStreamTableSchema(List<TableElement> tableElementList) {
    SchemaBuilder tableSchema = SchemaBuilder.struct();
    for (TableElement tableElement : tableElementList) {
      if (tableElement.getName().equalsIgnoreCase(SchemaUtil.ROWTIME_NAME) || tableElement.getName()
          .equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        throw new KsqlException(SchemaUtil.ROWTIME_NAME + "/" + SchemaUtil.ROWKEY_NAME + " are "
            + "reserved "
            + "token for "
            + "implicit "
            + "column."
            + " You cannot use them as a column name.");

      }
      tableSchema = tableSchema.field(tableElement.getName(), getKsqlType(tableElement.getType()));
    }

    return tableSchema;
  }

  //TODO: this needs to be moved to proper place to be accessible to everyone. Temporary!
  private Schema getKsqlType(final String sqlType) {
    switch (sqlType) {
      case "VARCHAR":
      case "STRING":
        return Schema.STRING_SCHEMA;
      case "BOOLEAN":
      case "BOOL":
        return Schema.BOOLEAN_SCHEMA;
      case "INTEGER":
      case "INT":
        return Schema.INT32_SCHEMA;
      case "BIGINT":
      case "LONG":
        return Schema.INT64_SCHEMA;
      case "DOUBLE":
        return Schema.FLOAT64_SCHEMA;
      default:
        return getKsqlComplexType(sqlType);
    }
  }

  private Schema getKsqlComplexType(final String sqlType) {
    if (sqlType.startsWith("ARRAY")) {
      return SchemaBuilder
          .array(getKsqlType(sqlType.substring("ARRAY".length() + 1, sqlType.length() - 1)));
    } else if (sqlType.startsWith("MAP")) {
      //TODO: For now only primitive data types for map are supported. Will have to add
      // nested types.
      String[] mapTypesStrs = sqlType.substring("MAP".length() + 1, sqlType.length() - 1)
          .trim().split(",");
      if (mapTypesStrs.length != 2) {
        throw new KsqlException("Map type is not defined correctly.: " + sqlType);
      }
      String keyType = mapTypesStrs[0].trim();
      String valueType = mapTypesStrs[1].trim();
      return SchemaBuilder.map(getKsqlType(keyType), getKsqlType(valueType));
    }
    throw new KsqlException("Unsupported type: " + sqlType);
  }

  protected void checkMetaData(MetaStore metaStore, String sourceName, String topicName) {
    // TODO: move the check to the runtime since it accesses metaStore
    KsqlPreconditions.checkArgument(
        metaStore.getSource(sourceName) == null,
        "Source already exists.");

    KsqlPreconditions.checkNotNull(
        metaStore.getTopic(topicName),
        String.format("The corresponding topic, %s, does not exist.", topicName));
  }

  protected RegisterTopicCommand registerTopicFirst(Map<String, Expression> properties,
                                    Map<String, Object> overriddenProperties) {
    if (properties.size() == 0) {
      throw new KsqlException("Create Stream/Table statement needs WITH clause.");
    }
    if (!properties.containsKey(DdlConfig.VALUE_FORMAT_PROPERTY)) {
      throw new KsqlException("Topic format(" + DdlConfig.VALUE_FORMAT_PROPERTY + ") should be set "
                              + "in WITH clause.");
    }
    if (!properties.containsKey(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY)) {
      throw new KsqlException("Corresponding kafka topic(" + DdlConfig.KAFKA_TOPIC_NAME_PROPERTY
                              + ") should be set in WITH clause.");
    }
    String kafkaTopicName = StringUtil.cleanQuotes(
        properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString());
    if (!kafkaTopicClient.isTopicExists(kafkaTopicName)) {
      throw new KsqlException("Kafka topic does not exist: " + kafkaTopicName);
    }
    return new RegisterTopicCommand(this.topicName, false, properties, overriddenProperties);
  }


  private void validateWithClause(Set<String> withClauseVariables) {

    Set<String> validSet = new HashSet<>();
    validSet.add(DdlConfig.VALUE_FORMAT_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.KEY_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.IS_WINDOWED_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.TIMESTAMP_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.STATE_STORE_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.TOPIC_NAME_PROPERTY.toUpperCase());


    for (String withVariable: withClauseVariables) {
      if (!validSet.contains(withVariable.toUpperCase())) {
        throw new KsqlException("Invalid config variable in the WITH clause: " + withVariable);
      }
    }
  }

}
