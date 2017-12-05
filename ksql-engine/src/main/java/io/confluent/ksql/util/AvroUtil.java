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

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.exception.ExceptionUtil;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.serde.DataSource;

public class AvroUtil {

  private static final Logger log = LoggerFactory.getLogger(AvroUtil.class);

  public static synchronized Pair<AbstractStreamCreateStatement, String> checkAndSetAvroSchema(AbstractStreamCreateStatement
           abstractStreamCreateStatement,
       Map<String, Object> streamsProperties, KsqlConfig engineKsqlConfig) {
    Map<String,Expression> ddlProperties = abstractStreamCreateStatement.getProperties();
    if (!ddlProperties.containsKey(DdlConfig.VALUE_FORMAT_PROPERTY)) {
      throw
          new KsqlException(String.format("%s should be set in WITH clause of CREATE "
                                          + "STRAEM/TABLE statement.", DdlConfig.VALUE_FORMAT_PROPERTY));
    }
    final String serde = StringUtil.cleanQuotes(
        ddlProperties.get(DdlConfig.VALUE_FORMAT_PROPERTY).toString());
    if (!serde.equalsIgnoreCase(DataSource.AVRO_SERDE_NAME)) {
      return new Pair<>(abstractStreamCreateStatement, null);
    }

    String kafkaTopicName = StringUtil.cleanQuotes(
        ddlProperties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString());
    KsqlConfig ksqlConfig = engineKsqlConfig.cloneWithPropertyOverwrite(streamsProperties);
    SchemaRegistryClient
        schemaRegistryClient = SchemaRegistryClientFactory.getSchemaRegistryClient(ksqlConfig
                                                                                       .getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY));
    try {
      SchemaMetadata schemaMetadata;
      if (abstractStreamCreateStatement.getProperties().containsKey(KsqlConstants.AVRO_SCHEMA_ID)) {
        int schemaId = -1;
        try {
          schemaId = Integer.parseInt(StringUtil.cleanQuotes(abstractStreamCreateStatement.getProperties().get(KsqlConstants.AVRO_SCHEMA_ID).toString()));
        } catch (NumberFormatException e) {
          throw new KsqlException(String.format("Invalid schema id property: %s.",
                                                abstractStreamCreateStatement.getProperties().get(KsqlConstants.AVRO_SCHEMA_ID).toString()));
        }
        schemaMetadata = schemaRegistryClient.getSchemaMetadata(
            kafkaTopicName +KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schemaId);
      } else {
        schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(kafkaTopicName +
                                                                      KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
      }

      String avroSchemaString = schemaMetadata.getSchema();
      streamsProperties.put(DdlConfig.AVRO_SCHEMA, avroSchemaString);
      Schema schema = SerDeUtil.getSchemaFromAvro(avroSchemaString);
      if (abstractStreamCreateStatement.getElements().isEmpty()) {
        abstractStreamCreateStatement = addAvroFields(abstractStreamCreateStatement, schema,
                                                      schemaMetadata.getId());
        return new Pair<>(abstractStreamCreateStatement, SqlFormatter.formatSql(abstractStreamCreateStatement));
      } else {
        return new Pair<>(abstractStreamCreateStatement, null);
      }
    } catch (Exception e) {
      String errorMessage = String.format(" Could not fetch the AVRO schema from schema "
                                          + "registry (url: %s). $s ",
                                          ksqlConfig.get(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY).toString(), e.getMessage());
      throw new KsqlException(errorMessage);
    }
  }

  private static AbstractStreamCreateStatement addAvroFields(AbstractStreamCreateStatement
                                                                             abstractStreamCreateStatement, Schema
                                                                             schema, int schemaId) {
    List<TableElement> elements = new ArrayList<>();
    for (Field field: schema.fields()) {
      TableElement tableElement = new TableElement(field.name(), SchemaUtil
          .getSQLTypeName(field.schema()));
      elements.add(tableElement);
    }
    StringLiteral schemaIdLiteral = new StringLiteral(String.format("%d", schemaId));
    Map<String, Expression> properties;
    if (abstractStreamCreateStatement.getProperties().containsKey(KsqlConstants.AVRO_SCHEMA_ID)) {
      properties = abstractStreamCreateStatement.getProperties();
    } else {
      properties = new HashMap<>();
      properties.putAll(abstractStreamCreateStatement.getProperties());
      properties.put(KsqlConstants.AVRO_SCHEMA_ID, schemaIdLiteral);
    }


    if (abstractStreamCreateStatement instanceof CreateStream) {
      return new CreateStream(abstractStreamCreateStatement.getName(), elements,
                              ((CreateStream) abstractStreamCreateStatement).isNotExists(),
                              properties);
    } else if (abstractStreamCreateStatement instanceof CreateTable) {
      return new CreateTable(abstractStreamCreateStatement.getName(), elements,
                             ((CreateStream) abstractStreamCreateStatement).isNotExists(),
                             properties);
    } else {
      throw new KsqlException(String.format("Invalid AbstractStreamCreateStatement: %s",
                                            SqlFormatter.formatSql(abstractStreamCreateStatement)));
    }
  }


  public static synchronized void validatePersistantQueryResults(PersistentQueryMetadata
                                                                     persistentQueryMetadata,
                                                                 Map<String, Object> streamsProperties,
                                                                 KsqlConfig engineKsqlConfig) {
    if (persistentQueryMetadata.getResultTopic().getKsqlTopicSerDe().getSerDe() == DataSource.DataSourceSerDe.AVRO) {
      String avroSchemaString = new MetastoreUtil().buildAvroSchema(persistentQueryMetadata
                                                                        .getResultSchema(),
                                                                    persistentQueryMetadata
                                                                        .getResultTopic().getName());
      boolean isValidSchema = AvroUtil.isValidAvroSchemaForTopic(persistentQueryMetadata.getResultTopic()
                                                                     .getTopicName(),
                                                                 avroSchemaString, streamsProperties,
                                                                 engineKsqlConfig);
      if (isValidSchema) {
        throw new KsqlException(String.format("Cannot register avro schema for %s since it is "
                                              + "not valid for schema registry.", persistentQueryMetadata
                                                  .getResultTopic().getKafkaTopicName()));
      }
    }
  }


  private static synchronized boolean isValidAvroSchemaForTopic(String topicName, String
      avroSchemaString, Map<String, Object> streamsProperties, KsqlConfig engineKsqlConfig) {
    KsqlConfig ksqlConfig = engineKsqlConfig.cloneWithPropertyOverwrite(streamsProperties);
    SchemaRegistryClient
        schemaRegistryClient = SchemaRegistryClientFactory.getSchemaRegistryClient(ksqlConfig
                                                                                       .getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY));

    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = parser.parse(avroSchemaString);
    try {
      return schemaRegistryClient.testCompatibility(topicName, avroSchema);
    } catch (IOException e) {
      log.error(ExceptionUtil.stackTraceToString(e));
      throw new KsqlException(String.format("Could not check Schema compatibility: %s", e
          .getMessage()));
    } catch (RestClientException e) {
      log.error(ExceptionUtil.stackTraceToString(e));
      throw new KsqlException(String.format("Could not connect to Schema Registry service: %s", e
          .getMessage()));
    }
  }

}
