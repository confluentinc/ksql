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
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.serde.DataSource;

public class AvroUtil {

  private static final Logger log = LoggerFactory.getLogger(AvroUtil.class);

  public Pair<AbstractStreamCreateStatement, String> checkAndSetAvroSchema(
      final AbstractStreamCreateStatement abstractStreamCreateStatement,
      final Map<String, Object> streamsProperties,
      final SchemaRegistryClient schemaRegistryClient
  ) {

    Map<String, Expression> ddlProperties = abstractStreamCreateStatement.getProperties();
    if (!ddlProperties.containsKey(DdlConfig.VALUE_FORMAT_PROPERTY)) {
      throw new KsqlException(String.format(
          "%s should be set in WITH clause of CREATE STREAM/TABLE statement.",
          DdlConfig.VALUE_FORMAT_PROPERTY
      )
      );
    }
    final String serde = StringUtil.cleanQuotes(
        ddlProperties.get(DdlConfig.VALUE_FORMAT_PROPERTY).toString()
    );
    if (!serde.equalsIgnoreCase(DataSource.AVRO_SERDE_NAME)) {
      return new Pair<>(abstractStreamCreateStatement, null);
    }

    String kafkaTopicName = StringUtil.cleanQuotes(
        ddlProperties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString()
    );
    try {
      // If the schema is not specified infer it from the Avro schema in Schema Registry.
      if (abstractStreamCreateStatement.getElements().isEmpty()) {
        SchemaMetadata schemaMetadata = fetchSchemaMetadata(
            abstractStreamCreateStatement,
            schemaRegistryClient,
            kafkaTopicName
        );

        String avroSchemaString = schemaMetadata.getSchema();
        streamsProperties.put(DdlConfig.AVRO_SCHEMA, avroSchemaString);
        Schema schema = SerDeUtil.getSchemaFromAvro(avroSchemaString);
        AbstractStreamCreateStatement abstractStreamCreateStatementCopy = addAvroFields(
            abstractStreamCreateStatement,
            schema,
            schemaMetadata.getId()
        );
        return new Pair<>(
            abstractStreamCreateStatementCopy,
            SqlFormatter.formatSql(abstractStreamCreateStatementCopy)
        );
      } else {
        return new Pair<>(abstractStreamCreateStatement, null);
      }
    } catch (Exception e) {
      String errorMessage = String.format(
          " Unable to verify the AVRO schema is compatible with KSQL. %s ",
          e.getMessage()
      );
      throw new KsqlException(errorMessage);
    }
  }

  private SchemaMetadata fetchSchemaMetadata(
      AbstractStreamCreateStatement abstractStreamCreateStatement,
      SchemaRegistryClient schemaRegistryClient,
      String kafkaTopicName
  ) throws IOException, RestClientException {

    if (abstractStreamCreateStatement.getProperties().containsKey(KsqlConstants.AVRO_SCHEMA_ID)) {
      int schemaId;
      try {
        schemaId = Integer.parseInt(
            StringUtil.cleanQuotes(
                abstractStreamCreateStatement
                    .getProperties()
                    .get(KsqlConstants.AVRO_SCHEMA_ID)
                    .toString()
            )
        );
      } catch (NumberFormatException e) {
        throw new KsqlException(String.format(
            "Invalid schema id property: %s.",
            abstractStreamCreateStatement
                .getProperties()
                .get(KsqlConstants.AVRO_SCHEMA_ID)
                .toString()
        ));
      }
      return schemaRegistryClient.getSchemaMetadata(
          kafkaTopicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX,
          schemaId
      );
    } else {
      return schemaRegistryClient.getLatestSchemaMetadata(
          kafkaTopicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
      );
    }
  }

  private AbstractStreamCreateStatement addAvroFields(
      final AbstractStreamCreateStatement abstractStreamCreateStatement,
      final Schema schema,
      int schemaId
  ) {
    List<TableElement> elements = new ArrayList<>();
    for (Field field : schema.fields()) {
      TableElement tableElement = new TableElement(field.name().toUpperCase(), SchemaUtil
          .getSqlTypeName(field.schema()));
      elements.add(tableElement);
    }
    StringLiteral schemaIdLiteral = new StringLiteral(String.format("%d", schemaId));
    Map<String, Expression> properties =
        new HashMap<>(abstractStreamCreateStatement.getProperties());
    if (!abstractStreamCreateStatement.getProperties().containsKey(KsqlConstants.AVRO_SCHEMA_ID)) {
      properties.put(KsqlConstants.AVRO_SCHEMA_ID, schemaIdLiteral);
    }

    return abstractStreamCreateStatement.copyWith(elements, properties);
  }


  public void validatePersistentQueryResults(
      final PersistentQueryMetadata persistentQueryMetadata,
      final SchemaRegistryClient schemaRegistryClient
  ) {

    if (persistentQueryMetadata.getResultTopicSerde() == DataSource.DataSourceSerDe.AVRO) {
      String avroSchemaString = SchemaUtil.buildAvroSchema(
          persistentQueryMetadata.getResultSchema(),
          persistentQueryMetadata.getResultTopic().getName()
      );
      boolean isValidSchema = isValidAvroSchemaForTopic(
          persistentQueryMetadata.getResultTopic().getTopicName(),
          avroSchemaString,
          schemaRegistryClient
      );
      if (!isValidSchema) {
        throw new KsqlException(String.format(
            "Cannot register avro schema for %s since it is not valid for schema registry.",
            persistentQueryMetadata.getResultTopic().getKafkaTopicName()
        ));
      }
    }
  }


  private boolean isValidAvroSchemaForTopic(
      final String topicName,
      final String avroSchemaString,
      final SchemaRegistryClient schemaRegistryClient
  ) {

    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = parser.parse(avroSchemaString);
    try {
      return schemaRegistryClient.testCompatibility(topicName, avroSchema);
    } catch (IOException e) {
      String errorMessage = String.format(
          "Could not check Schema compatibility: %s", e.getMessage()
      );
      log.error(errorMessage, e);
      throw new KsqlException(errorMessage);
    } catch (RestClientException e) {
      String errorMessage = String.format(
          "Could not connect to Schema Registry service: %s", e.getMessage()
      );
      log.error(errorMessage, e);
      throw new KsqlException(errorMessage);
    }
  }

}
