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

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.avro.AvroSchemaTranslator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroUtil {

  private static final Logger log = LoggerFactory.getLogger(AvroUtil.class);

  public static AbstractStreamCreateStatement checkAndSetAvroSchema(
      final AbstractStreamCreateStatement abstractStreamCreateStatement,
      final Map<String, Object> streamsProperties,
      final SchemaRegistryClient schemaRegistryClient
  ) {

    final Map<String, Expression> ddlProperties = abstractStreamCreateStatement.getProperties();
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
      return abstractStreamCreateStatement;
    }

    final String kafkaTopicName = StringUtil.cleanQuotes(
        ddlProperties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString()
    );
    try {
      // If the schema is not specified infer it from the Avro schema in Schema Registry.
      if (abstractStreamCreateStatement.getElements().isEmpty()) {
        final SchemaMetadata schemaMetadata = fetchSchemaMetadata(
            abstractStreamCreateStatement,
            schemaRegistryClient,
            kafkaTopicName
        );

        final String avroSchemaString = schemaMetadata.getSchema();
        streamsProperties.put(DdlConfig.AVRO_SCHEMA, avroSchemaString);
        final AbstractStreamCreateStatement abstractStreamCreateStatementCopy = addAvroFields(
            abstractStreamCreateStatement,
            AvroSchemaTranslator.toKsqlSchema(avroSchemaString),
            schemaMetadata.getId()
        );
        return abstractStreamCreateStatementCopy;
      } else {
        return abstractStreamCreateStatement;
      }
    } catch (final Exception e) {
      throw new KsqlException("Unable to verify if the Avro schema for topic " + kafkaTopicName
          + " is compatible with KSQL.\nReason: " + e.getMessage() + "\n\n"
          + "Please see https://github.com/confluentinc/ksql/issues/ to see if this "
          + "particular reason is already\n"
          + "known, and if not log a new issue. Please include the full Avro schema "
          + "that you are trying to use.");
    }
  }

  private static SchemaMetadata fetchSchemaMetadata(
      final AbstractStreamCreateStatement abstractStreamCreateStatement,
      final SchemaRegistryClient schemaRegistryClient,
      final String kafkaTopicName
  ) throws IOException {

    try {
      if (abstractStreamCreateStatement.getProperties().containsKey(KsqlConstants.AVRO_SCHEMA_ID)) {
        final int schemaId;
        try {
          schemaId = Integer.parseInt(
              StringUtil.cleanQuotes(
                  abstractStreamCreateStatement
                      .getProperties()
                      .get(KsqlConstants.AVRO_SCHEMA_ID)
                      .toString()
              )
          );
        } catch (final NumberFormatException e) {
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
    } catch (final RestClientException e) {
      if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
        throw new KsqlException("Avro schema for message values on topic " + kafkaTopicName
            + " does not exist in the Schema Registry.\n"
            + "Subject: " + kafkaTopicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX + "\n\n"
            + "Possible causes include:\n"
            + "- The topic itself does not exist\n"
            + "\t-> Use SHOW TOPICS; to check\n"
            + "- Messages on the topic are not Avro serialized\n"
            + "\t-> Use PRINT '" + kafkaTopicName + "' FROM BEGINNING; to verify\n"
            + "- Messages on the topic have not been serialized using the Confluent Schema "
            + "Registry Avro serializer\n"
            + "\t-> See " + KsqlConstants.DOC_URL_SR_SERIALISER + "\n"
            + "- The schema is registered on a different instance of the Schema Registry\n"
            + "\t-> Use the REST API to list available subjects\n\t"
            + KsqlConstants.DOC_URL_SR_REST_GETSUBJECTS + "\n"
        );
      }
      throw new KsqlException("Schema registry fetch for topic " + kafkaTopicName
          + " request failed.\n", e);

    }
  }

  private static AbstractStreamCreateStatement addAvroFields(
      final AbstractStreamCreateStatement abstractStreamCreateStatement,
      final Schema schema,
      final int schemaId
  ) {
    final List<TableElement> elements = new ArrayList<>();
    for (final Field field : schema.fields()) {
      final TableElement tableElement = new TableElement(field.name().toUpperCase(),
                                                   TypeUtil.getKsqlType(field.schema()));
      elements.add(tableElement);
    }
    final StringLiteral schemaIdLiteral = new StringLiteral(String.format("%d", schemaId));
    final Map<String, Expression> properties =
        new HashMap<>(abstractStreamCreateStatement.getProperties());
    if (!abstractStreamCreateStatement.getProperties().containsKey(KsqlConstants.AVRO_SCHEMA_ID)) {
      properties.put(KsqlConstants.AVRO_SCHEMA_ID, schemaIdLiteral);
    }

    return abstractStreamCreateStatement.copyWith(elements, properties);
  }


  public static void validatePersistentQueryResults(
      final PersistentQueryMetadata persistentQueryMetadata,
      final SchemaRegistryClient schemaRegistryClient
  ) {

    if (persistentQueryMetadata.getResultTopicSerde() == DataSource.DataSourceSerDe.AVRO) {
      final String avroSchemaString = SchemaUtil.buildAvroSchema(
          persistentQueryMetadata.getResultSchema(),
          persistentQueryMetadata.getResultTopic().getName()
      );
      final boolean isValidSchema = isValidAvroSchemaForTopic(
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


  private static boolean isValidAvroSchemaForTopic(
      final String topicName,
      final String avroSchemaString,
      final SchemaRegistryClient schemaRegistryClient
  ) {

    final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    final org.apache.avro.Schema avroSchema = parser.parse(avroSchemaString);
    try {
      return schemaRegistryClient.testCompatibility(topicName, avroSchema);
    } catch (final IOException e) {
      final String errorMessage = String.format(
          "Could not check Schema compatibility: %s", e.getMessage()
      );
      log.error(errorMessage, e);
      throw new KsqlException(errorMessage);
    } catch (final RestClientException e) {
      final String errorMessage = String.format(
          "Could not connect to Schema Registry service: %s", e.getMessage()
      );
      log.error(errorMessage, e);
      throw new KsqlException(errorMessage);
    }
  }
}
