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

package io.confluent.ksql.util;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.apache.kafka.connect.data.Schema;

public final class AvroUtil {

  private AvroUtil() {
  }

  static AbstractStreamCreateStatement checkAndSetAvroSchema(
      final AbstractStreamCreateStatement abstractStreamCreateStatement,
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

    if (!ddlProperties.containsKey(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY)) {
      throw new KsqlException(String.format(
          "Corresponding Kafka topic (%s) should be set in WITH clause.",
          DdlConfig.KAFKA_TOPIC_NAME_PROPERTY
      ));
    }
    final String kafkaTopicName = StringUtil.cleanQuotes(
            ddlProperties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString()
    );

    if (!abstractStreamCreateStatement.getElements().isEmpty()) {
      return abstractStreamCreateStatement;
    }

    // If the schema is not specified infer it from the Avro schema in Schema Registry.
    final SchemaMetadata schemaMetadata = getSchemaMetadata(
            abstractStreamCreateStatement, schemaRegistryClient, kafkaTopicName);

    try {
      final String avroSchemaString = schemaMetadata.getSchema();
      return addAvroFields(
              abstractStreamCreateStatement,
              toKsqlSchema(avroSchemaString),
              schemaMetadata.getId()
      );

    } catch (final Exception e) {
      throw new KsqlException("Unable to verify if the Avro schema for topic " + kafkaTopicName
              + " is compatible with KSQL.\nReason: " + e.getMessage() + "\n\n"
              + "Please see https://github.com/confluentinc/ksql/issues/ to see if this "
              + "particular reason is already\n"
              + "known, and if not log a new issue. Please include the full Avro schema "
              + "that you are trying to use."
      );
    }
  }

  private static SchemaMetadata getSchemaMetadata(final AbstractStreamCreateStatement
                                                          abstractStreamCreateStatement,
                                                  final SchemaRegistryClient schemaRegistryClient,
                                                  final String kafkaTopicName) {
    try {
      return fetchSchemaMetadata(
              abstractStreamCreateStatement,
              schemaRegistryClient,
              kafkaTopicName
      );
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
                + "\t-> See " + DocumentationLinks.SR_SERIALISER_DOC_URL + "\n"
                + "- The schema is registered on a different instance of the Schema Registry\n"
                + "\t-> Use the REST API to list available subjects\n\t"
                + DocumentationLinks.SR_REST_GETSUBJECTS_DOC_URL + "\n"
                );
      }
      throw new KsqlException("Schema registry fetch for topic " + kafkaTopicName
              + " request failed.\n", e);
    } catch (final Exception e) {
      throw new KsqlException("Schema registry fetch for topic " + kafkaTopicName
              + " request failed.\n", e);
    }
  }

  private static SchemaMetadata fetchSchemaMetadata(
      final AbstractStreamCreateStatement abstractStreamCreateStatement,
      final SchemaRegistryClient schemaRegistryClient,
      final String kafkaTopicName
  ) throws IOException, RestClientException {

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
  }

  private static AbstractStreamCreateStatement addAvroFields(
      final AbstractStreamCreateStatement abstractStreamCreateStatement,
      final Schema schema,
      final int schemaId
  ) {
    final List<TableElement> elements = TypeUtil.buildTableElementsForSchema(schema);
    final StringLiteral schemaIdLiteral = new StringLiteral(String.format("%d", schemaId));
    final Map<String, Expression> properties =
        new HashMap<>(abstractStreamCreateStatement.getProperties());
    if (!abstractStreamCreateStatement.getProperties().containsKey(KsqlConstants.AVRO_SCHEMA_ID)) {
      properties.put(KsqlConstants.AVRO_SCHEMA_ID, schemaIdLiteral);
    }

    return abstractStreamCreateStatement.copyWith(elements, properties);
  }

  public static boolean isValidSchemaEvolution(
      final PersistentQueryMetadata persistentQueryMetadata,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    if (persistentQueryMetadata.getResultTopicSerde() != DataSource.DataSourceSerDe.AVRO) {
      return true;
    }

    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(
        persistentQueryMetadata.getResultSchema(),
        persistentQueryMetadata.getResultTopic().getName()
    );

    final String topicName = persistentQueryMetadata.getResultTopic().getKafkaTopicName();

    return isValidAvroSchemaForTopic(topicName, avroSchema, schemaRegistryClient);
  }

  private static boolean isValidAvroSchemaForTopic(
      final String topicName,
      final org.apache.avro.Schema avroSchema,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    try {
      return schemaRegistryClient.testCompatibility(
          topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, avroSchema);
    } catch (final IOException e) {
      throw new KsqlException(String.format(
          "Could not check Schema compatibility: %s", e.getMessage()
      ));
    } catch (final RestClientException e) {
      if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
        // Assume the subject is unknown.
        // See https://github.com/confluentinc/schema-registry/issues/951
        return true;
      }
      throw new KsqlException(String.format(
          "Could not connect to Schema Registry service: %s", e.getMessage()
      ));
    }
  }

  static Schema toKsqlSchema(final String avroSchemaString) {
    final org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(avroSchemaString);
    final AvroData avroData = new AvroData(new AvroDataConfig(Collections.emptyMap()));
    return new ConnectSchemaTranslator().toKsqlSchema(avroData.toConnectSchema(avroSchema));
  }
}
