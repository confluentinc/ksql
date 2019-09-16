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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.serde.Format;
import java.io.IOException;

import org.apache.http.HttpStatus;
import org.apache.kafka.common.acl.AclOperation;

public final class AvroUtil {

  private AvroUtil() {
  }

  public static void throwOnInvalidSchemaEvolution(
      final PersistentQueryMetadata queryMetadata,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    if (queryMetadata.getResultTopicFormat() != Format.AVRO) {
      return;
    }

    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(
        queryMetadata.getPhysicalSchema().valueSchema(),
        queryMetadata.getSinkName()
    );

    final String topicName = queryMetadata.getResultTopic().getKafkaTopicName();

    if (!isValidAvroSchemaForTopic(topicName, avroSchema, schemaRegistryClient)) {
      throw new KsqlStatementException(String.format(
          "Cannot register avro schema for %s as the schema is incompatible with the current "
              + "schema version registered for the topic.%n"
              + "KSQL schema: %s%n"
              + "Registered schema: %s",
          topicName,
          avroSchema,
          getRegisteredSchema(topicName, schemaRegistryClient)
      ), queryMetadata.getStatementString());
    }
  }

  private static String getRegisteredSchema(
      final String topicName,
      final SchemaRegistryClient schemaRegistryClient) {
    try {
      return schemaRegistryClient
          .getLatestSchemaMetadata(topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX)
          .getSchema();
    } catch (Exception e) {
      return "Could not get registered schema due to exception: " + e.getMessage();
    }
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

      if (e.getStatus() == HttpStatus.SC_UNAUTHORIZED || e.getStatus() == HttpStatus.SC_FORBIDDEN) {
        throw new KsqlSchemaAuthorizationException(
            AclOperation.WRITE,
            topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
        );
      }

      throw new KsqlException(String.format(
          "Could not connect to Schema Registry service: %s", e.getMessage()
      ));
    }
  }
}
