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

package io.confluent.ksql.util;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.serde.DataSource;
import java.io.IOException;
import org.apache.http.HttpStatus;

public final class AvroUtil {

  private AvroUtil() {
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
}
