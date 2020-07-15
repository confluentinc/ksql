/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql.inference;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hc.core5.http.HttpStatus;

/**
 * A {@link TopicSchemaSupplier} that retrieves schemas from the Schema Registry.
 */
public class SchemaRegistryTopicSchemaSupplier implements TopicSchemaSupplier {

  private final SchemaRegistryClient srClient;
  private final Function<String, Format> formatFactory;

  public SchemaRegistryTopicSchemaSupplier(final SchemaRegistryClient srClient) {
    this(
        srClient,
        FormatFactory::fromName
    );
  }

  @VisibleForTesting
  SchemaRegistryTopicSchemaSupplier(
      final SchemaRegistryClient srClient,
      final Function<String, Format> formatFactory
  ) {
    this.srClient = Objects.requireNonNull(srClient, "srClient");
    this.formatFactory = Objects.requireNonNull(formatFactory, "formatFactory");
  }

  @Override
  public SchemaResult getValueSchema(final String topicName, final Optional<Integer> schemaId) {
    try {
      final String subject = topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;
      final int id;

      if (schemaId.isPresent()) {
        id = schemaId.get();
      } else {
        id = srClient.getLatestSchemaMetadata(subject).getId();
      }

      final ParsedSchema schema = srClient.getSchemaBySubjectAndId(subject, id);
      return fromParsedSchema(topicName, id, schema);
    } catch (final RestClientException e) {
      switch (e.getStatus()) {
        case HttpStatus.SC_NOT_FOUND:
        case HttpStatus.SC_UNAUTHORIZED:
        case HttpStatus.SC_FORBIDDEN:
          return notFound(topicName);
        default:
          throw new KsqlException("Schema registry fetch for topic "
              + topicName + " request failed.", e);
      }
    } catch (final Exception e) {
      throw new KsqlException("Schema registry fetch for topic "
          + topicName + " request failed.", e);
    }
  }

  public SchemaResult fromParsedSchema(
      final String topic,
      final int id,
      final ParsedSchema parsedSchema
  ) {
    try {
      final Format format = formatFactory.apply(parsedSchema.schemaType());
      final List<SimpleColumn> columns = format.toColumns(parsedSchema);
      return SchemaResult.success(SchemaAndId.schemaAndId(columns, id));
    } catch (final Exception e) {
      return notCompatible(topic, parsedSchema.canonicalString(), e);
    }
  }

  private static SchemaResult notFound(final String topicName) {
    return SchemaResult.failure(new KsqlException(
        "Schema for message values on topic " + topicName
            + " does not exist in the Schema Registry."
            + "Subject: " + topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
            + System.lineSeparator()
            + "Possible causes include:"
            + System.lineSeparator()
            + "- The topic itself does not exist"
            + "\t-> Use SHOW TOPICS; to check"
            + System.lineSeparator()
            + "- Messages on the topic are not serialized using a format Schema Registry supports"
            + "\t-> Use PRINT '" + topicName + "' FROM BEGINNING; to verify"
            + System.lineSeparator()
            + "- Messages on the topic have not been serialized using a Confluent Schema "
            + "Registry supported serializer"
            + "\t-> See " + DocumentationLinks.SR_SERIALISER_DOC_URL
            + System.lineSeparator()
            + "- The schema is registered on a different instance of the Schema Registry"
            + "\t-> Use the REST API to list available subjects"
            + "\t" + DocumentationLinks.SR_REST_GETSUBJECTS_DOC_URL
            + System.lineSeparator()
            + "- You do not have permissions to access the Schema Registry.Subject: "
            + topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
            + "\t-> See " + DocumentationLinks.SCHEMA_REGISTRY_SECURITY_DOC_URL));
  }

  private static SchemaResult notCompatible(
      final String topicName,
      final String schema,
      final Exception cause
  ) {
    return SchemaResult.failure(new KsqlException(
        "Unable to verify if the schema for topic " + topicName + " is compatible with KSQL."
            + System.lineSeparator()
            + "Reason: " + cause.getMessage()
            + System.lineSeparator()
            + System.lineSeparator()
            + "Please see https://github.com/confluentinc/ksql/issues/ to see if this particular "
            + "reason is already known."
            + System.lineSeparator()
            + "If not, please log a new issue, including the this full error message."
            + System.lineSeparator()
            + "Schema:" + schema,
        cause));
  }
}
