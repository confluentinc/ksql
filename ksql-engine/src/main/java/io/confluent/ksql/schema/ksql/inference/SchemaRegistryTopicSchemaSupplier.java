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
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.avro.Schema.Parser;
import org.apache.http.HttpStatus;
import org.apache.kafka.connect.data.Schema;

/**
 * A {@link TopicSchemaSupplier} that retrieves schemas from the Schema Registry.
 */
public class SchemaRegistryTopicSchemaSupplier implements TopicSchemaSupplier {

  private final SchemaRegistryClient srClient;
  private final Function<String, org.apache.avro.Schema> toAvroTranslator;
  private final Function<org.apache.avro.Schema, Schema> toConnectTranslator;
  private final Function<Schema, Schema> toKsqlTranslator;

  public SchemaRegistryTopicSchemaSupplier(final SchemaRegistryClient srClient) {
    this(
        srClient,
        schema -> new Parser().parse(schema),
        new AvroData(new AvroDataConfig(Collections.emptyMap()))::toConnectSchema,
        new ConnectSchemaTranslator()::toKsqlSchema);
  }

  @VisibleForTesting
  SchemaRegistryTopicSchemaSupplier(
      final SchemaRegistryClient srClient,
      final Function<String, org.apache.avro.Schema> toAvroTranslator,
      final Function<org.apache.avro.Schema, Schema> toConnectTranslator,
      final Function<Schema, Schema> toKsqlTranslator
  ) {
    this.srClient = Objects.requireNonNull(srClient, "srClient");
    this.toAvroTranslator = Objects.requireNonNull(toAvroTranslator, "toAvroTranslator");
    this.toConnectTranslator = Objects.requireNonNull(toConnectTranslator, "toConnectTranslator");
    this.toKsqlTranslator = Objects.requireNonNull(toKsqlTranslator, "toKsqlTranslator");
  }

  @Override
  public SchemaResult getValueSchema(final String topicName, final Optional<Integer> schemaId) {

    final Optional<SchemaMetadata> metadata = getSchema(topicName, schemaId);
    if (!metadata.isPresent()) {
      return notFound(topicName);
    }

    try {
      final Schema connectSchema = toConnectSchema(metadata.get().getSchema());

      return SchemaResult.success(SchemaAndId.schemaAndId(connectSchema, metadata.get().getId()));
    } catch (final Exception e) {
      return notCompatible(topicName, metadata.get().getSchema(), e);
    }
  }

  private Optional<SchemaMetadata> getSchema(
      final String topicName,
      final Optional<Integer> schemaId
  ) {
    try {
      final String subject = topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

      if (schemaId.isPresent()) {
        return Optional.of(srClient.getSchemaMetadata(subject, schemaId.get()));
      }

      return Optional.of(srClient.getLatestSchemaMetadata(subject));
    } catch (final RestClientException e) {
      switch (e.getStatus()) {
        case HttpStatus.SC_NOT_FOUND:
        case HttpStatus.SC_UNAUTHORIZED:
        case HttpStatus.SC_FORBIDDEN:
          return Optional.empty();
        default:
          throw new KsqlException("Schema registry fetch for topic "
              + topicName + " request failed.", e);
      }
    } catch (final Exception e) {
      throw new KsqlException("Schema registry fetch for topic "
          + topicName + " request failed.", e);
    }
  }

  private Schema toConnectSchema(final String avroSchemaString) {
    final org.apache.avro.Schema avroSchema = toAvroTranslator.apply(avroSchemaString);
    final Schema connectSchema = toConnectTranslator.apply(avroSchema);
    return toKsqlTranslator.apply(connectSchema);
  }

  private static SchemaResult notFound(final String topicName) {
    return SchemaResult.failure(new KsqlException(
        "Avro schema for message values on topic " + topicName
            + " does not exist in the Schema Registry."
            + "Subject: " + topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
            + System.lineSeparator()
            + "Possible causes include:"
            + System.lineSeparator()
            + "- The topic itself does not exist"
            + "\t-> Use SHOW TOPICS; to check"
            + System.lineSeparator()
            + "- Messages on the topic are not Avro serialized"
            + "\t-> Use PRINT '" + topicName + "' FROM BEGINNING; to verify"
            + System.lineSeparator()
            + "- Messages on the topic have not been serialized using the Confluent Schema "
            + "Registry Avro serializer"
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
