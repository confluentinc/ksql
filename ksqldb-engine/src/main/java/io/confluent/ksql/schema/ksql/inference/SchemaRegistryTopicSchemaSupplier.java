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

import static io.confluent.ksql.util.KsqlConstants.getSRSubject;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SchemaTranslator;
import io.confluent.ksql.serde.SerdeFeatures;
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
  private final Function<FormatInfo, Format> formatFactory;

  public SchemaRegistryTopicSchemaSupplier(final SchemaRegistryClient srClient) {
    this(
        srClient,
        FormatFactory::of
    );
  }

  @VisibleForTesting
  SchemaRegistryTopicSchemaSupplier(
      final SchemaRegistryClient srClient,
      final Function<FormatInfo, Format> formatFactory
  ) {
    this.srClient = Objects.requireNonNull(srClient, "srClient");
    this.formatFactory = Objects.requireNonNull(formatFactory, "formatFactory");
  }

  @Override
  public SchemaResult getKeySchema(
      final String topicName,
      final Optional<Integer> schemaId,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures
  ) {
    return getSchema(topicName, schemaId, expectedFormat, serdeFeatures, true);
  }

  @Override
  public SchemaResult getValueSchema(
      final String topicName,
      final Optional<Integer> schemaId,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures
  ) {
    return getSchema(topicName, schemaId, expectedFormat, serdeFeatures, false);
  }

  private SchemaResult getSchema(
      final String topicName,
      final Optional<Integer> schemaId,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures,
      final boolean isKey
  ) {
    try {
      final String subject = getSRSubject(topicName, isKey);

      final int id;
      if (schemaId.isPresent()) {
        id = schemaId.get();
      } else {
        id = srClient.getLatestSchemaMetadata(subject).getId();
      }

      final ParsedSchema schema = srClient.getSchemaBySubjectAndId(subject, id);
      return fromParsedSchema(topicName, id, schema, expectedFormat, serdeFeatures, isKey);
    } catch (final RestClientException e) {
      switch (e.getStatus()) {
        case HttpStatus.SC_NOT_FOUND:
        case HttpStatus.SC_UNAUTHORIZED:
        case HttpStatus.SC_FORBIDDEN:
          return notFound(topicName, isKey);
        default:
          throw new KsqlException("Schema registry fetch for topic " + (isKey ? "key" : "value")
              + " request failed. Topic: " + topicName, e);
      }
    } catch (final Exception e) {
      throw new KsqlException("Schema registry fetch for topic " + (isKey ? "key" : "value")
          + " request failed. Topic: " + topicName, e);
    }
  }

  private SchemaResult fromParsedSchema(
      final String topic,
      final int id,
      final ParsedSchema parsedSchema,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures,
      final boolean isKey
  ) {
    final Format format = formatFactory.apply(expectedFormat);
    final SchemaTranslator translator = format.getSchemaTranslator(expectedFormat.getProperties());

    if (!parsedSchema.schemaType().equals(translator.name())) {
      return incorrectFormat(topic, translator.name(), parsedSchema.schemaType(), isKey);
    }

    final List<SimpleColumn> columns;
    try {
      columns = translator.toColumns(
          parsedSchema,
          serdeFeatures,
          isKey
      );
    } catch (final Exception e) {
      return notCompatible(topic, parsedSchema.canonicalString(), e, isKey);
    }

    if (isKey && columns.size() > 1) {
      return multiColumnKeysNotSupported(topic, parsedSchema.canonicalString());
    }

    return SchemaResult.success(SchemaAndId.schemaAndId(columns, id));
  }

  private static SchemaResult incorrectFormat(
      final String topic,
      final String expectedFormat,
      final String actualFormat,
      final boolean isKey
  ) {
    final String config = isKey
        ? CommonCreateConfigs.KEY_FORMAT_PROPERTY
        : CommonCreateConfigs.VALUE_FORMAT_PROPERTY;
    return SchemaResult.failure(new KsqlException(
        (isKey ? "Key" : "Value") + " schema is not in the expected format. "
            + "You may want to set " + config + " to '" + actualFormat + "'."
            + System.lineSeparator()
            + "topic: " + topic
            + System.lineSeparator()
            + "expected format: " + expectedFormat
            + System.lineSeparator()
            + "actual format: " + actualFormat
    ));
  }

  private static SchemaResult notFound(final String topicName, final boolean isKey) {
    final String subject = getSRSubject(topicName, isKey);
    return SchemaResult.failure(new KsqlException(
        "Schema for message " + (isKey ? "keys" : "values") +  " on topic " + topicName
            + " does not exist in the Schema Registry."
            + "Subject: " + subject
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
            + "- You do not have permissions to access the Schema Registry.Subject: " + subject
            + "\t-> See " + DocumentationLinks.SCHEMA_REGISTRY_SECURITY_DOC_URL));
  }

  private static SchemaResult notCompatible(
      final String topicName,
      final String schema,
      final Exception cause,
      final boolean isKey
  ) {
    return SchemaResult.failure(new KsqlException(
        "Unable to verify if the " + (isKey ? "key" : "value") + " schema for topic "
            + topicName + " is compatible with ksqlDB."
            + System.lineSeparator()
            + "Reason: " + cause.getMessage()
            + System.lineSeparator()
            + System.lineSeparator()
            + "Please see https://github.com/confluentinc/ksql/issues/ to see if this particular "
            + "reason is already known."
            + System.lineSeparator()
            + "If not, please log a new issue, including this full error message."
            + System.lineSeparator()
            + "Schema:" + schema,
        cause));
  }

  private static SchemaResult multiColumnKeysNotSupported(
      final String topicName,
      final String schema
  ) {
    return SchemaResult.failure(new KsqlException(
        "The key schema for topic " + topicName
            + " contains multiple columns, which is not supported by ksqlDB at this time."
            + System.lineSeparator()
            + "Schema:" + schema));
  }
}
