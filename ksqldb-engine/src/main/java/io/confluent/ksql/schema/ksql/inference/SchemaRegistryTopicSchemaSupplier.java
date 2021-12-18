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
import io.confluent.ksql.serde.SchemaTranslationPolicies;
import io.confluent.ksql.serde.SchemaTranslationPolicy;
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
      final Optional<String> topicName,
      final Optional<Integer> schemaId,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures
  ) {
    return getSchema(topicName, schemaId, expectedFormat, serdeFeatures, true);
  }

  @Override
  public SchemaResult getValueSchema(
      final Optional<String> topicName,
      final Optional<Integer> schemaId,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures
  ) {
    return getSchema(topicName, schemaId, expectedFormat, serdeFeatures, false);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public SchemaResult getSchema(
      final Optional<String> topicName,
      final Optional<Integer> schemaId,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures,
      final boolean isKey
  ) {
    if (!topicName.isPresent() && !schemaId.isPresent()) {
      throw new IllegalArgumentException("One of topicName and schemaId should be provided");
    }

    try {
      final int id;
      final String subject;
      if (schemaId.isPresent()) {
        subject = topicName.map(s -> getSRSubject(s, isKey)).orElse(null);
        id = schemaId.get();
      } else {
        subject = getSRSubject(topicName.get(), isKey);
        id = srClient.getLatestSchemaMetadata(subject).getId();
      }

      final ParsedSchema schema = srClient.getSchemaBySubjectAndId(subject, id);
      return fromParsedSchema(topicName, id, schemaId, schema, expectedFormat, serdeFeatures,
          isKey);
    } catch (final RestClientException e) {
      switch (e.getStatus()) {
        case HttpStatus.SC_NOT_FOUND:
        case HttpStatus.SC_UNAUTHORIZED:
        case HttpStatus.SC_FORBIDDEN:
          return notFound(topicName, schemaId, isKey);
        default:
          throw new KsqlException(fetchExceptionMessage(topicName, schemaId, isKey), e);
      }
    } catch (final Exception e) {
      throw new KsqlException(fetchExceptionMessage(topicName, schemaId, isKey), e);
    }
  }

  private SchemaResult fromParsedSchema(
      final Optional<String> topic,
      final int id,
      final Optional<Integer> schemaId,
      final ParsedSchema parsedSchema,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures,
      final boolean isKey
  ) {
    final Format format = formatFactory.apply(expectedFormat);
    // Topic existence means schema id is not provided original so that default
    // policy should be used
    final SchemaTranslator translator =
        schemaId.isPresent() ? format.getSchemaTranslator(expectedFormat.getProperties(),
            SchemaTranslationPolicies.of(SchemaTranslationPolicy.ORIGINAL_FIELD_NAME))
            : format.getSchemaTranslator(expectedFormat.getProperties());

    if (!parsedSchema.schemaType().equals(translator.name())) {
      return incorrectFormat(topic, schemaId, translator.name(), parsedSchema.schemaType(), isKey);
    }

    final List<SimpleColumn> columns;
    try {
      columns = translator.toColumns(
          parsedSchema,
          serdeFeatures,
          isKey
      );
    } catch (final Exception e) {
      return notCompatible(topic, schemaId, parsedSchema.canonicalString(), e, isKey);
    }

    if (isKey && columns.size() > 1) {
      return multiColumnKeysNotSupported(topic, schemaId, parsedSchema.canonicalString());
    }

    return SchemaResult.success(SchemaAndId.schemaAndId(columns, parsedSchema, id));
  }

  private static SchemaResult incorrectFormat(
      final Optional<String> topic,
      final Optional<Integer> schemaId,
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
            + sourceMsg(topic, schemaId)
            + System.lineSeparator()
            + "expected format: " + expectedFormat
            + System.lineSeparator()
            + "actual format from Schema Registry: " + actualFormat
    ));
  }

  private static SchemaResult notFound(
      final Optional<String> topicName,
      final Optional<Integer> schemaId,
      final boolean isKey
  ) {
    final String suffixMsg = "- Messages on the topic have not been serialized using a Confluent "
        + "Schema Registry supported serializer"
        + System.lineSeparator()
        + "\t-> See " + DocumentationLinks.SR_SERIALISER_DOC_URL
        + System.lineSeparator()
        + "- The schema is registered on a different instance of the Schema Registry"
        + System.lineSeparator()
        + "\t-> Use the REST API to list available subjects"
        + "\t" + DocumentationLinks.SR_REST_GETSUBJECTS_DOC_URL
        + System.lineSeparator()
        + "- You do not have permissions to access the Schema Registry."
        + System.lineSeparator()
        + "\t-> See " + DocumentationLinks.SCHEMA_REGISTRY_SECURITY_DOC_URL;
    final String schemaIdMsg =
        schemaId.map(integer -> "Schema Id: " + integer + System.lineSeparator()).orElse("");

    if (topicName.isPresent()) {
      final String subject = getSRSubject(topicName.get(), isKey);
      return SchemaResult.failure(new KsqlException(
          "Schema for message " + (isKey ? "keys" : "values") + " on topic '" + topicName.get()
              + "'"
              + " does not exist in the Schema Registry."
              + System.lineSeparator()
              + "Subject: " + subject
              + System.lineSeparator()
              + schemaIdMsg
              + "Possible causes include:"
              + System.lineSeparator()
              + "- The topic itself does not exist"
              + System.lineSeparator()
              + "\t-> Use SHOW TOPICS; to check"
              + System.lineSeparator()
              + "- Messages on the topic are not serialized using a format Schema Registry supports"
              + System.lineSeparator()
              + "\t-> Use PRINT '" + topicName.get() + "' FROM BEGINNING; to verify"
              + System.lineSeparator()
              + suffixMsg));
    }
    return SchemaResult.failure(new KsqlException(
        "Schema for message " + (isKey ? "keys" : "values") + " on schema id'" + schemaId.get()
            + " does not exist in the Schema Registry."
            + System.lineSeparator()
            + schemaIdMsg
            + "Possible causes include:"
            + System.lineSeparator()
            + "- Schema Id " + schemaId
            + System.lineSeparator()
            + suffixMsg));
  }

  private static SchemaResult notCompatible(
      final Optional<String> topicName,
      final Optional<Integer> schemaId,
      final String schema,
      final Exception cause,
      final boolean isKey
  ) {
    return SchemaResult.failure(new KsqlException(
        "Unable to verify if the " + (isKey ? "key" : "value") + " schema for "
            + sourceMsg(topicName, schemaId)
            + " is compatible with ksqlDB."
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
      final Optional<String> topic,
      final Optional<Integer> schemaId,
      final String schema
  ) {
    return SchemaResult.failure(new KsqlException(
        "The key schema for " + sourceMsg(topic, schemaId)
            + " contains multiple columns, which is not supported by ksqlDB at this time."
            + System.lineSeparator()
            + "Schema:" + schema));
  }

  private static String fetchExceptionMessage(
      final Optional<String> topicName,
      final Optional<Integer> schemaId,
      final boolean isKey
  ) {
    return "Schema registry fetch for topic " + (isKey ? "key" : "value") + " request failed for "
        + sourceMsg(topicName, schemaId);
  }

  private static String sourceMsg(
      final Optional<String> topicName,
      final Optional<Integer> schemaId
  ) {
    String msg = "";
    if (topicName.isPresent()) {
      msg += ("topic: " + topicName.get());
    }
    if (schemaId.isPresent()) {
      msg += (msg.isEmpty() ? ("schema id: " + schemaId.get())
          : (", schema id: " + schemaId.get()));
    }
    return msg;
  }
}