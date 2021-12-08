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

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Supplier of schemas for topics
 */
public interface TopicSchemaSupplier {

  /**
   * Get the key schema for the supplied {@code topicName} or {@code schemaId}.
   *
   * <p>
   * At least one of topicName and schemaId should be provided. If only topicName is provided,
   * latest schema will be fetched under the subject constructed by topicName.
   * </p>
   *
   *
   * @param topicName the name of the topic.
   * @param schemaId schema id to retrieve.
   * @param expectedFormat the expected format of the schema.
   * @param serdeFeatures serde features associated with the request.
   * @return the schema and id or an error message should the schema not be present or compatible.
   * @throws RuntimeException on communication issues with remote services.
   */
  SchemaResult getKeySchema(
      Optional<String> topicName,
      Optional<Integer> schemaId,
      FormatInfo expectedFormat,
      SerdeFeatures serdeFeatures
  );

  /**
   * Get the value schema for the supplied {@code topicName} or {@code schemaId}.
   *
   * <p>
   * At least one of topicName and schemaId should be provided. If only topicName is provided,
   * latest schema will be fetched under the subject constructed by topicName.
   * </p>
   *
   * @param topicName the name of the topic.
   * @param schemaId schema id to retrieve.  *
   * @param expectedFormat the expected format of the schema.
   * @param serdeFeatures serde features associated with the request.
   * @return the schema and id or an error message should the schema not be present or compatible.
   * @throws RuntimeException on communication issues with remote services.
   */
  SchemaResult getValueSchema(
      Optional<String> topicName,
      Optional<Integer> schemaId,
      FormatInfo expectedFormat,
      SerdeFeatures serdeFeatures
  );

  final class SchemaAndId {

    final int id;
    final List<? extends SimpleColumn> columns;
    final ParsedSchema rawSchema;

    private SchemaAndId(
        final List<? extends SimpleColumn> columns,
        final ParsedSchema rawSchema,
        final int id
    ) {
      this.id = id;
      this.rawSchema = rawSchema;
      this.columns = ImmutableList.copyOf(Objects.requireNonNull(columns, "columns"));
    }

    static SchemaAndId schemaAndId(
        final List<? extends SimpleColumn> columns,
        final ParsedSchema rawSchema,
        final int id
    ) {
      return new SchemaAndId(columns, rawSchema, id);
    }
  }

  final class SchemaResult {

    final Optional<SchemaAndId> schemaAndId;
    final Optional<Exception> failureReason;

    private SchemaResult(
        final Optional<SchemaAndId> schemaAndId,
        final Optional<Exception> failureReason
    ) {
      this.schemaAndId = Objects.requireNonNull(schemaAndId, "schemaAndId");
      this.failureReason = Objects.requireNonNull(failureReason, "failureReason");
    }

    static SchemaResult success(final SchemaAndId schemaAndId) {
      return new SchemaResult(Optional.of(schemaAndId), Optional.empty());
    }

    static SchemaResult failure(final Exception cause) {
      return new SchemaResult(Optional.empty(), Optional.of(cause));
    }

    public Optional<SchemaAndId> getSchemaAndId() {
      return schemaAndId;
    }

    public Optional<Exception> getFailureReason() {
      return failureReason;
    }
  }
}