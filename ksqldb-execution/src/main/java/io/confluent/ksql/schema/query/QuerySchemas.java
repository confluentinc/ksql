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

package io.confluent.ksql.schema.query;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Pojo for holding data about the schemas and formats in use at the different stages within a
 * topology of a query.
 *
 * <p>Contains an map of 'logger name prefix' to the schema and formats used when creating a serde.
 *
 * <p>If {@link RuntimeBuildContext#KSQL_TEST_TRACK_SERDE_TOPICS}
 * system property set the class will also track the 'logger name prefix' used when creating a serde
 * to the topic name the serde is asked to handle data for.
 *
 * <p>These two combined can determine the schema and formats per topic.
 *
 * <p>This class is predominately used in the {@code QueryTranslationTest} in the
 * ksql-functional-tests module to ensure the schemas of data persisted to topics doesn't change
 * between releases.
 */
public final class QuerySchemas {

  // Maps logger name prefixes -> Schema Info
  private final LinkedHashMap<String, SchemaInfo> loggerToSchemas = new LinkedHashMap<>();

  // Maps topic name -> (map of key/value flag -> set of logger name prefixes)
  private final Map<String, Map<Boolean, Set<String>>> topicsToLoggers = new HashMap<>();

  /**
   * Called when creating a key serde.
   *
   * <p>Associates a logger name with a schema and key format.
   *
   * @param loggerNamePrefix the logger name prefix used when creating the serde.
   * @param schema the logical schema used when creating the serde.
   * @param keyFormat the key format used when creating the serde.
   */
  public void trackKeySerdeCreation(
      final String loggerNamePrefix,
      final LogicalSchema schema,
      final KeyFormat keyFormat
  ) {
    trackSerde(loggerNamePrefix, schema, Optional.of(keyFormat), Optional.empty());
  }

  /**
   * Called when creating a value serde.
   *
   * <p>Associates a logger name with a schema and value format.
   *
   * @param loggerNamePrefix the logger name prefix used when creating the serde.
   * @param schema the logical schema used when creating the serde.
   * @param valueFormat the value format used when creating the serde.
   */
  public void trackValueSerdeCreation(
      final String loggerNamePrefix,
      final LogicalSchema schema,
      final ValueFormat valueFormat
  ) {
    trackSerde(loggerNamePrefix, schema, Optional.empty(), Optional.of(valueFormat));
  }

  /**
   * Called when a serializer or deserializer does its thing.
   *
   * <p>Associates a topic name with a logger name.
   *
   * @param topicName the name of the topic
   * @param key flag indicating if this relationship comes from a key or value serde
   * @param loggerNamePrefix the logger name
   */
  public synchronized void trackSerdeOp(
      final String topicName,
      final boolean key,
      final String loggerNamePrefix
  ) {
    topicsToLoggers
        .computeIfAbsent(topicName, k1 -> new HashMap<>())
        .computeIfAbsent(key, (k -> new HashSet<>()))
        .add(loggerNamePrefix);
  }

  /**
   * @return map of all logger name prefix to schema and format info.
   */
  public Map<String, SchemaInfo> getLoggerSchemaInfo() {
    return Collections.unmodifiableMap(loggerToSchemas);
  }

  /**
   * Builds complete SchemaInfo for a topic:
   */
  public SchemaInfo getTopicInfo(final String topicName) {
    final Map<Boolean, Set<String>> kvLoggerNames = topicsToLoggers.get(topicName);
    if (kvLoggerNames == null) {
      throw new IllegalArgumentException("Unknown topic: " + topicName);
    }

    final Set<String> keyLoggerNames = kvLoggerNames.getOrDefault(true, ImmutableSet.of());
    if (keyLoggerNames.size() != 1) {
      throw new IllegalStateException("Multiple key logger names registered for topic."
          + System.lineSeparator()
          + "topic: " + topicName
          + "loggers: " + keyLoggerNames
      );
    }

    final Set<String> valueTopicNames = kvLoggerNames.getOrDefault(false, ImmutableSet.of());
    if (valueTopicNames.size() != 1) {
      throw new IllegalStateException("Multiple value logger names registered for topic."
          + System.lineSeparator()
          + "topic: " + topicName
          + "loggers: " + valueTopicNames
      );
    }

    final SchemaInfo keyInfo = loggerToSchemas.get(Iterables.getOnlyElement(keyLoggerNames));
    final SchemaInfo valueInfo = loggerToSchemas.get(Iterables.getOnlyElement(valueTopicNames));
    if (keyInfo == null || valueInfo == null) {
      throw new IllegalStateException("Incomplete schema info for topic."
          + System.lineSeparator()
          + "topic: " + topicName
          + "keyInfo: " + keyInfo
          + "valueInfo: " + valueInfo
      );
    }

    return keyInfo.equals(valueInfo)
        ? valueInfo
        : valueInfo.merge(keyInfo.keyFormat, keyInfo.valueFormat);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QuerySchemas that = (QuerySchemas) o;
    return Objects.equals(loggerToSchemas, that.loggerToSchemas)
        && Objects.equals(topicsToLoggers, that.topicsToLoggers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(loggerToSchemas, topicsToLoggers);
  }

  private void trackSerde(
      final String loggerNamePrefix,
      final LogicalSchema schema,
      final Optional<KeyFormat> keyFormat,
      final Optional<ValueFormat> valueFormat
  ) {
    loggerToSchemas.compute(loggerNamePrefix, (k, existing) -> {
      if (existing == null) {
        return new SchemaInfo(schema, keyFormat, valueFormat);
      }

      if (!existing.schema.equals(schema)) {
        throw new IllegalStateException("Inconsistent schema: "
            + "existing: " + existing.schema + ", new: " + schema);
      }

      return existing.merge(keyFormat, valueFormat);
    });
  }

  @Immutable
  public static final class SchemaInfo {

    private final LogicalSchema schema;
    private final Optional<KeyFormat> keyFormat;
    private final Optional<ValueFormat> valueFormat;

    public SchemaInfo(
        final LogicalSchema schema,
        final Optional<KeyFormat> keyFormat,
        final Optional<ValueFormat> valueFormat
    ) {
      this.schema = requireNonNull(schema, "schema");
      this.keyFormat = requireNonNull(keyFormat, "keyFormat");
      this.valueFormat = requireNonNull(valueFormat, "valueFormat");
    }

    @Override
    public String toString() {
      return "schema=" + schema
          + ", keyFormat=" + keyFormat.map(Object::toString).orElse("?")
          + ", valueFormat=" + valueFormat.map(Object::toString).orElse("?");
    }

    public LogicalSchema schema() {
      return schema;
    }

    public Optional<KeyFormat> keyFormat() {
      return keyFormat;
    }

    public Optional<ValueFormat> valueFormat() {
      return valueFormat;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SchemaInfo that = (SchemaInfo) o;
      return Objects.equals(schema, that.schema)
          && Objects.equals(keyFormat, that.keyFormat)
          && Objects.equals(valueFormat, that.valueFormat);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schema, keyFormat, valueFormat);
    }

    SchemaInfo merge(
        final Optional<KeyFormat> keyFormat,
        final Optional<ValueFormat> valueFormat
    ) {
      if (this.keyFormat.isPresent() && keyFormat.isPresent()) {
        throw new IllegalStateException("key format already set");
      }

      if (this.valueFormat.isPresent() && valueFormat.isPresent()) {
        throw new IllegalStateException("value format already set");
      }

      return new SchemaInfo(
          schema,
          keyFormat.isPresent() ? keyFormat : this.keyFormat,
          valueFormat.isPresent() ? valueFormat : this.valueFormat
      );
    }
  }
}
