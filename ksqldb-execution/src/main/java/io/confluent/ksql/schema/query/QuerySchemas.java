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
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import java.util.stream.Collectors;

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

  private static final boolean IS_KEY_SCHEMA = true;
  private static final boolean IS_VALUE_SCHEMA = false;

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

  Map<String, Map<Boolean, Set<String>>> getTopicsToLoggers() {
    return Collections.unmodifiableMap(topicsToLoggers);
  }

  /**
   * Returns all different schemas and serde formats that the {@code topicName} is using. The
   * {@link MultiSchemaInfo} contains a list of key/value schemas and serde formats that were
   * tracked for the specified topic.
   * </p>
   * Key and/or value schemas may be empty if no schemas were detected.
   */
  public MultiSchemaInfo getTopicInfo(final String topicName) {
    final Set<SchemaInfo> keySchemasInfo = getTopicSchemas(topicName, IS_KEY_SCHEMA);
    final Set<SchemaInfo> valueSchemasInfo = getTopicSchemas(topicName, IS_VALUE_SCHEMA);

    return new MultiSchemaInfo(keySchemasInfo, valueSchemasInfo);
  }

  /**
   * Returns a list of schemas and serde formats detected on the specified topicName.
   * The isKeySchema is a boolean value that specifies whether to look at the key schema (True)
   * or at the value schema (False).
   * </p>
   * Each schema information is tracked by the loggers the topicName has. Some topics may have
   * multiple schemas and serde formats if the topicName is used with stream-stream joins and
   * foreign key joins.
   */
  private Set<SchemaInfo> getTopicSchemas(final String topicName,
                                          final boolean isKeySchema) {
    // Look at the different loggers that tne topicName uses. There might be multiple
    // loggers if the topicName was used by joins, which contains internal state stores.
    final Map<Boolean, Set<String>> kvLoggerNames = topicsToLoggers.get(topicName);
    if (kvLoggerNames == null) {
      throw new IllegalArgumentException("Unknown topic: " + topicName);
    }

    final Map<SchemaInfo, Set<String>> schemaToLoggers = new HashMap<>();

    // Look at the loggers linked to the key or value serde, and get all schema and formats
    // detected.
    for (final String loggerName : kvLoggerNames.getOrDefault(isKeySchema, ImmutableSet.of())) {
      final SchemaInfo schemaInfo = loggerToSchemas.get(loggerName);
      if (schemaInfo != null) {
        schemaToLoggers.computeIfAbsent(schemaInfo, k -> new HashSet<>()).add(loggerName);
      }
    }

    return schemaToLoggers.keySet();
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

  public static class MultiSchemaInfo {
    private final Set<SchemaInfo> keySchemas;
    private final Set<SchemaInfo> valueSchemas;
    private final Set<KeyFormat> keyFormats;
    private final Set<ValueFormat> valueFormats;

    public MultiSchemaInfo(final Set<SchemaInfo> keySchemas, final Set<SchemaInfo> valueSchemas) {
      this.keySchemas = ImmutableSet.copyOf(requireNonNull(keySchemas, "keySchemas"));
      this.valueSchemas = ImmutableSet.copyOf(requireNonNull(valueSchemas, "valuesSchemas"));

      keyFormats = ImmutableSet.copyOf(keySchemas.stream()
          .map(SchemaInfo::keyFormat)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toSet()));

      valueFormats = ImmutableSet.copyOf(valueSchemas.stream()
          .map(SchemaInfo::valueFormat)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toSet()));
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "keySchemas is ImmutableSet")
    public Set<SchemaInfo> getKeySchemas() {
      return keySchemas;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "valueSchemas is ImmutableSet")
    public Set<SchemaInfo> getValueSchemas() {
      return valueSchemas;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "keyFormats is ImmutableSet")
    public Set<KeyFormat> getKeyFormats() {
      return keyFormats;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "valueFormats is ImmutableSet")
    public Set<ValueFormat> getValueFormats() {
      return valueFormats;
    }
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
