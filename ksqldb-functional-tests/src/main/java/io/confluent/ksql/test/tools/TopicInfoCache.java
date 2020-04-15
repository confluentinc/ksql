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

package io.confluent.ksql.test.tools;

import static java.util.Objects.requireNonNull;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.parser.DurationParser;
import io.confluent.ksql.parser.SchemaParser;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;

/**
 * Cache of info known about topics in use in the test.
 */
public class TopicInfoCache {

  private static final Pattern INTERNAL_TOPIC_PATTERN = Pattern
      .compile("_confluent.*query_(.*_\\d+)-.*-(changelog|repartition)");

  private static final Pattern WINDOWED_JOIN_PATTERN = Pattern
      .compile(
          "CREATE .* JOIN .* WITHIN (\\d+ \\w+) ON .*",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL
      );

  private static final Pattern LEFT_INTERNAL = Pattern
      .compile(".*(?:left-repartition|Join-repartition|THIS-\\d+-store-changelog)");

  private static final Pattern RIGHT_INTERNAL = Pattern
      .compile(".*(?:right-repartition|OTHER-\\d+-store-changelog)");

  private final KsqlExecutionContext ksqlEngine;
  private final SchemaRegistryClient srClient;
  private final LoadingCache<String, TopicInfo> cache;

  public TopicInfoCache(
      final KsqlExecutionContext ksqlEngine,
      final SchemaRegistryClient srClient
  ) {
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.srClient = requireNonNull(srClient, "srClient");
    this.cache = CacheBuilder.newBuilder()
        .build(CacheLoader.from(this::load));
  }

  public TopicInfo get(final String topicName) {
    return cache.getUnchecked(topicName);
  }

  public List<TopicInfo> all() {
    return ImmutableList.copyOf(cache.asMap().values());
  }

  public void clear() {
    cache.invalidateAll();
  }

  private TopicInfo load(final String topicName) {
    try {
      final java.util.regex.Matcher matcher = INTERNAL_TOPIC_PATTERN.matcher(topicName);
      if (matcher.matches()) {
        // Internal topic:
        final QueryId queryId = new QueryId(matcher.group(1));
        final PersistentQueryMetadata query = ksqlEngine
            .getPersistentQuery(queryId)
            .orElseThrow(() -> new TestFrameworkException("Unknown queryId for internal topic: "
                + queryId));

        final java.util.regex.Matcher windowedJoinMatcher = WINDOWED_JOIN_PATTERN
            .matcher(query.getStatementString());

        final OptionalLong changeLogWindowSize = topicName.endsWith("-changelog")
            && windowedJoinMatcher.matches()
            ? OptionalLong.of(DurationParser.parse(windowedJoinMatcher.group(1)).toMillis())
            : OptionalLong.empty();

        final LogicalSchema schema = getInternalSchema(topicName, query);

        return new TopicInfo(
            topicName,
            schema,
            query.getResultTopic().getKeyFormat(),
            query.getResultTopic().getValueFormat(),
            changeLogWindowSize
        );
      }

      // Source / sink topic:
      final Set<TopicInfo> keyTypes = ksqlEngine.getMetaStore().getAllDataSources().values()
          .stream()
          .filter(source -> source.getKafkaTopicName().equals(topicName))
          .map(source -> new TopicInfo(
              topicName,
              source.getSchema(),
              source.getKsqlTopic().getKeyFormat(),
              source.getKsqlTopic().getValueFormat(),
              OptionalLong.empty()
          ))
          .collect(Collectors.toSet());

      if (keyTypes.isEmpty()) {
        throw new TestFrameworkException("no source found for topic");
      }

      return Iterables.get(keyTypes, 0);
    } catch (final Exception e) {
      throw new TestFrameworkException("Failed to determine key type for"
          + System.lineSeparator() + "topic: " + topicName
          + System.lineSeparator() + "reason: " + e.getMessage(), e);
    }
  }

  private static LogicalSchema getInternalSchema(
      final String topicName,
      final PersistentQueryMetadata query
  ) {
    if (LEFT_INTERNAL.matcher(topicName).matches()) {
      return getSchemaFromQuery("PROJECT", "PrependAliasLeft", query);
    }

    if (RIGHT_INTERNAL.matcher(topicName).matches()) {
      return getSchemaFromQuery("PROJECT", "PrependAliasRight", query);
    }

    if (topicName.endsWith("Aggregate-Aggregate-Materialize-changelog")) {
      return getSchemaFromQuery("AGGREGATE", "Aggregate\\.Aggregate", query);
    }

    if (topicName.endsWith("Aggregate-GroupBy-repartition")) {
      return getSchemaFromQuery("GROUP_BY", "Aggregate\\.GroupBy", query);
    }

    if (topicName.endsWith("Reduce-changelog")) {
      return getSchemaFromQuery("SOURCE", "KsqlTopic\\.Source", query);
    }

    throw new UnsupportedOperationException("Unsupported internal topics type: " + topicName);
  }

  private static LogicalSchema getSchemaFromQuery(
      final String stepType,
      final String loggerSuffix,
      final PersistentQueryMetadata query
  ) {
    final String executionPlan = query.getExecutionPlan();

    final Pattern pattern = Pattern
        .compile(".*\\[ " + stepType + " ]\\s+\\|\\s+Schema: (.*)\\s+\\|\\s+Logger:\\s+.*\\."
            + loggerSuffix + "\\b.*", Pattern.DOTALL);

    final Matcher matcher = pattern.matcher(executionPlan);
    if (!matcher.find()) {
      throw new IllegalStateException("Failed to determine schema from:"
          + System.lineSeparator()
          + "reason: no matching step found"
          + System.lineSeparator()
          + "stepType: " + stepType
          + System.lineSeparator()
          + "loggerSuffix: " + loggerSuffix
          + System.lineSeparator()
          + "executionPlan: " + executionPlan);
    }

    final String schemaText = matcher.group(1);

    if (matcher.find()) {
      throw new IllegalStateException("Failed to determine schema from:"
          + System.lineSeparator()
          + "reason: multiple lines matched the step, meaning schema is ambiguous"
          + System.lineSeparator()
          + "stepType: " + stepType
          + System.lineSeparator()
          + "loggerSuffix: " + loggerSuffix
          + System.lineSeparator()
          + "executionPlan: " + executionPlan);
    }

    return new SchemaParser(TypeRegistry.EMPTY).parseInternal(schemaText);
  }

  public final class TopicInfo {

    private final String topicName;
    private final LogicalSchema schema;
    private final KeyFormat keyFormat;
    private final ValueFormat valueFormat;
    private final OptionalLong changeLogWindowSize;

    private TopicInfo(
        final String topicName,
        final LogicalSchema schema,
        final KeyFormat keyFormat,
        final ValueFormat valueFormat,
        final OptionalLong changeLogWindowSize
    ) {
      this.topicName = requireNonNull(topicName, "topicName");
      this.schema = requireNonNull(schema, "schema");
      this.keyFormat = requireNonNull(keyFormat, "keyFormat");
      this.valueFormat = requireNonNull(valueFormat, "valueFormat");
      this.changeLogWindowSize = requireNonNull(changeLogWindowSize, "changeLogWindowSize");
    }

    public String getTopicName() {
      return topicName;
    }

    public LogicalSchema getSchema() {
      return schema;
    }

    public KeyFormat getKeyFormat() {
      return keyFormat;
    }

    public ValueFormat getValueFormat() {
      return valueFormat;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Serializer<Object> getKeySerializer() {
      final SerdeSupplier<?> keySerdeSupplier = SerdeUtil
          .getKeySerdeSupplier(keyFormat, schema);

      final Serializer<?> serializer = keySerdeSupplier.getSerializer(srClient);

      serializer.configure(ImmutableMap.of(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
      ), true);

      return (Serializer) serializer;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Serializer<Object> getValueSerializer() {
      final SerdeSupplier<?> valueSerdeSupplier = SerdeUtil
          .getSerdeSupplier(valueFormat.getFormat(), schema);

      final Serializer<?> serializer = valueSerdeSupplier.getSerializer(srClient);

      serializer.configure(ImmutableMap.of(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
      ), false);

      return (Serializer) serializer;
    }

    public Deserializer<?> getKeyDeserializer() {
      final SerdeSupplier<?> keySerdeSupplier = SerdeUtil
          .getKeySerdeSupplier(keyFormat, schema);

      final Deserializer<?> deserializer = keySerdeSupplier.getDeserializer(srClient);

      deserializer.configure(ImmutableMap.of(), true);

      if (!changeLogWindowSize.isPresent()) {
        return deserializer;
      }

      final TimeWindowedDeserializer<?> changeLogDeserializer =
          new TimeWindowedDeserializer<>(deserializer, changeLogWindowSize.getAsLong());

      changeLogDeserializer.setIsChangelogTopic(true);

      return changeLogDeserializer;
    }

    public Deserializer<?> getValueDeserializer() {
      final SerdeSupplier<?> valueSerdeSupplier = SerdeUtil
          .getSerdeSupplier(valueFormat.getFormat(), schema);

      final Deserializer<?> deserializer = valueSerdeSupplier.getDeserializer(srClient);

      deserializer.configure(ImmutableMap.of(), false);

      return deserializer;
    }

    /**
     * Coerce the key value to the correct type.
     *
     * <p>The type of the key loaded from the JSON test case file may not be the exact match on
     * type, e.g. JSON will load a small number as an integer, but the key type of the source might
     * be a long.
     *
     * @param record the record to coerce
     * @param msgIndex the index of the message, displayed in the error message
     * @return a new Record with the correct key type.
     */
    public Record coerceRecordKey(
        final Record record,
        final int msgIndex
    ) {
      try {
        final Object coerced = keyCoercer().apply(record.rawKey());
        return record.withKey(coerced);
      } catch (final Exception e) {
        throw new AssertionError(
            "Topic '" + record.getTopicName() + "', message " + msgIndex
                + ": Invalid test-case: could not coerce key in test case to required type. "
                + e.getMessage(),
            e);
      }
    }

    private Function<Object, Object> keyCoercer() {
      final SqlType keyType = schema
          .key()
          .get(0)
          .type();

      return key -> DefaultSqlValueCoercer.INSTANCE
          .coerce(key, keyType)
          .orElseThrow(() -> new AssertionError("Invalid key value for topic " + topicName + "."
              + System.lineSeparator()
              + "Expected KeyType: " + keyType
              + System.lineSeparator()
              + "Actual KeyType: " + SchemaConverters.javaToSqlConverter()
              .toSqlType(key.getClass())
              + ", key: " + key + "."
              + System.lineSeparator()
              + "This is likely caused by the key type in the test-case not matching the schema."
          ))
          .orElse(null);
    }
  }
}
