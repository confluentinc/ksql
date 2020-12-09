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
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.DurationParser;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas.SchemaInfo;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
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
 *
 * <p>Info for source and sink topics is obtained by finding a {@link
 * io.confluent.ksql.metastore.model.DataSource} with a matching source topic name in the {@link
 * io.confluent.ksql.metastore.MetaStore}.
 *
 * <p>Info for internal topics is obtained from the {@link
 * PersistentQueryMetadata#getQuerySchemas()}. This is a map of {@code loggerNamePrefix} to {@link
 * SchemaInfo}. This map is populated as a query is built, so presents the <i>actual</i> schema and
 * formats used. This class uses pattern matching against the topic name to determine the correct
 * {@code loggerNamePrefix} to look up and any additional logic needded.
 */
public class TopicInfoCache {

  private static final String TOPIC_PATTERN_PREFIX = "_confluent.*query_(?<queryId>.*_\\d+)-";

  private static final List<InternalTopicPattern> INTERNAL_TOPIC_PATTERNS = ImmutableList.of(
      // GROUP BY change-logs and repartition topics:
      new InternalTopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "Aggregate-.*-changelog"),
          GroupByChangeLogPattern::new
      ),
      // Stream-stream join state store change-logs:
      new InternalTopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "KSTREAM-\\w+-\\d+-store-changelog"),
          StreamStreamJoinChangeLogPattern::new
      ),
      // Catch all
      new InternalTopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + ".*-(changelog|repartition)"),
          InternalTopic::new
      )
  );

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
      final Optional<InternalTopic> internalTopic = INTERNAL_TOPIC_PATTERNS.stream()
          .map(e -> e.match(topicName))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst();

      if (internalTopic.isPresent()) {
        // Internal topic:
        final QueryId queryId = internalTopic.get().queryId();

        final PersistentQueryMetadata query = ksqlEngine
            .getPersistentQuery(queryId)
            .orElseThrow(() ->
                new TestFrameworkException("Unknown queryId for internal topic: " + queryId));

        final SchemaInfo schemaInfo = query.getQuerySchemas().getTopicInfo(topicName);

        final KeyFormat keyFormat = schemaInfo.keyFormat().orElseThrow(IllegalStateException::new);

        return new TopicInfo(
            topicName,
            query.getLogicalSchema(),
            internalTopic.get().keyFormat(keyFormat, query),
            schemaInfo.valueFormat().orElseThrow(IllegalStateException::new),
            internalTopic.get().changeLogWindowSize(query)
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
        throw new TestFrameworkException("No information found for topic '"
            + topicName + "'. Available topics: " + cache.asMap().keySet());
      }

      return Iterables.get(keyTypes, 0);
    } catch (final Exception e) {
      throw new TestFrameworkException("Failed to determine key type for"
          + System.lineSeparator() + "topic: " + topicName
          + System.lineSeparator() + "reason: " + e.getMessage(), e);
    }
  }

  private static class InternalTopic {

    private final QueryId queryId;

    InternalTopic(final Matcher matcher) {
      this.queryId = new QueryId(matcher.group("queryId"));
    }

    QueryId queryId() {
      return queryId;
    }

    /**
     * Gives the pattern a chance to adjust the key format
     */
    KeyFormat keyFormat(final KeyFormat baseFormat, final PersistentQueryMetadata query) {
      return baseFormat;
    }

    /**
     * Used by stream-stream join changelogs of windowed stream, where the statestore key is
     * double-wrapped in {@link org.apache.kafka.streams.kstream.Windowed}. This can't be
     * represented using {@link KeyFormat} alone.
     */
    OptionalLong changeLogWindowSize(final PersistentQueryMetadata query) {
      return OptionalLong.empty();
    }
  }

  private static class InternalTopicPattern {

    private final Pattern pattern;
    private final Function<Matcher, InternalTopic> topicFactory;

    InternalTopicPattern(
        final Pattern pattern,
        final Function<Matcher, InternalTopic> topicFactory
    ) {
      this.pattern = requireNonNull(pattern, "pattern");
      this.topicFactory = requireNonNull(topicFactory, "topicFactory");
    }

    Optional<InternalTopic> match(final String topicName) {
      final Matcher matcher = pattern.matcher(topicName);
      if (!matcher.matches()) {
        return Optional.empty();
      }

      return Optional.of(topicFactory.apply(matcher));
    }
  }

  /**
   * Pattern for aggregate change logs.
   *
   * <p>Windowed Aggregates, e.g
   *
   * <pre>
   * {@code
   *   CREATE TABLE FOO AS
   *     SELECT ID, COUNT()
   *     FROM BAR
   *     WINDOW TUMBLING (SIZE 30 SECONDS)
   *     GROUP BY ID;
   * }
   * </pre>
   *
   * <p>The test key serde created and passed to Kafka Streams for the change log is <i>not</i> a
   * windowed serde. Kafka Streams handles that part. This class ensures the windowed part is
   * added.
   */
  private static class GroupByChangeLogPattern extends InternalTopic {

    private static final Pattern WINDOWED_GROUP_BY_PATTERN = Pattern
        .compile(
            ".*\\b+WINDOW\\s+(?<windowType>\\w+)\\s+"
                + "\\(\\s+(:?SIZE\\s+)?(?<duration>\\d+\\s+\\w+)[^)]*\\).*\\bGROUP\\s+BY.*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

    GroupByChangeLogPattern(final Matcher matcher) {
      super(matcher);
    }

    @Override
    public KeyFormat keyFormat(final KeyFormat keyFormat, final PersistentQueryMetadata query) {
      final Matcher matcher = WINDOWED_GROUP_BY_PATTERN.matcher(query.getStatementString());
      if (!matcher.matches()) {
        return keyFormat;
      }

      final WindowType windowType = WindowType.of(matcher.group("windowType"));
      final Optional<Duration> duration = windowType.requiresWindowSize()
          ? Optional.of(DurationParser.parse(matcher.group("duration")))
          : Optional.empty();

      return KeyFormat.windowed(
          keyFormat.getFormatInfo(),
          keyFormat.getFeatures(),
          WindowInfo.of(windowType, duration)
      );
    }
  }

  /**
   * Pattern for change logs backing the state stores used in stream-stream joins.
   *
   * <p>Stream-stream joins between windowed sources have a windowed key format, and the state
   * stores and changelogs used during the join wrap throws windowed keys in another layer of
   * windowing. This can't be expressed in {@link KeyFormat}. Instead, this class extracts the
   * changelog window size from the statement, and this is used later to double wrap the raw key
   * serde.
   */
  private static class StreamStreamJoinChangeLogPattern extends InternalTopic {

    private static final Pattern WINDOWED_JOIN_PATTERN = Pattern.compile(
        ".*\\bSELECT\\b.*\\bJOIN\\b.*\\bWITHIN\\b\\s*(?<duration>\\d+\\s+\\w+)\\s*\\bON\\b.*",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    private final String topicName;

    StreamStreamJoinChangeLogPattern(final Matcher matcher) {
      super(matcher);
      this.topicName = matcher.group(0);
    }

    @Override
    OptionalLong changeLogWindowSize(final PersistentQueryMetadata query) {
      if (!topicName.endsWith("-changelog")) {
        return OptionalLong.empty();
      }

      final Matcher windowedJoinMatcher = WINDOWED_JOIN_PATTERN
          .matcher(query.getStatementString());

      if (!windowedJoinMatcher.matches()) {
        return OptionalLong.empty();
      }

      return OptionalLong
          .of(DurationParser.parse(windowedJoinMatcher.group("duration")).toMillis());
    }
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

      final Serializer<?> serializer = keySerdeSupplier.getSerializer(srClient, true);

      serializer.configure(ImmutableMap.of(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
      ), true);

      return (Serializer) serializer;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Serializer<Object> getValueSerializer() {
      final SerdeSupplier<?> valueSerdeSupplier = SerdeUtil
          .getSerdeSupplier(FormatFactory.of(valueFormat.getFormatInfo()), schema);

      final Serializer<?> serializer = valueSerdeSupplier.getSerializer(srClient, false);

      serializer.configure(ImmutableMap.of(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
      ), false);

      return (Serializer) serializer;
    }

    public Deserializer<?> getKeyDeserializer() {
      final SerdeSupplier<?> keySerdeSupplier = SerdeUtil
          .getKeySerdeSupplier(keyFormat, schema);

      final Deserializer<?> deserializer = keySerdeSupplier.getDeserializer(srClient, true);

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
          .getSerdeSupplier(FormatFactory.of(valueFormat.getFormatInfo()), schema);

      final Deserializer<?> deserializer = valueSerdeSupplier.getDeserializer(srClient, false);

      deserializer.configure(ImmutableMap.of(), false);

      return deserializer;
    }
  }
}
