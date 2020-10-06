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
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;

/**
 * Cache of info known about topics in use in the test.
 *
 * <p>Info for source and sink topics is obtained by finding a {@link
 * io.confluent.ksql.metastore.model.DataSource} with a matching source topic name in the {@link
 * io.confluent.ksql.metastore.MetaStore}.
 *
 * <p>Info for internal topics is obtained from the {@link PersistentQueryMetadata#getSchemas()}.
 * This is a map of {@code loggerNamePrefix} to {@link SchemaInfo}. This map is populated as a query
 * is built, so presents the <i>actual</i> schema and formats used. This class uses pattern matching
 * against the topic name to determine the correct {@code loggerNamePrefix} to look up and any
 * additional logic needded.
 */
public class TopicInfoCache {

  private static final String TOPIC_PATTERN_PREFIX = "_confluent.*query_(?<queryId>.*_\\d+)-";

  private static final List<InternalTopicPattern> INTERNAL_TOPIC_PATTERNS = ImmutableList.of(
      // GROUP BY repartition topics:
      new TopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "(?<name>Aggregate-.*)-repartition"),
          matcher -> matcher.group("queryId") + "." + matcher.group("name").replace("-", ".")
      ),
      // GROUP BY change-logs and repartition topics:
      new GroupByChangeLogPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "(?<name>Aggregate-.*)-changelog"),
          matcher -> matcher.group("queryId") + "." + matcher.group("name").replace("-", ".")
      ),
      // Suppress change-logs:
      new TopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "Suppress-store-changelog"),
          matcher -> matcher.group("queryId") + ".Suppress.Suppress"
      ),
      // Table changelog:
      new TopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "KsqlTopic-Reduce-changelog"),
          matcher -> matcher.group("queryId") + ".KsqlTopic.Source"
      ),
      new TopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "KafkaTopic_(?<name>.*)-Reduce-changelog"),
          matcher -> matcher.group("queryId") + ".KafkaTopic_" + matcher.group("name") + ".Source"
      ),
      // Pre-join repartition:
      new TopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "(?<side1>.*Join)-(?<side2>.*)-repartition"),
          matcher -> matcher.group("queryId") + "." + matcher.group("side1"),
          matcher -> matcher.group("queryId") + "." + matcher.group("side1") + "."
              + capitalizeFirst(matcher.group("side2"))
      ),
      new TopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "(?<side>.*)-repartition"),
          matcher -> matcher.group("queryId") + "." + matcher.group("side"),
          matcher -> matcher.group("queryId") + "." + matcher.group("side") + ".Left"
      ),
      // Stream-stream join state store change-logs:
      new StreamStreamJoinChangeLogPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + "(?<storeName>KSTREAM-\\w+-\\d+-store)-changelog")
      ),
      // Catch all
      new TopicPattern(
          Pattern.compile(TOPIC_PATTERN_PREFIX + ".*-(changelog|repartition)"),
          matcher -> {
            throw new TestFrameworkException("Unknown internal topic pattern: " + matcher.group(0));
          }
      )
  );

  private static final Pattern WINDOWED_JOIN_PATTERN = Pattern
      .compile(
          ".*\\bSELECT\\b.*\\bJOIN\\b.*\\bWITHIN\\b\\s*(?<duration>\\d+\\s+\\w+)\\s*\\bON\\b.*",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL
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
          .map(p -> p.match(topicName))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst();

      if (internalTopic.isPresent()) {
        // Internal topic:
        final PersistentQueryMetadata query = ksqlEngine
            .getPersistentQuery(internalTopic.get().queryId())
            .orElseThrow(() -> new TestFrameworkException("Unknown queryId for internal topic: "
                + internalTopic.get().queryId())
            );

        final java.util.regex.Matcher windowedJoinMatcher = WINDOWED_JOIN_PATTERN
            .matcher(query.getStatementString());

        final OptionalLong changeLogWindowSize = topicName.endsWith("-changelog")
            && windowedJoinMatcher.matches()
            ? OptionalLong
            .of(DurationParser.parse(windowedJoinMatcher.group("duration")).toMillis())
            : OptionalLong.empty();

        return new TopicInfo(
            topicName,
            query.getLogicalSchema(),
            internalTopic.get().keyFormat(query),
            internalTopic.get().valueFormat(query),
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

  private static String capitalizeFirst(final String text) {
    return text.substring(0, 1).toUpperCase() + text.substring(1);
  }

  private static Processor findFirstProcessorWithStore(
      final PersistentQueryMetadata query,
      final String storeName
  ) {
    final TopologyDescription description = query.getTopology().describe();
    return description.subtopologies().stream()
        .flatMap(subtopology -> subtopology.nodes().stream())
        .filter(node -> node instanceof TopologyDescription.Processor)
        .map(TopologyDescription.Processor.class::cast)
        .filter(node -> node.stores().contains(storeName))
        .findFirst()
        .orElseThrow(() -> new TestFrameworkException(
            "Processor node with store not found. store: " + storeName));
  }

  private interface InternalTopic {

    QueryId queryId();

    KeyFormat keyFormat(PersistentQueryMetadata query);

    ValueFormat valueFormat(PersistentQueryMetadata query);
  }

  /**
   * Impl that gets the topic info from the {@link io.confluent.ksql.schema.query.QuerySchemas}.
   */
  private static class QuerySchemasInternalTopic implements InternalTopic {

    private final QueryId queryId;
    private final Function<PersistentQueryMetadata, String> keyLoggerNameSupplier;
    private final Function<PersistentQueryMetadata, String> valueLoggerNameSupplier;

    QuerySchemasInternalTopic(
        final QueryId queryId,
        final Function<PersistentQueryMetadata, String> keyLoggerNameSupplier,
        final Function<PersistentQueryMetadata, String> valueLoggerNameSupplier
    ) {
      this.queryId = requireNonNull(queryId, "queryId");
      this.keyLoggerNameSupplier = requireNonNull(keyLoggerNameSupplier, "loggerNamePrefix");
      this.valueLoggerNameSupplier = requireNonNull(valueLoggerNameSupplier,
          "valueLoggerNamePrefix");
    }

    @Override
    public QueryId queryId() {
      return queryId;
    }

    @Override
    public KeyFormat keyFormat(final PersistentQueryMetadata query) {
      final String loggerNamePrefix = keyLoggerNameSupplier.apply(query);

      return getSchemaInfo(query, loggerNamePrefix)
          .keyFormat()
          .orElseThrow(() -> new TestFrameworkException(
              "No key schema registered for schema name: " + loggerNamePrefix));
    }

    @Override
    public ValueFormat valueFormat(final PersistentQueryMetadata query) {
      final String loggerNamePrefix = valueLoggerNameSupplier.apply(query);

      return getSchemaInfo(query, loggerNamePrefix)
          .valueFormat()
          .orElseThrow(() -> new TestFrameworkException(
              "No value schema registered for schema name: " + loggerNamePrefix));
    }

    private static SchemaInfo getSchemaInfo(
        final PersistentQueryMetadata query,
        final String loggerNamePrefix
    ) {
      final SchemaInfo schemaInfo = query.getSchemas().get(loggerNamePrefix);
      if (schemaInfo == null) {
        throw new TestFrameworkException(
            "Unknown schema name for internal topic: " + loggerNamePrefix);
      }

      return schemaInfo;
    }
  }

  private interface InternalTopicPattern {

    Optional<InternalTopic> match(String topicName);
  }

  private static class TopicPattern implements InternalTopicPattern {

    private final Pattern pattern;
    private final Function<Matcher, String> keyMapper;
    private final Function<Matcher, String> valueMapper;

    TopicPattern(final Pattern pattern, final Function<Matcher, String> mapper) {
      this(pattern, mapper, mapper);
    }

    TopicPattern(
        final Pattern pattern,
        final Function<Matcher, String> keyMapper,
        final Function<Matcher, String> valueMapper
    ) {
      this.pattern = requireNonNull(pattern, "pattern");
      this.keyMapper = requireNonNull(keyMapper, "keyMapper");
      this.valueMapper = requireNonNull(valueMapper, "valueMapper");
    }

    @Override
    public Optional<InternalTopic> match(final String topicName) {
      final Matcher matcher = pattern.matcher(topicName);
      if (!matcher.matches()) {
        return Optional.empty();
      }

      final QueryId queryId = new QueryId(matcher.group("queryId"));

      final String keyName = keyMapper.apply(matcher);
      final String valueName = valueMapper.apply(matcher);
      return Optional.of(new QuerySchemasInternalTopic(
          queryId,
          query -> keyName,
          query -> valueName
      ));
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
  private static class GroupByChangeLogPattern implements InternalTopicPattern {

    // WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY
    private static final Pattern WINDOWED_GROUP_BY_PATTERN = Pattern
        .compile(
            ".*\\b+WINDOW\\s+(?<windowType>\\w+)\\s+"
                + "\\(\\s+(:?SIZE\\s+)?(?<duration>\\d+\\s+\\w+)[^)]*\\).*\\bGROUP\\s+BY.*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );

    private final TopicPattern topicPattern;

    GroupByChangeLogPattern(
        final Pattern pattern,
        final Function<Matcher, String> mapper
    ) {
      topicPattern = new TopicPattern(pattern, mapper);
    }

    @Override
    public Optional<InternalTopic> match(final String topicName) {
      return topicPattern.match(topicName)
          .map(GroupByChangeLongInternalTopic::new);
    }

    private static class GroupByChangeLongInternalTopic implements InternalTopic {

      private final InternalTopic basicInfo;

      GroupByChangeLongInternalTopic(final InternalTopic basicInfo) {
        this.basicInfo = requireNonNull(basicInfo, "basicInfo");
      }

      @Override
      public QueryId queryId() {
        return basicInfo.queryId();
      }

      @Override
      public KeyFormat keyFormat(final PersistentQueryMetadata query) {
        final KeyFormat keyFormat = basicInfo.keyFormat(query);

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

      @Override
      public ValueFormat valueFormat(final PersistentQueryMetadata query) {
        return basicInfo.valueFormat(query);
      }
    }
  }

  /**
   * Pattern for change logs backing the state stores used in stream-stream joins.
   *
   * <p>Determining the format requires inspecting at the name of the processor.
   */
  private static class StreamStreamJoinChangeLogPattern implements InternalTopicPattern {

    private static final Pattern NODE_NAME_PATTERN = Pattern
        .compile("(?<side1>.*)-(?<side2>this|other).*");

    private final Pattern pattern;

    StreamStreamJoinChangeLogPattern(
        final Pattern pattern
    ) {
      this.pattern = requireNonNull(pattern, "pattern");
    }

    @Override
    public Optional<InternalTopic> match(final String topicName) {
      final Matcher matcher = pattern.matcher(topicName);
      if (!matcher.matches()) {
        return Optional.empty();
      }

      final QueryId queryId = new QueryId(matcher.group("queryId"));
      final String storeName = matcher.group("storeName");

      return Optional.of(new QuerySchemasInternalTopic(
          queryId,
          query -> getKeyLoggerName(queryId, storeName, query),
          query -> getValueLoggerName(queryId, storeName, query)
      ));
    }

    private static String getKeyLoggerName(
        final QueryId queryId,
        final String storeName,
        final PersistentQueryMetadata query
    ) {
      final String nodeName = findFirstProcessorWithStore(query, storeName).name();
      final Matcher matcher = NODE_NAME_PATTERN.matcher(nodeName);
      if (!matcher.matches()) {
        throw new TestFrameworkException("node name did not match pattern. "
            + "name: " + nodeName + ", pattern: " + NODE_NAME_PATTERN);
      }

      return queryId + "." + matcher.group("side1");
    }

    private static String getValueLoggerName(
        final QueryId queryId,
        final String storeName,
        final PersistentQueryMetadata query
    ) {
      final String nodeName = findFirstProcessorWithStore(query, storeName).name();
      final Matcher matcher = NODE_NAME_PATTERN.matcher(nodeName);
      if (!matcher.matches()) {
        throw new TestFrameworkException("node name did not match pattern. "
            + "name: " + nodeName + ", pattern: " + NODE_NAME_PATTERN);
      }

      final String side1 = matcher.group("side1");
      final String side2 = matcher.group("side2");
      return queryId + "." + side1 + "." + (side2.equals("this") ? "Left" : "Right");
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

      final Serializer<?> serializer = keySerdeSupplier.getSerializer(srClient);

      serializer.configure(ImmutableMap.of(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
      ), true);

      return (Serializer) serializer;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Serializer<Object> getValueSerializer() {
      final SerdeSupplier<?> valueSerdeSupplier = SerdeUtil
          .getSerdeSupplier(FormatFactory.of(valueFormat.getFormatInfo()), schema);

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
          .getSerdeSupplier(FormatFactory.of(valueFormat.getFormatInfo()), schema);

      final Deserializer<?> deserializer = valueSerdeSupplier.getDeserializer(srClient);

      deserializer.configure(ImmutableMap.of(), false);

      return deserializer;
    }
  }
}
