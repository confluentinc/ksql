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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.hasItems;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Pattern;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class PostConditionsNode {

  private final List<SourceNode> sources;
  private final Optional<PostTopicsNode> topics;

  public PostConditionsNode(
      @JsonProperty("sources") final List<SourceNode> sources,
      @JsonProperty("topics") final Optional<PostTopicsNode> topics
  ) {
    this.sources = sources == null ? ImmutableList.of() : ImmutableList.copyOf(sources);
    this.topics = Objects.requireNonNull(topics, "topics");
  }

  @SuppressWarnings("unused") // Invoked via reflection
  public List<SourceNode> getSources() {
    return sources;
  }

  @SuppressWarnings("unused") // Invoked via reflection
  public Optional<PostTopicsNode> getTopics() {
    return topics;
  }

  @SuppressWarnings("unchecked")
  public PostConditions build() {
    final Matcher<DataSource>[] matchers = sources.stream()
        .map(SourceNode::build)
        .toArray(Matcher[]::new);

    final Matcher<Iterable<DataSource>> sourcesMatcher = hasItems(matchers);

    final PostTopicsNode topicsNode = topics.orElseGet(PostTopicsNode::new);

    final Pattern blackListPattern = topicsNode.buildBlackList();

    final Matcher<Iterable<PostTopicNode>> topicsMatcher = topicsNode.buildTopics();

    return new PostConditions(sourcesMatcher, topicsMatcher, blackListPattern, this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PostConditionsNode that = (PostConditionsNode) o;
    return sources.equals(that.sources)
        && topics.equals(that.topics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sources, topics);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class PostTopicsNode {

    private final Optional<String> blackList;
    private final List<PostTopicNode> topics;

    public PostTopicsNode() {
      this(Optional.empty(), Optional.empty());
    }

    public PostTopicsNode(
        @JsonProperty("blacklist") final Optional<String> blackList,
        @JsonProperty("topics") final Optional<List<PostTopicNode>> topics
    ) {
      this.blackList = Objects.requireNonNull(blackList, "blackList");
      this.topics = topics.orElseGet(ImmutableList::of);

      // Fail early
      buildBlackList();
    }

    public Optional<String> getBlackList() {
      return blackList;
    }

    public List<PostTopicNode> getTopics() {
      return topics;
    }

    Pattern buildBlackList() {
      try {
        return Pattern.compile(blackList.orElse(PostConditions.MATCH_NOTHING));
      } catch (final Exception e) {
        throw new InvalidFieldException("blacklist", "not valid regex", e);
      }
    }

    @SuppressWarnings("unchecked")
    Matcher<Iterable<PostTopicNode>> buildTopics() {
      final Matcher<PostTopicNode>[] matchers = topics.stream()
          .map(Matchers::is)
          .toArray(Matcher[]::new);

      return hasItems(matchers);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final PostTopicsNode that = (PostTopicsNode) o;
      return blackList.equals(that.blackList)
          && topics.equals(that.topics);
    }

    @Override
    public int hashCode() {
      return Objects.hash(blackList, topics);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class PostTopicNode {

    private final String name;
    private final KeyFormat keyFormat;
    private final ValueFormat valueFormat;
    private final OptionalInt partitions;

    public PostTopicNode(
        @JsonProperty(value = "name", required = true) final String name,
        @JsonProperty(value = "keyFormat", required = true) final KeyFormat keyFormat,
        @JsonProperty(value = "valueFormat", required = true) final ValueFormat valueFormat,
        @JsonProperty(value = "partitions") final OptionalInt partitions
    ) {
      this.name = requireNonNull(name, "name");
      this.keyFormat = requireNonNull(keyFormat, "KeyFormat");
      this.valueFormat = requireNonNull(valueFormat, "valueFormat");
      this.partitions = requireNonNull(partitions, "partitions");

      if (this.name.isEmpty()) {
        throw new InvalidFieldException("name", "empty or missing");
      }

      if (partitions.isPresent() && partitions.getAsInt() < 1) {
        throw new IllegalArgumentException("Partition count must be positive, but was: "
            + partitions);
      }
    }

    public String getName() {
      return name;
    }

    public KeyFormat getKeyFormat() {
      return keyFormat;
    }

    public ValueFormat getValueFormat() {
      return valueFormat;
    }

    public OptionalInt getPartitions() {
      return partitions;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final PostTopicNode that = (PostTopicNode) o;
      return Objects.equals(name, that.name)
          && Objects.equals(keyFormat, that.keyFormat)
          && Objects.equals(valueFormat, that.valueFormat)
          && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, keyFormat, valueFormat, partitions);
    }

    @Override
    public String toString() {
      return "Topic{"
          + "name='" + name + '\''
          + ", keyFormat=" + keyFormat
          + ", valueFormat=" + valueFormat
          + ", partitions=" + partitions
          + '}';
    }
  }
}
