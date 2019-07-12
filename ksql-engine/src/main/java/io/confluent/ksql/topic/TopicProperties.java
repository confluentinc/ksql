/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.topic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.TopicDescription;

/**
 * A container for all properties required for creating/validating
 * a kafka topic.
 */
public final class TopicProperties {

  public static final short DEFAULT_REPLICAS = -1;

  private static final String INVALID_TOPIC_NAME = ":INVALID:";
  private static final int INVALID_PARTITIONS = -1;

  private final String topicName;
  private final Integer partitions;
  private final Short replicas;

  @VisibleForTesting
  TopicProperties(
      final String topicName,
      final Integer partitions,
      final Short replicas
  ) {
    this.topicName = topicName;
    this.partitions = partitions;
    this.replicas = replicas;
  }

  @Override
  public String toString() {
    return "TopicProperties{" + "topicName='" + getTopicName() + '\''
        + ", partitions=" + getPartitions()
        + ", replicas=" + getReplicas()
        + '}';
  }

  public String getTopicName() {
    return topicName == null ? INVALID_TOPIC_NAME : topicName;
  }

  public int getPartitions() {
    return partitions == null ? INVALID_PARTITIONS : partitions;
  }

  public short getReplicas() {
    return replicas == null ? DEFAULT_REPLICAS : replicas;
  }

  /**
   * Constructs a {@link TopicProperties} with the following precedence order:
   *
   * <ul>
   *   <li>The statement itself, if it has a WITH clause</li>
   *   <li>The overrides, if present (note that this is a legacy approach)</li>
   *   <li>The KsqlConfig property, if present (note that this is a legacy approach)</li>
   *   <li>The topic properties from the source that it is reading from. If the source is a join,
   *   then the left value is used as the source.</li>
   *   <li>Generated based on some recipe - this is the case for topic name, which will never
   *   use the source topic</li>
   * </ul>
   *
   * <p>It is possible that only partial information exists at higher levels of precedence. If
   * this is the case, the values will be inferred in cascading fashion (e.g. topic name from
   * WITH clause, replicas from property overrides and partitions source topic).</p>
   */
  public static final class Builder {

    private String name;
    private TopicProperties fromWithClause = new TopicProperties(null, null, null);
    private TopicProperties fromOverrides = new TopicProperties(null, null, null);
    private TopicProperties fromKsqlConfig = new TopicProperties(null, null, null);
    private Supplier<TopicProperties> fromSource = () -> new TopicProperties(null, null, null);

    Builder withName(final String name) {
      this.name = name;
      return this;
    }

    Builder withWithClause(
        final Optional<String> name,
        final Optional<Integer> partitionCount,
        final Optional<Short> replicationFactor
    ) {
      fromWithClause = new TopicProperties(
          name.orElse(null),
          partitionCount.orElse(null),
          replicationFactor.orElse(null)
      );
      return this;
    }

    Builder withOverrides(final Map<String, Object> overrides) {
      final Integer partitions = parsePartitionsOverride(
          overrides.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY));

      final Short replicas = parseReplicasOverride(
          overrides.get(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY));

      fromOverrides = new TopicProperties(null, partitions, replicas);
      return this;
    }

    Builder withKsqlConfig(final KsqlConfig config) {
      // requires check for containsKey because `getInt` will return 0 otherwise
      Integer partitions = null;
      if (config.values().containsKey(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY)) {
        partitions = config.getInt(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY);
      }

      // requires check for containsKey because `getShort` will return 0 otherwise
      Short replicas = null;
      if (config.values().containsKey(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)) {
        replicas = config.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY);
      }

      fromKsqlConfig = new TopicProperties(null, partitions, replicas);
      return this;
    }

    Builder withSource(final Supplier<TopicDescription> descriptionSupplier) {
      fromSource = Suppliers.memoize(() -> {
        final TopicDescription description = descriptionSupplier.get();
        final Integer partitions = description.partitions().size();
        final Short replicas = (short) description.partitions().get(0).replicas().size();

        return new TopicProperties(null, partitions, replicas);
      });
      return this;
    }

    public TopicProperties build() {
      // this method should use the field directly instead of accessors to force null checks

      final String name = ObjectUtils.firstNonNull(fromWithClause.topicName, this.name);
      Objects.requireNonNull(name, "Was not supplied with any valid source for topic name!");
      if (StringUtils.strip(name).isEmpty()) {
        throw new KsqlException("Must have non-empty topic name.");
      }

      final Integer partitions = Stream.of(
          fromWithClause.partitions,
          fromOverrides.partitions,
          fromKsqlConfig.partitions)
          .filter(Objects::nonNull)
          .findFirst()
          .orElseGet(() -> fromSource.get().partitions);
      if (partitions == null) {
        throw new KsqlException("Cannot determine partitions for creating topic " + name);
      }

      final Short replicas = Stream.of(
          fromWithClause.replicas,
          fromOverrides.replicas,
          fromKsqlConfig.replicas)
          .filter(Objects::nonNull)
          .findFirst()
          .orElseGet(() -> fromSource.get().replicas);

      return new TopicProperties(name, partitions, replicas);
    }
  }

  private static Integer parsePartitionsOverride(final Object value) {
    if (value instanceof Integer || value == null) {
      return (Integer) value;
    }

    try {
      return Integer.parseInt(value.toString());
    } catch (final Exception e) {
      throw new KsqlException("Failed to parse property override '"
          + KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY + "': " + e.getMessage(), e);
    }
  }

  private static Short parseReplicasOverride(final Object value) {
    if (value instanceof Short || value == null) {
      return (Short) value;
    }

    try {
      return Short.parseShort(value.toString());
    } catch (final Exception e) {
      throw new KsqlException("Failed to parse property override '"
          + KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY + "': " + e.getMessage(), e);
    }
  }
}
