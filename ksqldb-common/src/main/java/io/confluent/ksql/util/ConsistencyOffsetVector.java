/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a consistency token that is communicated between the client and server.
 * This token is used to ensure monotonic reads for table pull queries as in: once a client has
 * received a row with an offset x, subsequent reads will only return rows of offsets > x.
 */
//@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsistencyOffsetVector implements OffsetVector {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.registerModule(new Jdk8Module());
  }

  // This field should be the first
  private int version;
  // Topic -> Partition -> Offset
  private Map<String, Map<Integer, Long>> offsetVector;
  private ReadWriteLock rwLock;

  public ConsistencyOffsetVector() {
    this.version = 0;
    this.offsetVector = new HashMap<>();
    this.rwLock = new ReentrantReadWriteLock();
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  @JsonCreator
  public ConsistencyOffsetVector(
      final @JsonProperty(value = "version", required = true) int version,
      final @JsonProperty(value = "offsetVector", required = true)
          Map<String, Map<Integer, Long>> offsetVector
  ) {
    this.version = version;
    this.offsetVector = offsetVector;
    this.rwLock = new ReentrantReadWriteLock();
  }

  public int getVersion() {
    return version;
  }

  public ImmutableMap<String, Map<Integer, Long>> getOffsetVector() {
    return ImmutableMap.copyOf(offsetVector);
  }

  public void addTopicOffsets(final String topicID, final Map<Integer, Long> offsets) {
    offsetVector.putIfAbsent(topicID, new HashMap<>());
    offsetVector.get(topicID).putAll(offsets);
  }

  public void setVersion(final int newVersion) {
    version = newVersion;
  }

  @JsonAnySetter
  public void setOffsetVector(final Map<String, Map<Integer, Long>> topicOffsets) {
    offsetVector.putAll(topicOffsets);
  }

  public void update(final String topic, final int partition, final long offset) {

    try {
      rwLock.writeLock().lock();
      if (!offsetVector.containsKey(topic)) {
        final HashMap<Integer, Long> partitionOffsets = new HashMap<>();
        offsetVector.put(topic, partitionOffsets);
      }
      final Map<Integer, Long> partitionOffsets = offsetVector.get(topic);
      if (partitionOffsets.containsKey(partition)) {
        final long oldOffset = partitionOffsets.get(partition);
        partitionOffsets.put(partition, Math.max(oldOffset, offset));
      } else {
        partitionOffsets.put(partition, offset);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public void merge(final OffsetVector other) {
    if (getClass() != other.getClass()) {
      throw new KsqlException("Offset vector types don't match");
    }

    final ConsistencyOffsetVector otherCV = (ConsistencyOffsetVector) other;
    try {
      rwLock.writeLock().lock();
      for (final Entry<String, Map<Integer, Long>> topicEntry : otherCV.offsetVector.entrySet()) {
        final String topic = topicEntry.getKey();
        if (offsetVector.containsKey(topic)) {
          final Map<Integer, Long> partitionOffsets = offsetVector.get(topic);
          final Map<Integer, Long> otherPartitionOffsets = otherCV.offsetVector.get(topic);
          for (final Entry<Integer, Long> p : otherPartitionOffsets.entrySet()) {
            if (partitionOffsets.containsKey(p.getKey())) {
              final long offset = partitionOffsets.get(p.getKey());
              partitionOffsets.put(p.getKey(), Math.max(p.getValue(), offset));
            } else {
              partitionOffsets.put(p.getKey(), p.getValue());
            }
          }
        } else {
          final Map<Integer, Long> partitionOffsets = new HashMap<>(topicEntry.getValue());
          offsetVector.put(topic, partitionOffsets);
        }
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public boolean dominates(final OffsetVector other) {
    if (getClass() != other.getClass()) {
      throw new KsqlException("Offset vector types don't match");
    }
    final ConsistencyOffsetVector otherCV = (ConsistencyOffsetVector) other;

    try {
      rwLock.readLock().lock();
      for (final Entry<String, Map<Integer, Long>> topicEntry : offsetVector.entrySet()) {
        final String topic = topicEntry.getKey();
        if (otherCV.offsetVector.containsKey(topic)) {
          final Map<Integer, Long> partitionOffsets = offsetVector.get(topic);
          final Map<Integer, Long> otherPartitionOffsets = otherCV.offsetVector.get(topic);
          for (final Entry<Integer, Long> p : partitionOffsets.entrySet()) {
            if (otherPartitionOffsets.containsKey(p.getKey())) {
              final long offset = partitionOffsets.get(p.getKey());
              final long otherOffset = otherPartitionOffsets.get(p.getKey());
              if (offset < otherOffset) {
                return false;
              }
            }
          }
        }
      }
      return true;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public boolean lessThanOrEqualTo(final OffsetVector other) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @JsonIgnore
  @Override
  public List<Long> getDenseRepresentation() {
    throw new UnsupportedOperationException("Unsupported");
  }

  public String serialize() {
    try {
      final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(this);
      return Base64.getEncoder().encodeToString(bytes);
    } catch (Exception e) {
      throw new KsqlException("Couldn't encode consistency token", e);
    }
  }

  public static ConsistencyOffsetVector deserialize(final String token) {
    try {
      final byte[] bytes = Base64.getDecoder().decode(token);
      return OBJECT_MAPPER.readValue(bytes, ConsistencyOffsetVector.class);
    } catch (Exception e) {
      throw new KsqlException("Couldn't decode consistency token", e);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConsistencyOffsetVector that = (ConsistencyOffsetVector) o;
    return this.dominates(that) && that.dominates(this);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, offsetVector, rwLock);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ConsistencyOffsetVector{");
    sb.append("version=").append(version);
    sb.append(", offsetVector=").append(offsetVector);
    sb.append(", rwLock=").append(rwLock);
    sb.append('}');
    return sb.toString();
  }

  public static boolean isConsistencyVectorEnabled(
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overrides
  ) {
    if (overrides.containsKey(KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED)) {
      return Boolean.TRUE.equals(overrides.get(
          KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED));
    } else {
      return ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED);
    }
  }

  public static boolean isConsistencyVectorEnabled(final Map<String, Object> requestProperties) {
    final Object consistencyEnabled
        = requestProperties.get(KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED);

    if (consistencyEnabled instanceof Boolean) {
      return (boolean) consistencyEnabled;
    }

    return KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED_DEFAULT;
  }
}
