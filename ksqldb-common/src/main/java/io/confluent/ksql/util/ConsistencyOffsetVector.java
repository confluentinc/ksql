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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a consistency token that is communicated between the client and server.
 * This token is used to ensure monotonic reads for table pull queries as in: once a client has
 * received a row with an offset x, subsequent reads will only return rows of offsets > x.
 */
//@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsistencyOffsetVector {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.registerModule(new Jdk8Module());
  }

  // This field should be the first
  private int version;
  // Topic -> Partition -> Offset
  private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetVector;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  @JsonCreator
  public ConsistencyOffsetVector(
      final @JsonProperty(value = "version", required = true) int version,
      final @JsonProperty(value = "offsetVector", required = true)
          ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetVector
  ) {
    this.version = version;
    this.offsetVector = offsetVector;
  }

  public static ConsistencyOffsetVector emptyVector() {
    return new ConsistencyOffsetVector(0, new ConcurrentHashMap<>());
  }

  public ConsistencyOffsetVector withComponent(
      final String topic, final int partition, final long offset) {
    offsetVector
        .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
        .compute(
            partition,
            (integer, prior) -> prior == null || offset > prior ? Long.valueOf(offset) : prior
        );
    return this;
  }

  public Map<Integer, Long> getTopicOffsets(final String topic) {
    final ConcurrentHashMap<Integer, Long> partitionMap = offsetVector.get(topic);
    return partitionMap == null ? Collections.emptyMap() :
        Collections.unmodifiableMap(partitionMap);
  }

  public void update(final String topic, final int partition, final long offset) {
    offsetVector
        .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
        .compute(partition, (key, prior) -> prior == null || offset > prior ? Long.valueOf(offset) :
            prior);
  }

  public void merge(final ConsistencyOffsetVector other) {
    if (other == null) {
      return;
    } else if (getClass() != other.getClass()) {
      throw new KsqlException("Offset vector types don't match");
    } else {
      for (final Entry<String, ConcurrentHashMap<Integer, Long>> entry :
          other.offsetVector.entrySet()) {
        final String topic = entry.getKey();
        final Map<Integer, Long> partitionMap =
            offsetVector.computeIfAbsent(topic, k -> new ConcurrentHashMap<>());
        for (final Entry<Integer, Long> partitionOffset : entry.getValue().entrySet()) {
          final Integer partition = partitionOffset.getKey();
          final Long offset = partitionOffset.getValue();
          if (!partitionMap.containsKey(partition)
              || partitionMap.get(partition) < offset) {
            partitionMap.put(partition, offset);
          }
        }
      }
    }
  }

  public ConsistencyOffsetVector copy() {
    return new ConsistencyOffsetVector(version, deepCopy(offsetVector));
  }

  // Required for serialize/deserialize
  public int getVersion() {
    return version;
  }

  // Required for serialize/deserialize
  public void setVersion(final int newVersion) {
    version = newVersion;
  }

  // Required for serialize/deserialize
  public ImmutableMap<String, Map<Integer, Long>> getOffsetVector() {
    return ImmutableMap.copyOf(offsetVector);
  }

  @JsonAnySetter
  // Required for serialize/deserialize
  public void setOffsetVector(
      final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> topicOffsets) {
    offsetVector.putAll(topicOffsets);
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
    final ConsistencyOffsetVector vector1 = (ConsistencyOffsetVector) o;
    return Objects.equals(offsetVector, vector1.offsetVector) && version == vector1.version;
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException("Mutable object not suitable for hash key");
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ConsistencyOffsetVector{");
    sb.append("version=").append(version);
    sb.append(", offsetVector=").append(offsetVector);
    sb.append('}');
    return sb.toString();
  }

  private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> deepCopy(
      final Map<String, ? extends Map<Integer, Long>> map) {
    if (map == null) {
      return new ConcurrentHashMap<>();
    } else {
      final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> copy =
          new ConcurrentHashMap<>(map.size());
      for (final Entry<String, ? extends Map<Integer, Long>> entry : map.entrySet()) {
        copy.put(entry.getKey(), new ConcurrentHashMap<>(entry.getValue()));
      }
      return copy;
    }
  }
}
