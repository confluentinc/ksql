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

package io.confluent.ksql.util;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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
public class ConsistencyOffsetVector implements OffsetVector, Serializable {

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

  public int getVersion() {
    return version;
  }

  @VisibleForTesting
  public ImmutableMap<String, Map<Integer, Long>> getOffsetVector() {
    return ImmutableMap.copyOf(offsetVector);
  }

  @VisibleForTesting
  public Map<Integer, Long> getTopicOffsets(final String topicID) {
    return ImmutableMap.copyOf(offsetVector.get(topicID));
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

  @Override
  public List<Long> getDenseRepresentation() {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public String serialize() {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      rwLock.readLock().lock();
      oos.writeObject(this);
      oos.flush();
      return Base64.getEncoder().encodeToString(bos.toByteArray());
    } catch (Exception e) {
      throw new KsqlException("Couldn't encode consistency token", e);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public void deserialize(final String serializedString) {
    final byte[] data = Base64.getDecoder().decode(serializedString);
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data))) {
      final ConsistencyOffsetVector ct = (ConsistencyOffsetVector) in.readObject();
      version = ct.version;
      offsetVector.putAll(ct.offsetVector);
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

  public static boolean isConsistencyVectorEnabled(final Map<String, Object> requestProperties) {
    final Object consistencyEnabled
        = requestProperties.get(KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED);

    if (consistencyEnabled instanceof Boolean) {
      return (boolean) consistencyEnabled;
    }

    return KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED_DEFAULT;
  }
}
