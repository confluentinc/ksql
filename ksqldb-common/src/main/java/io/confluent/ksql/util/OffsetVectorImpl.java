/*
 * Copyright 2020 Confluent Inc.
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

import java.io.ByteArrayOutputStream;
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

public final class OffsetVectorImpl implements OffsetVector, Serializable {
  private int version;
  private Map<String, Map<Integer, Long>> offsetVector;
  private ReadWriteLock rwLock;

  public OffsetVectorImpl() {
    this.version = 0;
    this.offsetVector = new HashMap<>();
    this.rwLock = new ReentrantReadWriteLock();
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

    final OffsetVectorImpl otherImpl = (OffsetVectorImpl) other;
    try {
      rwLock.writeLock().lock();
      for (final Entry<String, Map<Integer, Long>> topicEntry : otherImpl.offsetVector.entrySet()) {
        final String topic = topicEntry.getKey();
        if (offsetVector.containsKey(topic)) {
          final Map<Integer, Long> partitionOffsets = offsetVector.get(topic);
          final Map<Integer, Long> otherPartitionOffsets = otherImpl.offsetVector.get(topic);
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
    final OffsetVectorImpl otherImpl = (OffsetVectorImpl) other;

    try {
      rwLock.readLock().lock();
      for (final Entry<String, Map<Integer, Long>> topicEntry : offsetVector.entrySet()) {
        final String topic = topicEntry.getKey();
        if (otherImpl.offsetVector.containsKey(topic)) {
          final Map<Integer, Long> partitionOffsets = offsetVector.get(topic);
          final Map<Integer, Long> otherPartitionOffsets = otherImpl.offsetVector.get(topic);
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final OffsetVectorImpl that = (OffsetVectorImpl) o;
    return this.dominates(that) && that.dominates(this);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, offsetVector, rwLock);
  }

  @Override
  public String toString() {
    return "OffsetVectorImpl{"
      + "version=" + version
      + ", offsetVector=" + offsetVector
      + ", rwLock=" + rwLock
      + '}';
  }
}


