/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * OffsetVector for push query continuation tokens. This class is similar to
 * {@link ConsistencyOffsetVector} and it's possible that they will be merged in the future. They
 * both represent offsets associated with a source topic, though they are serialized and
 * deserialized a bit differently.
 */
public class PushOffsetVector implements OffsetVector {

  private final AtomicReference<List<Long>> offsets = new AtomicReference<>();

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  @JsonCreator
  public PushOffsetVector(final @JsonProperty(value = "o") List<Long> offsets) {
    this.offsets.set(ImmutableList.copyOf(offsets));
  }

  public PushOffsetVector() {
    this.offsets.set(Collections.emptyList());
  }

  @Override
  public void merge(final OffsetVector other) {
    final List<Long> offsetsOther = other.getDenseRepresentation();
    if (offsets.get().isEmpty()) {
      offsets.set(offsetsOther);
      return;
    } else if (offsetsOther.isEmpty()) {
      return;
    }
    Preconditions.checkState(offsetsOther.size() == offsets.get().size(),
        "Should be equal other:" + offsetsOther.size() + ",  offsets:" + offsets.get().size());
    final ImmutableList.Builder<Long> builder = ImmutableList.builder();
    int partition = 0;
    for (Long offset : offsets.get()) {
      builder.add(Math.max(offsetsOther.get(partition), offset));
      partition++;
    }
    this.offsets.set(builder.build());
  }

  public PushOffsetVector mergeCopy(final OffsetVector other) {
    final PushOffsetVector copy = copy();
    copy.merge(other);
    return copy;
  }

  @Override
  public boolean lessThanOrEqualTo(final OffsetVector other) {
    final List<Long> offsetsOther = other.getDenseRepresentation();
    // Special case that says that a vectors is "less than or equal" to an uninitialized vector
    if (offsetsOther.isEmpty()) {
      return true;
    }
    Preconditions.checkState(offsetsOther.size() == offsets.get().size());
    int partition = 0;
    for (Long offset : offsets.get()) {
      final long offsetOther = offsetsOther.get(partition);
      if (offset >= 0 && offsetOther >= 0) {
        if (offset > offsetOther) {
          return false;
        }
      }
      partition++;
    }
    return true;
  }

  @JsonIgnore
  @Override
  public List<Long> getDenseRepresentation() {
    return getOffsets();
  }

  @JsonIgnore
  public Map<Integer, Long> getSparseRepresentation() {
    int i = 0;
    final Map<Integer, Long> offsets = new HashMap<>();
    for (Long offset : getOffsets()) {
      offsets.put(i++, offset);
    }
    return ImmutableMap.copyOf(offsets);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  @JsonProperty("o")
  public List<Long> getOffsets() {
    return offsets.get();
  }

  @JsonIgnore
  @Override
  public boolean dominates(final OffsetVector other) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @JsonIgnore
  @Override
  public void update(final String topic, final int partition, final long offset) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @JsonIgnore
  @Override
  public String serialize() {
    throw new UnsupportedOperationException("Unsupported");
  }

  public PushOffsetVector copy() {
    return new PushOffsetVector(offsets.get());
  }

  @JsonIgnore
  @Override
  public String toString() {
    return "PushOffsetVector{"
        + "offsets=" + offsets
        + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(offsets.get());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PushOffsetVector that = (PushOffsetVector) o;
    return Objects.equals(offsets.get(), that.offsets.get());
  }

  public static boolean isContinuationTokenEnabled(final Map<String, Object> requestProperties) {
    final Object continuationTokenEnabled
            = requestProperties.get(KsqlConfig.KSQL_QUERY_PUSH_V2_ALOS_ENABLED);

    if (continuationTokenEnabled instanceof Boolean) {
      return (boolean) continuationTokenEnabled;
    }

    return KsqlConfig.KSQL_QUERY_PUSH_V2_ALOS_ENABLED_DEFAULT;
  }
}
