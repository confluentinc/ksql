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

import java.util.Objects;
import java.util.Optional;

/**
 * Any metadata sent alongside or independent of a row.
 */
public class RowMetadata {

  private final Optional<PushOffsetRange> pushOffsetsRange;
  private final Optional<ConsistencyOffsetVector> consistencyOffsetVector;

  public RowMetadata(
      final Optional<PushOffsetRange> pushOffsetsRange,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    this.pushOffsetsRange = pushOffsetsRange;
    this.consistencyOffsetVector = consistencyOffsetVector;
  }

  public Optional<PushOffsetRange> getPushOffsetsRange() {
    return pushOffsetsRange;
  }

  public Optional<ConsistencyOffsetVector> getConsistencyOffsetVector() {
    return consistencyOffsetVector;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RowMetadata rowMetadata = (RowMetadata) o;
    return Objects.equals(pushOffsetsRange, rowMetadata.pushOffsetsRange)
        && Objects.equals(consistencyOffsetVector, rowMetadata.consistencyOffsetVector);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pushOffsetsRange, consistencyOffsetVector);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("RowMetadata{");
    sb.append("pushOffsetsRange=").append(pushOffsetsRange);
    sb.append(", consistencyOffsetVector=").append(consistencyOffsetVector);
    sb.append('}');
    return sb.toString();
  }

  public static RowMetadata of(
          final ConsistencyOffsetVector consistencyOffsetVector
  ) {
    return new RowMetadata(Optional.empty(), Optional.of(consistencyOffsetVector));
  }
}
