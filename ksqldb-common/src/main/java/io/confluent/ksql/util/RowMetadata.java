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

  public RowMetadata(final Optional<PushOffsetRange> pushOffsetsRange) {
    this.pushOffsetsRange = pushOffsetsRange;
  }

  public Optional<PushOffsetRange> getPushOffsetsRange() {
    return pushOffsetsRange;
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
    return Objects.equals(pushOffsetsRange, rowMetadata.pushOffsetsRange);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pushOffsetsRange);
  }
}
