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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Any metadata sent alongside or independent of a row.
 */
public class RowMetadata {

  // If set, the start of the offset range associated with this batch of data.
  private final Optional<List<Long>> startOffsets;
  // The offsets associated with this row/batch of rows, or the end of the offset range if start
  // offsets is set.
  private final List<Long> offsets;

  public RowMetadata(final Optional<List<Long>> startOffsets, final List<Long> offsets) {
    this.startOffsets = startOffsets;
    this.offsets = offsets;
  }

  /**
   * If this represents a range of offsets, the starting offsets of the range.
   */
  public Optional<List<Long>> getStartOffsets() {
    return startOffsets;
  }

  /**
   * The offsets associated with this set of rows, or the end of a range, if start is also set.
   */
  public List<Long> getOffsets() {
    return offsets;
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
    return Objects.equals(startOffsets, rowMetadata.startOffsets)
        && Objects.equals(offsets, rowMetadata.offsets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startOffsets, offsets);
  }
}
