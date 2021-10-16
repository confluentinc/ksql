package io.confluent.ksql.util;

import java.util.List;
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
}
