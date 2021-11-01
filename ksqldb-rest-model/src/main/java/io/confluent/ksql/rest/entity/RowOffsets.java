package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

/**
 * Represents a fixed or range of offsets that the previous batch of data represents.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public class RowOffsets {

  private final Optional<List<Long>> startOffsets;
  private final List<Long> offsets;

  @JsonCreator
  public RowOffsets(
      final @JsonProperty(value = "startOffsets", required = true) Optional<List<Long>> startOffsets,
      final @JsonProperty(value = "offsets", required = true) List<Long> offsets) {
    this.startOffsets = startOffsets.map(so -> ImmutableList.copyOf(so));
    this.offsets = ImmutableList.copyOf(offsets);
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
  public String toString() {
    return "RowOffsets{"
        + "startOffsets='" + startOffsets + '\''
        + ", offsets='" + offsets + '\''
        + '}';
  }
}
