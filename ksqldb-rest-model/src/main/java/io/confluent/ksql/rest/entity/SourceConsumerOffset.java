package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SourceConsumerOffset {

  private final int partition;
  private final long logEndOffset;
  private final long consumerOffset;

  @JsonCreator
  public SourceConsumerOffset(
      @JsonProperty("partition") int partition,
      @JsonProperty("logEndOffset") long logEndOffset,
      @JsonProperty("consumerOffset") long consumerOffset
  ) {
    this.partition = partition;
    this.logEndOffset = logEndOffset;
    this.consumerOffset = consumerOffset;
  }

  public int getPartition() {
    return partition;
  }

  public long getConsumerOffset() {
    return consumerOffset;
  }

  public long getLogEndOffset() {
    return logEndOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceConsumerOffset that = (SourceConsumerOffset) o;
    return partition == that.partition &&
        logEndOffset == that.logEndOffset &&
        consumerOffset == that.consumerOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, logEndOffset, consumerOffset);
  }
}
