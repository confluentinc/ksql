package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerOffset {

  private final int partition;
  private final long logStartOffset;
  private final long logEndOffset;
  private final long consumerOffset;

  @JsonCreator
  public ConsumerOffset(
      @JsonProperty("partition") int partition,
      @JsonProperty("logStartOffset") long logStartOffset,
      @JsonProperty("logEndOffset") long logEndOffset,
      @JsonProperty("consumerOffset") long consumerOffset
  ) {
    this.partition = partition;
    this.logStartOffset = logStartOffset;
    this.logEndOffset = logEndOffset;
    this.consumerOffset = consumerOffset;
  }

  public int getPartition() {
    return partition;
  }

  public long getConsumerOffset() {
    return consumerOffset;
  }

  public long getLogStartOffset() {
    return logStartOffset;
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
    ConsumerOffset that = (ConsumerOffset) o;
    return partition == that.partition &&
        logStartOffset == that.logStartOffset &&
        logEndOffset == that.logEndOffset &&
        consumerOffset == that.consumerOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, logStartOffset, logEndOffset, consumerOffset);
  }
}
