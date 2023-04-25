package io.confluent.ksql.test.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class InputConditions {

  private final boolean waitForQueryHeader;

  public InputConditions(
      @JsonProperty("waitForQueryHeader") final boolean waitForQueryHeader
  ) {
    this.waitForQueryHeader = waitForQueryHeader;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final InputConditions that = (InputConditions) o;
    return waitForQueryHeader == that.waitForQueryHeader;
  }

  @Override
  public int hashCode() {
    return Objects.hash(waitForQueryHeader);
  }

  public boolean getWaitForQueryHeader() {
    return waitForQueryHeader;
  }
}
