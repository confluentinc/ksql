package io.confluent.ksql.test.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class InputConditions {

  private final boolean waitForActivePushQuery;

  public InputConditions(
      @JsonProperty("waitForActivePushQuery") final boolean waitForActivePushQuery
  ) {
    this.waitForActivePushQuery = waitForActivePushQuery;
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
    return waitForActivePushQuery == that.waitForActivePushQuery;
  }

  @Override
  public int hashCode() {
    return Objects.hash(waitForActivePushQuery);
  }

  public boolean getWaitForActivePushQuery() {
    return waitForActivePushQuery;
  }
}
