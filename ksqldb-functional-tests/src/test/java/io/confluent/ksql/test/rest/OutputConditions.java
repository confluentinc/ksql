package io.confluent.ksql.test.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class OutputConditions {

  private final boolean verifyOrder;

  public OutputConditions(
    @JsonProperty("verifyOrder") final boolean verifyOrder
  ) {
    this.verifyOrder = verifyOrder;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final OutputConditions that = (OutputConditions) o;
    return verifyOrder == that.verifyOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(verifyOrder);
  }

  public boolean getVerifyOrder() {
    return verifyOrder;
  }
}
