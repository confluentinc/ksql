/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.kql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class PlanNodeId {

  private final String id;

  @JsonCreator
  public PlanNodeId(final String id) {
    requireNonNull(id, "id is null");
    this.id = id;
  }

  @Override
  @JsonValue
  public String toString() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PlanNodeId that = (PlanNodeId) o;

    if (!id.equals(that.id)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
