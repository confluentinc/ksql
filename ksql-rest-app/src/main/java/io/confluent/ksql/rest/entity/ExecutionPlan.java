package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName("executionPlan")
public class ExecutionPlan extends KsqlEntity {

  private final String executionPlan;

  @JsonCreator
  public ExecutionPlan(@JsonProperty("executionPlanText") String executionPlan) {
    super(executionPlan);
    this.executionPlan = executionPlan;
  }

  public String getExecutionPlan() {
    return executionPlan;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExecutionPlan)) {
      return false;
    }
    ExecutionPlan executionPlan = (ExecutionPlan) o;
    return Objects.equals(getExecutionPlan(), executionPlan.getExecutionPlan());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getExecutionPlan());
  }
}
