/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName("executionPlan")
@JsonSubTypes({})
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
