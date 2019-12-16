/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Objects;

@Immutable
public class TableTableJoin<K> implements ExecutionStep<KTableHolder<K>> {
  private final ExecutionStepPropertiesV1 properties;
  private final JoinType joinType;
  private final ExecutionStep<KTableHolder<K>> leftSource;
  private final ExecutionStep<KTableHolder<K>> rightSource;

  public TableTableJoin(
      @JsonProperty(value = "properties", required = true) ExecutionStepPropertiesV1 properties,
      @JsonProperty(value = "joinType", required = true) JoinType joinType,
      @JsonProperty(value = "leftSource", required = true)
      ExecutionStep<KTableHolder<K>> leftSource,
      @JsonProperty(value = "rightSource", required = true)
      ExecutionStep<KTableHolder<K>> rightSource) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.joinType = Objects.requireNonNull(joinType, "joinType");
    this.leftSource = Objects.requireNonNull(leftSource, "leftSource");
    this.rightSource = Objects.requireNonNull(rightSource, "rightSource");
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return ImmutableList.of(leftSource, rightSource);
  }

  public ExecutionStep<KTableHolder<K>> getLeftSource() {
    return leftSource;
  }

  public ExecutionStep<KTableHolder<K>> getRightSource() {
    return rightSource;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  @Override
  public KTableHolder<K> build(PlanBuilder builder) {
    return builder.visitTableTableJoin(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableTableJoin<?> that = (TableTableJoin<?>) o;
    return Objects.equals(properties, that.properties)
        && joinType == that.joinType
        && Objects.equals(leftSource, that.leftSource)
        && Objects.equals(rightSource, that.rightSource);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, joinType, leftSource, rightSource);
  }
}
