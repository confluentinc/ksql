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
  private final ExecutionStepProperties properties;
  private final JoinType joinType;
  private final ExecutionStep<KTableHolder<K>> left;
  private final ExecutionStep<KTableHolder<K>> right;

  public TableTableJoin(
      @JsonProperty(value = "properties", required = true) ExecutionStepProperties properties,
      @JsonProperty(value = "joinType", required = true) JoinType joinType,
      @JsonProperty(value = "left", required = true) ExecutionStep<KTableHolder<K>> left,
      @JsonProperty(value = "right", required = true) ExecutionStep<KTableHolder<K>> right) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.joinType = Objects.requireNonNull(joinType, "joinType");
    this.left = Objects.requireNonNull(left, "left");
    this.right = Objects.requireNonNull(right, "right");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return ImmutableList.of(left, right);
  }

  public ExecutionStep<KTableHolder<K>> getLeft() {
    return left;
  }

  public ExecutionStep<KTableHolder<K>> getRight() {
    return right;
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
        && Objects.equals(left, that.left)
        && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, joinType, left, right);
  }
}
