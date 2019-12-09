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
public class StreamTableJoin<K> implements ExecutionStep<KStreamHolder<K>> {

  private final ExecutionStepPropertiesV1 properties;
  private final JoinType joinType;
  private final Formats internalFormats;
  private final ExecutionStep<KStreamHolder<K>> left;
  private final ExecutionStep<KTableHolder<K>> right;

  public StreamTableJoin(
      @JsonProperty(value = "properties", required = true) ExecutionStepPropertiesV1 properties,
      @JsonProperty(value = "joinType", required = true) JoinType joinType,
      @JsonProperty(value = "internalFormats", required = true) Formats internalFormats,
      @JsonProperty(value = "left", required = true) ExecutionStep<KStreamHolder<K>> left,
      @JsonProperty(value = "right", required = true) ExecutionStep<KTableHolder<K>> right) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.internalFormats = Objects.requireNonNull(internalFormats, "internalFormats");
    this.joinType = Objects.requireNonNull(joinType, "joinType");
    this.left = Objects.requireNonNull(left, "left");
    this.right = Objects.requireNonNull(right, "right");
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return ImmutableList.of(left, right);
  }

  public Formats getInternalFormats() {
    return internalFormats;
  }

  public ExecutionStep<KStreamHolder<K>> getLeft() {
    return left;
  }

  public ExecutionStep<KTableHolder<K>> getRight() {
    return right;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  @Override
  public KStreamHolder<K> build(PlanBuilder builder) {
    return builder.visitStreamTableJoin(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamTableJoin<?> that = (StreamTableJoin<?>) o;
    return Objects.equals(properties, that.properties)
        && joinType == that.joinType
        && Objects.equals(internalFormats, that.internalFormats)
        && Objects.equals(left, that.left)
        && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, joinType, internalFormats, left, right);
  }
}
