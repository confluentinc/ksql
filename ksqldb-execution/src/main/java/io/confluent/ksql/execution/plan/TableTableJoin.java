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

import static io.confluent.ksql.execution.plan.StreamStreamJoin.LEGACY_KEY_COL;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class TableTableJoin<K> implements ExecutionStep<KTableHolder<K>> {

  private final ExecutionStepPropertiesV1 properties;
  private final JoinType joinType;
  private final ColumnName keyColName;
  private final ExecutionStep<KTableHolder<K>> leftSource;
  private final ExecutionStep<KTableHolder<K>> rightSource;

  /**
   * This constructor is required while {@code keyColName} is not mandatory.
   *
   * <p>{@code keyColName} was introduced in 0.10.0 and can be mandatory once 0.9.0 query plans
   * are no longer supported.
   *
   * @see <a href="https://github.com/confluentinc/ksql/issues/5421">Tracking issue</a>
   * @deprecated use the public constructor.
   */
  @SuppressWarnings("unused") // Invoked via reflection by Jackson
  @JsonCreator
  @Deprecated
  private TableTableJoin(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "joinType", required = true) final JoinType joinType,
      @JsonProperty(value = "keyName", defaultValue = LEGACY_KEY_COL)
      final Optional<ColumnName> keyColName,
      @JsonProperty(value = "leftSource", required = true)
      final ExecutionStep<KTableHolder<K>> leftSource,
      @JsonProperty(value = "rightSource", required = true)
      final ExecutionStep<KTableHolder<K>> rightSource
  ) {
    this(
        props,
        joinType,
        keyColName.orElse(ColumnName.of(LEGACY_KEY_COL)),
        leftSource,
        rightSource
    );
  }

  public TableTableJoin(
      final ExecutionStepPropertiesV1 props,
      final JoinType joinType,
      final ColumnName keyColName,
      final ExecutionStep<KTableHolder<K>> leftSource,
      final ExecutionStep<KTableHolder<K>> rightSource
  ) {
    this.properties = requireNonNull(props, "props");
    this.joinType = requireNonNull(joinType, "joinType");
    this.keyColName = requireNonNull(keyColName, "keyColName");
    this.leftSource = requireNonNull(leftSource, "leftSource");
    this.rightSource = requireNonNull(rightSource, "rightSource");
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

  public ColumnName getKeyColName() {
    return keyColName;
  }

  @Override
  public KTableHolder<K> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitTableTableJoin(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitTableTableJoin(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableTableJoin<?> that = (TableTableJoin<?>) o;
    return Objects.equals(properties, that.properties)
        && joinType == that.joinType
        && Objects.equals(keyColName, that.keyColName)
        && Objects.equals(leftSource, that.leftSource)
        && Objects.equals(rightSource, that.rightSource);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, joinType, keyColName, leftSource, rightSource);
  }
}
