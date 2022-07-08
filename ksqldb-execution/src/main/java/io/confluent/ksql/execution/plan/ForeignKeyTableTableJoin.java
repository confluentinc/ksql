/*
 * Copyright 2021 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ForeignKeyTableTableJoin<KLeftT, KRightT>
    implements ExecutionStep<KTableHolder<KLeftT>> {

  private final ExecutionStepPropertiesV1 properties;
  private final JoinType joinType;
  private final Optional<ColumnName> leftJoinColumnName;
  private final Optional<Expression> leftJoinExpression;
  private final Formats formats;
  private final ExecutionStep<KTableHolder<KLeftT>> leftSource;
  private final ExecutionStep<KTableHolder<KRightT>> rightSource;

  @JsonCreator
  public ForeignKeyTableTableJoin(
      @JsonProperty(value = "properties", required = true)
      final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "joinType", required = true)
      final JoinType joinType,
      @JsonProperty(value = "leftJoinColumnName")
      final Optional<ColumnName> leftJoinColumnName,
      @JsonProperty(value = "leftJoinExpression")
      final Optional<Expression> leftJoinExpression,
      @JsonProperty(value = "formats", required = true)
      final Formats formats,
      @JsonProperty(value = "leftSource", required = true)
      final ExecutionStep<KTableHolder<KLeftT>> leftSource,
      @JsonProperty(value = "rightSource", required = true)
      final ExecutionStep<KTableHolder<KRightT>> rightSource
  ) {
    this.properties = requireNonNull(props, "props");
    this.joinType = requireNonNull(joinType, "joinType");
    if (joinType == JoinType.OUTER) {
      throw new IllegalArgumentException("OUTER join not supported.");
    }
    this.leftJoinColumnName = requireNonNull(leftJoinColumnName, "leftJoinColumnName");
    this.formats = requireNonNull(formats, "formats");
    this.leftSource = requireNonNull(leftSource, "leftSource");
    this.rightSource = requireNonNull(rightSource, "rightSource");
    this.leftJoinExpression = requireNonNull(leftJoinExpression, "leftJoinExpression");
    if (!leftJoinColumnName.isPresent() && !leftJoinExpression.isPresent()) {
      throw new IllegalArgumentException(
          "Either leftJoinColumnName or leftJoinExpression must be provided."
      );
    }
    if (leftJoinColumnName.isPresent() && leftJoinExpression.isPresent()) {
      throw new IllegalArgumentException(
          "Either leftJoinColumnName or leftJoinExpression must be empty."
      );
    }
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

  public ExecutionStep<KTableHolder<KLeftT>> getLeftSource() {
    return leftSource;
  }

  public ExecutionStep<KTableHolder<KRightT>> getRightSource() {
    return rightSource;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public Optional<ColumnName> getLeftJoinColumnName() {
    return leftJoinColumnName;
  }

  public Optional<Expression> getLeftJoinExpression() {
    return leftJoinExpression;
  }

  public Formats getFormats() {
    return formats;
  }

  @Override
  public KTableHolder<KLeftT> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitForeignKeyTableTableJoin(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitForeignKeyTableTableJoin(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ForeignKeyTableTableJoin<?, ?> that = (ForeignKeyTableTableJoin<?, ?>) o;
    return Objects.equals(properties, that.properties)
        && joinType == that.joinType
        && Objects.equals(leftJoinColumnName, that.leftJoinColumnName)
        && Objects.equals(formats, that.formats)
        && Objects.equals(leftSource, that.leftSource)
        && Objects.equals(rightSource, that.rightSource)
        && Objects.equals(leftJoinExpression, that.leftJoinExpression);

  }

  @Override
  public int hashCode() {
    return Objects.hash(
        properties,
        joinType,
        leftJoinColumnName,
        formats,
        leftSource,
        rightSource,
        leftJoinExpression
    );
  }
}
