/*
 * Copyright 2022 Confluent Inc.
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.ColumnName;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class StreamSelect<K> implements ExecutionStep<KStreamHolder<K>> {

  private final ExecutionStepPropertiesV1 properties;
  private final ExecutionStep<KStreamHolder<K>> source;
  private final ImmutableList<ColumnName> keyColumnNames;
  private final Optional<ImmutableList<ColumnName>> selectedKeys;
  private final ImmutableList<SelectExpression> selectExpressions;

  public StreamSelect(
      final ExecutionStepPropertiesV1 props,
      final ExecutionStep<KStreamHolder<K>> source,
      final List<ColumnName> keyColumnNames,
      final Optional<List<ColumnName>> selectedKeys,
      final List<SelectExpression> selectExpressions
  ) {
    this.properties = requireNonNull(props, "props");
    this.source = requireNonNull(source, "source");
    this.keyColumnNames = ImmutableList.copyOf(keyColumnNames);
    this.selectedKeys = selectedKeys.map(ImmutableList::copyOf);
    this.selectExpressions = ImmutableList.copyOf(selectExpressions);

    if (selectExpressions.isEmpty()) {
      throw new IllegalArgumentException("Need at least one select expression");
    }
  }

  /**
   * This constructor is required while {@code keyColumnNames} are not mandatory.
   *
   * <p>{@code keyColumnNames} was introduced in 0.10.0 and can be mandatory once 0.9.0 query plans
   * are no longer supported.
   *
   * @see <a href="https://github.com/confluentinc/ksql/issues/5420">Tracking issue</a>
   * @deprecated use the public constructor.
   */
  @SuppressWarnings("unused") // Invoked via reflection by Jackson
  @JsonCreator
  @Deprecated
  private StreamSelect(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final ExecutionStep<KStreamHolder<K>> source,
      @JsonProperty(value = "keyColumnNames") final Optional<List<ColumnName>> keyColumnNames,
      @JsonProperty(value = "selectedKeys") final Optional<List<ColumnName>> selectedKeys,
      @JsonProperty(value = "selectExpressions", required = true) final
      List<SelectExpression> selectExpressions
  ) {
    this(
        props,
        source,
        keyColumnNames.orElseGet(ImmutableList::of),
        selectedKeys,
        selectExpressions
    );
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "keyColumnNames is ImmutableList")
  public List<ColumnName> getKeyColumnNames() {
    return keyColumnNames;
  }

  public Optional<ImmutableList<ColumnName>> getSelectedKeys() {
    return selectedKeys;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "selectExpressions is ImmutableList")
  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  public ExecutionStep<KStreamHolder<K>> getSource() {
    return source;
  }

  @Override
  public KStreamHolder<K> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitStreamSelect(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitStreamSelect(this);
  }

  @Override
  public StepType type() {
    // the list of column names is verified in the schema compatibility
    // check when inserting into the metastore (See DataSource#canUpgradeTo)
    return StepType.PASSIVE;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamSelect<?> that = (StreamSelect<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(keyColumnNames, that.keyColumnNames)
        && Objects.equals(selectedKeys, that.selectedKeys)
        && Objects.equals(selectExpressions, that.selectExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, source, keyColumnNames, selectedKeys, selectExpressions);
  }
}
