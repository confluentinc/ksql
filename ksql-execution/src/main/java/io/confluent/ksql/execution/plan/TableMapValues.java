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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class TableMapValues<K> implements ExecutionStep<KTableHolder<K>> {

  private final ExecutionStepProperties properties;
  private final ExecutionStep<KTableHolder<K>> source;
  private final List<SelectExpression> selectExpressions;
  private final String selectNodeName;

  public TableMapValues(
      final ExecutionStepProperties properties,
      final ExecutionStep<KTableHolder<K>> source,
      final List<SelectExpression> selectExpressions,
      final String selectNodeName
  ) {
    this.properties = requireNonNull(properties, "properties");
    this.source = requireNonNull(source, "source");
    this.selectExpressions = ImmutableList.copyOf(selectExpressions);
    this.selectNodeName = requireNonNull(selectNodeName, "selectNodeName");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  public ExecutionStep<KTableHolder<K>> getSource() {
    return source;
  }

  public String getSelectNodeName() {
    return selectNodeName;
  }

  @Override
  public KTableHolder<K> build(final PlanBuilder builder) {
    return builder.visitTableMapValues(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableMapValues<?> that = (TableMapValues<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(selectExpressions, that.selectExpressions)
        && Objects.equals(selectNodeName, that.selectNodeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, source, selectExpressions, selectNodeName);
  }
}
