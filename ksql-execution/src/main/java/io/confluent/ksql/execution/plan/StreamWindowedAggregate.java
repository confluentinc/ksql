/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;

public class StreamWindowedAggregate
    implements ExecutionStep<KTableHolder<Windowed<Struct>>> {
  private final ExecutionStepProperties properties;
  private final ExecutionStep<KGroupedStreamHolder> source;
  private final Formats formats;
  private final int nonFuncColumnCount;
  private final List<FunctionCall> aggregations;
  private final KsqlWindowExpression windowExpression;

  public StreamWindowedAggregate(
      final ExecutionStepProperties properties,
      final ExecutionStep<KGroupedStreamHolder> source,
      final Formats formats,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations,
      final KsqlWindowExpression windowExpression) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.nonFuncColumnCount = nonFuncColumnCount;
    this.aggregations = Objects.requireNonNull(aggregations, "aggregations");
    this.windowExpression = Objects.requireNonNull(windowExpression, "windowExpression");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public int getNonFuncColumnCount() {
    return nonFuncColumnCount;
  }

  public List<FunctionCall> getAggregations() {
    return aggregations;
  }

  public Formats getFormats() {
    return formats;
  }

  public KsqlWindowExpression getWindowExpression() {
    return windowExpression;
  }

  public ExecutionStep<KGroupedStreamHolder> getSource() {
    return source;
  }

  @Override
  public KTableHolder<Windowed<Struct>> build(final PlanBuilder builder) {
    return builder.visitStreamWindowedAggregate(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamWindowedAggregate that = (StreamWindowedAggregate) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(formats, that.formats)
        && Objects.equals(aggregations, that.aggregations)
        && nonFuncColumnCount == that.nonFuncColumnCount
        && Objects.equals(windowExpression, that.windowExpression);
  }

  @Override
  public int hashCode() {

    return Objects.hash(
        properties,
        source,
        formats,
        aggregations,
        nonFuncColumnCount,
        windowExpression
    );
  }
}
