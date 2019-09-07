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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class TableAggregate<T, G> implements ExecutionStep<T> {
  private static final String SOURCE = "source";
  private static final String FORMATS = "formats";
  private static final String NON_FUNC_COLUMN_COUNT = "nonFuncColumnCount";
  private static final String AGGREGATIONS = "aggregations";

  @JsonProperty(PROPERTIES)
  private final ExecutionStepProperties properties;
  @JsonProperty(SOURCE)
  private final ExecutionStep<G> source;
  @JsonProperty(FORMATS)
  private final Formats formats;
  @JsonProperty(NON_FUNC_COLUMN_COUNT)
  private final int nonFuncColumnCount;
  @JsonProperty(AGGREGATIONS)
  private final List<FunctionCall> aggregations;

  @JsonCreator
  public TableAggregate(
      @JsonProperty(PROPERTIES) final ExecutionStepProperties properties,
      @JsonProperty(SOURCE) final ExecutionStep<G> source,
      @JsonProperty(FORMATS) final Formats formats,
      @JsonProperty(NON_FUNC_COLUMN_COUNT) final int nonFuncColumnCount,
      @JsonProperty(AGGREGATIONS) final List<FunctionCall> aggregations) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.nonFuncColumnCount = nonFuncColumnCount;
    this.aggregations = Objects.requireNonNull(aggregations, "aggValToFunctionMap");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  @Override
  public T build(final KsqlQueryBuilder builder) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableAggregate<?, ?> that = (TableAggregate<?, ?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(formats, that.formats)
        && nonFuncColumnCount == that.nonFuncColumnCount
        && Objects.equals(aggregations, that.aggregations);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, formats, nonFuncColumnCount, aggregations);
  }
}
