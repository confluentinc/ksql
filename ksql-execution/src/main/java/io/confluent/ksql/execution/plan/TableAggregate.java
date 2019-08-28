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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Immutable
public class TableAggregate<T, G> implements ExecutionStep<T> {
  private final ExecutionStepProperties properties;
  private final ExecutionStep<G> source;
  private final Formats formats;
  private final Map<Integer, KsqlAggregateFunction> indexToFunctionMap;
  private final Map<Integer, Integer> indexToValColumnMap;

  public TableAggregate(
      final ExecutionStepProperties properties,
      final ExecutionStep<G> source,
      final Formats formats,
      final Map<Integer, KsqlAggregateFunction> indexToFunctionMap,
      final Map<Integer, Integer> indexToValColumnMap) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.indexToFunctionMap = Objects.requireNonNull(indexToFunctionMap, "indexToFunctionMap");
    this.indexToValColumnMap = Objects.requireNonNull(indexToValColumnMap, "indexToValColumnMap");
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
        && Objects.equals(indexToFunctionMap, that.indexToFunctionMap)
        && Objects.equals(indexToValColumnMap, that.indexToValColumnMap);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, formats, indexToFunctionMap, indexToValColumnMap);
  }
}
