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
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamFlatMap<K> implements ExecutionStep<KStreamHolder<K>> {

  private final ExecutionStepProperties properties;
  private final ExecutionStep<KStreamHolder<K>> source;

  final List<FunctionCall> functionCalls;
  final FunctionRegistry functionRegistry;
  final LogicalSchema inputSchema;
  final LogicalSchema outputSchema;

  public StreamFlatMap(
      final ExecutionStepProperties properties,
      final ExecutionStep<KStreamHolder<K>> source,
      final List<FunctionCall> functionCalls,
      final FunctionRegistry functionRegistry,
      final LogicalSchema inputSchema,
      final LogicalSchema outputSchema
  ) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.functionCalls = functionCalls;
    this.functionRegistry = functionRegistry;
    this.inputSchema = inputSchema;
    this.outputSchema = outputSchema;
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public ExecutionStep<KStreamHolder<K>> getSource() {
    return source;
  }

  @Override
  public KStreamHolder<K> build(final PlanBuilder builder) {
    return builder.visitFlatMap(this);
  }

  public List<FunctionCall> getFunctionCalls() {
    return functionCalls;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  public LogicalSchema getInputSchema() {
    return inputSchema;
  }

  public LogicalSchema getOutputSchema() {
    return outputSchema;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamFlatMap<?> that = (StreamFlatMap<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, source);
  }
}
