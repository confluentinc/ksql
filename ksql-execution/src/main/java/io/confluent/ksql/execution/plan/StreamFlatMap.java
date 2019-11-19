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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamFlatMap<K> implements ExecutionStep<KStreamHolder<K>> {

  private final ExecutionStepProperties properties;
  private final ExecutionStep<KStreamHolder<K>> source;
  private final List<FunctionCall> tableFunctions;

  public StreamFlatMap(
      @JsonProperty(value = "properties", required = true) ExecutionStepProperties properties,
      @JsonProperty(value = "source", required = true) ExecutionStep<KStreamHolder<K>> source,
      @JsonProperty(value = "tableFunctions", required = true)
      List<FunctionCall> tableFunctions
  ) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.tableFunctions = Objects.requireNonNull(tableFunctions);
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  @Override
  public KStreamHolder<K> build(PlanBuilder builder) {
    return builder.visitFlatMap(this);
  }

  public List<FunctionCall> getTableFunctions() {
    return tableFunctions;
  }

  public ExecutionStep<KStreamHolder<K>> getSource() {
    return source;
  }

}
