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
import io.confluent.ksql.execution.function.udtf.TableFunctionApplier;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamFlatMap<K> implements ExecutionStep<KStreamHolder<K>> {

  private final ExecutionStepProperties properties;
  private final ExecutionStep<KStreamHolder<K>> source;
  private final TableFunctionApplier functionHolder;

  public StreamFlatMap(
      final ExecutionStepProperties properties,
      final ExecutionStep<KStreamHolder<K>> source,
      final TableFunctionApplier functionHolder
  ) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.functionHolder = functionHolder;
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
  public KStreamHolder<K> build(final PlanBuilder builder) {
    return builder.visitFlatMap(this);
  }

  public TableFunctionApplier getFunctionHolder() {
    return functionHolder;
  }

  public ExecutionStep<KStreamHolder<K>> getSource() {
    return source;
  }

}
