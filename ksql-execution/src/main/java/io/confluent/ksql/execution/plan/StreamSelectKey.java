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
import io.confluent.ksql.name.ColumnName;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;

@Immutable
public class StreamSelectKey<K> implements ExecutionStep<KStreamHolder<Struct>> {
  private final ExecutionStepProperties properties;
  private final ColumnName fieldName;
  private final ExecutionStep<KStreamHolder<K>> source;
  private final boolean updateRowKey;

  public StreamSelectKey(
      final ExecutionStepProperties properties,
      final ExecutionStep<KStreamHolder<K>> source,
      final ColumnName fieldName,
      final boolean updateRowKey) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    this.updateRowKey = updateRowKey;
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public boolean isUpdateRowKey() {
    return updateRowKey;
  }

  public ColumnName getFieldName() {
    return fieldName;
  }

  public ExecutionStep<KStreamHolder<K>> getSource() {
    return source;
  }

  @Override
  public KStreamHolder<Struct> build(final PlanBuilder builder) {
    return builder.visitStreamSelectKey(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamSelectKey<?> that = (StreamSelectKey<?>) o;
    return updateRowKey == that.updateRowKey
        && Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, updateRowKey);
  }
}
