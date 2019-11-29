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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

@Immutable
public abstract class AbstractStreamSource<K> implements ExecutionStep<K> {
  private final ExecutionStepProperties properties;
  private final Optional<AutoOffsetReset> offsetReset;
  private final SourceName sourceName;
  private final SourceName alias;

  public static LogicalSchemaWithMetaAndKeyFields getSchemaWithMetaAndKeyFields(
      SourceName alias,
      LogicalSchema schema) {
    return LogicalSchemaWithMetaAndKeyFields.fromOriginal(alias, schema);
  }

  @VisibleForTesting
  public AbstractStreamSource(
      ExecutionStepProperties properties,
      Optional<AutoOffsetReset> offsetReset,
      SourceName sourceName,
      SourceName alias) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.offsetReset = Objects.requireNonNull(offsetReset, "offsetReset");
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
    this.alias = Objects.requireNonNull(alias, "alias");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.emptyList();
  }

  public Optional<AutoOffsetReset> getOffsetReset() {
    return offsetReset;
  }

  public SourceName getAlias() {
    return alias;
  }

  public SourceName getSourceName() {
    return sourceName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractStreamSource that = (AbstractStreamSource) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(offsetReset, that.offsetReset)
        && Objects.equals(sourceName, that.sourceName)
        && Objects.equals(alias, that.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        properties,
        offsetReset,
        sourceName,
        alias
    );
  }
}
