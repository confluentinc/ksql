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
import io.confluent.ksql.serde.RefinementInfo;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class TableSuppress<K> implements ExecutionStep<KTableHolder<K>> {

  private final ExecutionStepPropertiesV1 properties;
  private final ExecutionStep<KTableHolder<K>> source;
  private final RefinementInfo refinementInfo;
  private final Formats internalFormats;

  public TableSuppress(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final ExecutionStep<KTableHolder<K>> source,
      @JsonProperty(value = "refinementInfo", required = true) final RefinementInfo refinementInfo,
      @JsonProperty(value = "internalFormats", required = true) final Formats internalFormats
  ) {
    this.properties = Objects.requireNonNull(props, "props");
    this.source = Objects.requireNonNull(source, "source");
    this.refinementInfo = Objects.requireNonNull(refinementInfo, "refinementInfo");
    this.internalFormats = Objects.requireNonNull(internalFormats, "internalFormats");
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

  public RefinementInfo getRefinementInfo() {
    return refinementInfo;
  }

  public Formats getInternalFormats() {
    return internalFormats;
  }

  public ExecutionStep<KTableHolder<K>> getSource() {
    return source;
  }

  @Override
  public KTableHolder<K> build(final PlanBuilder builder) {
    return builder.visitTableSuppress(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableSuppress<?> that = (TableSuppress<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(refinementInfo, that.refinementInfo)
        && Objects.equals(internalFormats, that.internalFormats);
  }

  @Override
  public int hashCode() {

    return Objects.hash(
        properties,
        source,
        refinementInfo,
        internalFormats
    );
  }
}
