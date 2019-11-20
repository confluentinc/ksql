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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamToTable<K> implements ExecutionStep<KTableHolder<K>> {
  private final ExecutionStep<KStreamHolder<K>> source;
  private final Formats formats;
  private final ExecutionStepProperties properties;

  public StreamToTable(
      @JsonProperty(value = "source", required = true) ExecutionStep<KStreamHolder<K>> source,
      @JsonProperty(value = "formats", required = true) Formats formats,
      @JsonProperty(value = "properties", required = true)
      ExecutionStepProperties properties) {
    this.source = Objects.requireNonNull(source, "source");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.properties = Objects.requireNonNull(properties, "properties");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return ImmutableList.of(source);
  }

  public ExecutionStep<KStreamHolder<K>> getSource() {
    return source;
  }

  public Formats getFormats() {
    return formats;
  }

  @Override
  public KTableHolder<K> build(PlanBuilder builder) {
    return builder.visitStreamToTable(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamToTable<?> that = (StreamToTable<?>) o;
    return Objects.equals(source, that.source)
        && Objects.equals(formats, that.formats)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {

    return Objects.hash(source, formats, properties);
  }
}
