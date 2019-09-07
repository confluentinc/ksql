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
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamToTable<S, T> implements ExecutionStep<T> {
  private static final String FORMATS = "formats";
  private static final String SOURCE = "source";

  @JsonProperty(SOURCE)
  private final ExecutionStep<S> source;
  @JsonProperty(FORMATS)
  private final Formats formats;
  @JsonProperty(PROPERTIES)
  private final ExecutionStepProperties properties;

  @JsonCreator
  public StreamToTable(
      @JsonProperty(SOURCE) final ExecutionStep<S> source,
      @JsonProperty(FORMATS) final Formats formats,
      @JsonProperty(PROPERTIES) final ExecutionStepProperties properties) {
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
    final StreamToTable<?, ?> that = (StreamToTable<?, ?>) o;
    return Objects.equals(source, that.source)
        && Objects.equals(formats, that.formats)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {

    return Objects.hash(source, formats, properties);
  }
}
