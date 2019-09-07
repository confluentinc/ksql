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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamSelectKey<S> implements ExecutionStep<S> {
  private static final String PROPERTIES = "properties";
  private static final String SOURCE = "source";
  private static final String FIELD_NAME = "fieldName";
  private static final String UPDATE_ROW_KEY = "updateRowKey";

  @JsonProperty(PROPERTIES)
  private final ExecutionStepProperties properties;
  @JsonProperty(SOURCE)
  private final ExecutionStep<S> source;
  @JsonProperty(FIELD_NAME)
  private final String fieldName;
  @JsonProperty(UPDATE_ROW_KEY)
  private final boolean updateRowKey;

  @JsonCreator
  public StreamSelectKey(
      @JsonProperty(PROPERTIES) final ExecutionStepProperties properties,
      @JsonProperty(SOURCE) final ExecutionStep<S> source,
      @JsonProperty(FIELD_NAME) final String fieldName,
      @JsonProperty(UPDATE_ROW_KEY) final boolean updateRowKey) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = source;  // Objects.requireNonNull(source, "source");
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

  public ExecutionStep<S> getSource() {
    return source;
  }

  @Override
  public S build(final KsqlQueryBuilder streamsBuilder) {
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
