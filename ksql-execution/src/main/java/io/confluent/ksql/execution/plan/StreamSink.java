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
import com.google.errorprone.annotations.Immutable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamSink<K> implements ExecutionStep<KStreamHolder<K>> {
  private final ExecutionStepProperties properties;
  private final ExecutionStep<KStreamHolder<K>>  source;
  private final Formats formats;
  private final String topicName;

  public StreamSink(
      @JsonProperty(value = "properties", required = true) ExecutionStepProperties properties,
      @JsonProperty(value = "source", required = true) ExecutionStep<KStreamHolder<K>> source,
      @JsonProperty(value = "formats", required = true) Formats formats,
      @JsonProperty(value = "topicName", required = true) String topicName) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.source = Objects.requireNonNull(source, "source");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public Formats getFormats() {
    return formats;
  }

  public ExecutionStep<KStreamHolder<K>> getSource() {
    return source;
  }

  @Override
  public KStreamHolder<K> build(PlanBuilder builder) {
    return builder.visitStreamSink(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamSink<?> that = (StreamSink<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(formats, that.formats)
        && Objects.equals(topicName, that.topicName);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, formats, topicName);
  }
}
