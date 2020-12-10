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
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

@Immutable
public class StreamSink<K> implements ExecutionStep<KStreamHolder<K>> {

  private static final ImmutableList<Property> MUST_MATCH = ImmutableList.of(
      new Property("class", Object::getClass),
      new Property("properties", ExecutionStep::getProperties),
      new Property("formats", s -> ((StreamSink<?>) s).formats),
      new Property("topicName", s -> ((StreamSink<?>) s).topicName),
      new Property("timestampColumn", s -> ((StreamSink<?>) s).timestampColumn)
  );

  private final ExecutionStepPropertiesV1 properties;
  private final ExecutionStep<KStreamHolder<K>>  source;
  private final Formats formats;
  private final String topicName;
  private final Optional<TimestampColumn> timestampColumn;

  public StreamSink(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final ExecutionStep<KStreamHolder<K>> source,
      @JsonProperty(value = "formats", required = true) final Formats formats,
      @JsonProperty(value = "topicName", required = true) final String topicName,
      @JsonProperty(value = "timestampColumn") final Optional<TimestampColumn> timestampColumn
  ) {
    this.properties = Objects.requireNonNull(props, "props");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.source = Objects.requireNonNull(source, "source");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.timestampColumn = Objects.requireNonNull(timestampColumn, "timestampColumn");
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
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

  public Optional<TimestampColumn> getTimestampColumn() {
    return timestampColumn;
  }

  @Override
  public KStreamHolder<K> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitStreamSink(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitStreamSink(this);
  }

  @Override
  public void validateUpgrade(@Nonnull final ExecutionStep<?> to) {
    mustMatch(to, MUST_MATCH);
    getSource().validateUpgrade(((StreamSink<?>) to).source);
  }

  @Override
  public StepType type() {
    return StepType.ENFORCING;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamSink<?> that = (StreamSink<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(formats, that.formats)
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(timestampColumn, that.timestampColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, source, formats, topicName, timestampColumn);
  }
}
