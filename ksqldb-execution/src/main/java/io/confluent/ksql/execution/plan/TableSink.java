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
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

@Immutable
public class TableSink<K> implements ExecutionStep<KTableHolder<K>> {
  private final ExecutionStepPropertiesV1 properties;
  private final ExecutionStep<KTableHolder<K>> source;
  private final Formats formats;
  private final String topicName;
  private final Optional<TimestampColumn> timestampColumn;

  private static final ImmutableList<Property> MUST_MATCH = ImmutableList.of(
      new Property("class", Object::getClass),
      new Property("properties", ExecutionStep::getProperties),
      new Property("formats", s -> ((TableSink<?>) s).formats),
      new Property("topicName", s -> ((TableSink<?>) s).topicName),
      new Property("timestampColumn", s -> ((TableSink<?>) s).timestampColumn)
  );

  public TableSink(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final ExecutionStep<KTableHolder<K>> source,
      @JsonProperty(value = "formats", required = true) final Formats formats,
      @JsonProperty(value = "topicName", required = true) final String topicName,
      @JsonProperty(value = "timestampColumn") final Optional<TimestampColumn> timestampColumn
  ) {
    this.properties = Objects.requireNonNull(props, "props");
    this.source = Objects.requireNonNull(source, "source");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.timestampColumn = Objects.requireNonNull(timestampColumn, "timestampColumn");
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
    return properties;
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public Formats getFormats() {
    return formats;
  }

  public ExecutionStep<KTableHolder<K>> getSource() {
    return source;
  }

  public Optional<TimestampColumn> getTimestampColumn() {
    return timestampColumn;
  }

  @Override
  public KTableHolder<K> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitTableSink(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitTableSink(this);
  }

  @Override
  public void validateUpgrade(@Nonnull final ExecutionStep<?> to) {
    mustMatch(to, MUST_MATCH);
    getSource().validateUpgrade(((TableSink<?>) to).source);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableSink<?> tableSink = (TableSink<?>) o;
    return Objects.equals(properties, tableSink.properties)
        && Objects.equals(source, tableSink.source)
        && Objects.equals(formats, tableSink.formats)
        && Objects.equals(topicName, tableSink.topicName)
        && Objects.equals(timestampColumn, tableSink.timestampColumn);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, formats, topicName, timestampColumn);
  }
}
