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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.kafka.connect.data.Struct;

@Immutable
public class StreamGroupByKey implements ExecutionStep<KGroupedStreamHolder> {

  private static final ImmutableList<Property> MUST_MATCH = ImmutableList.of(
      new Property("class", Object::getClass),
      new Property("properties", ExecutionStep::getProperties),
      new Property("internal formats", s -> ((StreamGroupByKey) s).internalFormats)
  );

  private final ExecutionStepPropertiesV1 properties;
  private final ExecutionStep<KStreamHolder<Struct>> source;
  private final Formats internalFormats;

  public StreamGroupByKey(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final
      ExecutionStep<KStreamHolder<Struct>> source,
      @JsonProperty(value = "internalFormats", required = true) final Formats internalFormats) {
    this.properties = Objects.requireNonNull(props, "props");
    this.internalFormats = Objects.requireNonNull(internalFormats, "internalFormats");
    this.source = Objects.requireNonNull(source, "source");
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

  public ExecutionStep<KStreamHolder<Struct>> getSource() {
    return source;
  }

  public Formats getInternalFormats() {
    return internalFormats;
  }

  @Override
  public KGroupedStreamHolder build(final PlanBuilder builder) {
    return builder.visitStreamGroupByKey(this);
  }

  @Override
  public void validateUpgrade(@Nonnull final ExecutionStep<?> to) {
    mustMatch(to, MUST_MATCH);
    getSource().validateUpgrade(((StreamGroupByKey) to).source);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamGroupByKey that = (StreamGroupByKey) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(internalFormats, that.internalFormats);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, internalFormats);
  }
}
