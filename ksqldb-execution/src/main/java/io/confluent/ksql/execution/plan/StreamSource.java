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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

@Immutable
public final class StreamSource extends SourceStep<KStreamHolder<GenericKey>> {

  private static final ImmutableList<Property> MUST_MATCH = ImmutableList.of(
      new Property("class", Object::getClass),
      new Property("properties", ExecutionStep::getProperties),
      new Property("topicName", s -> ((StreamSource) s).topicName),
      new Property("formats", s -> ((StreamSource) s).formats),
      new Property("timestampColumn", s -> ((StreamSource) s).timestampColumn)
  );

  public StreamSource(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "topicName", required = true) final String topicName,
      @JsonProperty(value = "formats", required = true) final Formats formats,
      @JsonProperty("timestampColumn") final Optional<TimestampColumn> timestampColumn,
      @JsonProperty(value = "sourceSchema", required = true) final LogicalSchema sourceSchema
  ) {
    super(
        props,
        topicName,
        formats,
        timestampColumn,
        sourceSchema
    );
  }

  @Override
  public KStreamHolder<GenericKey> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitStreamSource(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitStreamSource(this);
  }

  @Override
  public void validateUpgrade(@Nonnull final ExecutionStep<?> to) {
    ExecutionStep<?> source = to;
    while (!(source instanceof StreamSource)) {
      if (to.getSources().isEmpty()) {
        throw new KsqlException("Query is not upgradeable. The root source node of "
            + "the upgrade tree must be StreamSource, but was " + source.getClass());
      } else if (to.getSources().size() > 1) {
        throw new KsqlException("Query is not upgradeable. Cannot change a non-join source "
            + "into a join source.");
      } else if (to.type() != StepType.PASSIVE) {
        throw new KsqlException("Query is not upgradeable. Cannot add a " + to.getClass()
            + " step that is not in the original query plan.");
      }

      source = to.getSources().get(0);
    }

    mustMatch(source, MUST_MATCH);
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
    final StreamSource that = (StreamSource) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(formats, that.formats)
        && Objects.equals(timestampColumn, that.timestampColumn)
        && Objects.equals(sourceSchema, that.sourceSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        properties,
        topicName,
        formats,
        timestampColumn,
        sourceSchema
    );
  }
}
