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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

@Immutable
public class TableSelectKey<K> implements ExecutionStep<KTableHolder<K>> {

  private static final ImmutableList<Property> MUST_MATCH = ImmutableList.of(
      new Property("class", Object::getClass),
      new Property("properties", ExecutionStep::getProperties),
      new Property("internal formats", t -> ((TableSelectKey<?>) t).internalFormats),
      new Property("keyExpressions", t -> ((TableSelectKey<?>) t).keyExpressions)
  );

  private final ExecutionStepPropertiesV1 properties;
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
  private final ImmutableList<Expression> keyExpressions;
  @EffectivelyImmutable
  private final ExecutionStep<? extends KTableHolder<K>> source;
  private final Formats internalFormats;

  public TableSelectKey(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true)
      final ExecutionStep<? extends KTableHolder<K>> source,
      @JsonProperty(value = "internalFormats", required = true) final Formats internalFormats,
      // maintain legacy name for backwards compatibility
      @JsonProperty(value = "keyExpression", required = true)
      final List<Expression> keyExpressions
  ) {
    this.properties = Objects.requireNonNull(props, "props");
    this.source = Objects.requireNonNull(source, "source");
    this.internalFormats = Objects.requireNonNull(internalFormats, "internalFormats");
    this.keyExpressions = ImmutableList.copyOf(
        Objects.requireNonNull(keyExpressions, "keyExpressions"));
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

  public Formats getInternalFormats() {
    return internalFormats;
  }

  // maintain legacy name for backwards compatibility
  @JsonProperty(value = "keyExpression", required = true)
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "keyExpressions is ImmutableList")
  public List<Expression> getKeyExpressions() {
    return keyExpressions;
  }

  public ExecutionStep<? extends KTableHolder<K>> getSource() {
    return source;
  }

  @Override
  public KTableHolder<K> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitTableSelectKey(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitTableSelectKey(this);
  }

  @Override
  public void validateUpgrade(@Nonnull final ExecutionStep<?> to) {
    mustMatch(to, MUST_MATCH);
    getSource().validateUpgrade(((TableSelectKey<?>) to).source);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableSelectKey<?> that = (TableSelectKey<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(internalFormats, that.internalFormats)
        && Objects.equals(keyExpressions, that.keyExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, source, internalFormats, keyExpressions);
  }
}
