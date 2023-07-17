/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.kafka.connect.data.Struct;

@Immutable
public class StreamSelectKeyV1 implements ExecutionStep<KStreamHolder<Struct>> {

  private static final ImmutableList<Property> MUST_MATCH = ImmutableList.of(
      new Property("class", Object::getClass),
      new Property("properties", ExecutionStep::getProperties),
      new Property("keyExpression", s -> ((StreamSelectKeyV1) s).keyExpression)
  );

  private final ExecutionStepPropertiesV1 properties;
  private final Expression keyExpression;
  @EffectivelyImmutable
  private final ExecutionStep<? extends KStreamHolder<?>> source;

  public StreamSelectKeyV1(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final
      ExecutionStep<? extends KStreamHolder<?>> source,
      @JsonProperty(value = "keyExpression", required = true) final Expression keyExpression
  ) {
    this.properties = Objects.requireNonNull(props, "props");
    this.source = Objects.requireNonNull(source, "source");
    this.keyExpression = Objects.requireNonNull(keyExpression, "keyExpression");
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

  public Expression getKeyExpression() {
    return keyExpression;
  }

  public ExecutionStep<? extends KStreamHolder<?>> getSource() {
    return source;
  }

  @Override
  public KStreamHolder<Struct> build(final PlanBuilder builder) {
    return builder.visitStreamSelectKey(this);
  }

  @Override
  public void validateUpgrade(@Nonnull final ExecutionStep<?> to) {
    mustMatch(to, MUST_MATCH);
    getSource().validateUpgrade(((StreamSelectKeyV1) to).source);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamSelectKeyV1 that = (StreamSelectKeyV1) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(keyExpression, that.keyExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, source, keyExpression);
  }
}
