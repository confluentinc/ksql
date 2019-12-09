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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;

@Immutable
public class StreamSelectKey implements ExecutionStep<KStreamHolder<Struct>> {

  private final ExecutionStepPropertiesV1 properties;
  private final Expression keyExpression;
  @EffectivelyImmutable
  private final ExecutionStep<? extends KStreamHolder<?>> source;

  public StreamSelectKey(
      @JsonProperty(value = "properties", required = true) ExecutionStepPropertiesV1 properties,
      @JsonProperty(value = "source", required = true)
      ExecutionStep<? extends KStreamHolder<?>> source,
      @JsonProperty(value = "keyExpression", required = true) Expression keyExpression
  ) {
    this.properties = Objects.requireNonNull(properties, "properties");
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
  public KStreamHolder<Struct> build(PlanBuilder builder) {
    return builder.visitStreamSelectKey(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamSelectKey that = (StreamSelectKey) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source);
  }
}
