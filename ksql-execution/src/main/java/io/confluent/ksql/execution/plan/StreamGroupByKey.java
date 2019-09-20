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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedStream;

@Immutable
public class StreamGroupByKey implements ExecutionStep<KGroupedStream<Struct, GenericRow>> {
  private final ExecutionStepProperties properties;
  private final ExecutionStep<KStreamHolder<Struct>> source;
  private final Formats formats;

  public StreamGroupByKey(
      final ExecutionStepProperties properties,
      final ExecutionStep<KStreamHolder<Struct>> source,
      final Formats formats) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.source = Objects.requireNonNull(source, "source");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public ExecutionStep<KStreamHolder<Struct>> getSource() {
    return source;
  }

  public Formats getFormats() {
    return formats;
  }

  @Override
  public KGroupedStream<Struct, GenericRow> build(final PlanBuilder builder) {
    return builder.visitStreamGroupByKey(this);
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
        && Objects.equals(formats, that.formats);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, formats);
  }
}
