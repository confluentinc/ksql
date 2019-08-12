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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

@Immutable
public class StreamTableJoin<K> implements ExecutionStep<KStream<K, GenericRow>> {

  private final ExecutionStepProperties properties;
  private final JoinType joinType;
  private final Formats formats;
  private final ExecutionStep<KStream<K, GenericRow>> left;
  private final ExecutionStep<KTable<K, GenericRow>> right;

  public StreamTableJoin(
      final ExecutionStepProperties properties,
      final JoinType joinType,
      final Formats formats,
      final ExecutionStep<KStream<K, GenericRow>> left,
      final ExecutionStep<KTable<K, GenericRow>> right) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.joinType = Objects.requireNonNull(joinType, "joinType");
    this.left = Objects.requireNonNull(left, "left");
    this.right = Objects.requireNonNull(right, "right");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return ImmutableList.of(left, right);
  }

  @Override
  public KStream<K, GenericRow> build(final StreamsBuilder streamsBuilder) {
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
    final StreamTableJoin<?> that = (StreamTableJoin<?>) o;
    return Objects.equals(properties, that.properties)
        && joinType == that.joinType
        && Objects.equals(formats, that.formats)
        && Objects.equals(left, that.left)
        && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, joinType, formats, left, right);
  }
}
