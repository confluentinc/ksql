/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

@Immutable
public class PrintTopic extends Statement {

  private final QualifiedName topic;
  private final boolean fromBeginning;
  private final int intervalValue;
  private final OptionalInt limit;

  public PrintTopic(
      final Optional<NodeLocation> location,
      final QualifiedName topic,
      final boolean fromBeginning,
      final OptionalInt intervalValue,
      final OptionalInt limit
  ) {
    super(location);
    this.topic = requireNonNull(topic, "topic");
    this.fromBeginning = fromBeginning;
    this.intervalValue = requireNonNull(intervalValue, "intervalValue").orElse(1);
    this.limit = requireNonNull(limit, "limit");
  }

  public QualifiedName getTopic() {
    return topic;
  }

  public boolean getFromBeginning() {
    return fromBeginning;
  }

  public int getIntervalValue() {
    return intervalValue;
  }

  public OptionalInt getLimit() {
    return limit;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PrintTopic)) {
      return false;
    }
    final PrintTopic that = (PrintTopic) o;
    return fromBeginning == that.fromBeginning
        && Objects.equals(topic, that.topic)
        && intervalValue == that.intervalValue
        && Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, fromBeginning, intervalValue, limit);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("topic", topic)
        .add("fromBeginning", fromBeginning)
        .add("intervalValue", intervalValue)
        .add("limit", limit)
        .toString();
  }
}
