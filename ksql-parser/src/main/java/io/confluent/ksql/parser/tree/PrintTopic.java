/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.commons.lang3.StringUtils;

public class PrintTopic extends Statement {

  private final QualifiedName topic;
  private final boolean fromBeginning;
  private final int intervalValue;
  private final Integer limit;


  public PrintTopic(
      final NodeLocation location,
      final QualifiedName topic,
      final boolean fromBeginning,
      final Optional<Integer> intervalValue,
      final Optional<String> limit
  ) {
    this(Optional.of(location), topic, fromBeginning, intervalValue, limit);
  }

  private PrintTopic(
      final Optional<NodeLocation> location,
      final QualifiedName topic,
      final boolean fromBeginning,
      final Optional<Integer> intervalValue,
      final Optional<String> limit
  ) {
    super(location);
    this.topic = requireNonNull(topic, "table is null");
    this.fromBeginning = fromBeginning;
    this.intervalValue = intervalValue.orElse(1);
    this.limit = limit.filter(StringUtils::isNumeric)
        .map(Integer::parseInt)
        .orElse(null);
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
    return (limit == null) ? OptionalInt.empty() : OptionalInt.of(limit);
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
    return getFromBeginning() == that.getFromBeginning()
        && Objects.equals(getTopic(), that.getTopic())
        && getIntervalValue() == that.getIntervalValue()
        && Objects.equals(getLimit(), that.getLimit());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTopic(), getFromBeginning(), getIntervalValue(), getLimit());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("topic", topic)
        .toString();
  }
}
