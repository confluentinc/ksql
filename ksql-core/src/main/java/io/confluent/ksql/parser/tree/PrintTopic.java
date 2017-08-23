/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PrintTopic extends Statement {

  private final QualifiedName topic;
  private final boolean fromBeginning;
  private final LongLiteral intervalValue;

  public PrintTopic(QualifiedName topic, boolean fromBeginning, LongLiteral intervalValue) {
    this(Optional.empty(), topic, fromBeginning, intervalValue);
  }

  public PrintTopic(
      NodeLocation location,
      QualifiedName topic,
      boolean fromBeginning,
      LongLiteral intervalValue
  ) {
    this(Optional.of(location), topic, fromBeginning, intervalValue);
  }

  private PrintTopic(
      Optional<NodeLocation> location,
      QualifiedName topic,
      boolean fromBeginning,
      LongLiteral intervalValue
  ) {
    super(location);
    this.topic = requireNonNull(topic, "table is null");
    this.fromBeginning = fromBeginning;
    this.intervalValue = intervalValue;
  }

  public QualifiedName getTopic() {
    return topic;
  }

  public boolean getFromBeginning() {
    return fromBeginning;
  }

  public LongLiteral getIntervalValue() {
    return intervalValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PrintTopic)) {
      return false;
    }
    PrintTopic that = (PrintTopic) o;
    return getFromBeginning() == that.getFromBeginning()
        && Objects.equals(getTopic(), that.getTopic())
        && Objects.equals(getIntervalValue(), that.getIntervalValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTopic(), getFromBeginning(), getIntervalValue());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("topic", topic)
        .toString();
  }
}
