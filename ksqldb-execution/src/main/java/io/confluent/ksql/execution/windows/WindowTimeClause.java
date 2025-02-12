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

package io.confluent.ksql.execution.windows;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Pojo for a time clause added to a window expression
 */
@Immutable
public class WindowTimeClause {

  private final long value;

  private final TimeUnit unit;

  @JsonCreator
  public static WindowTimeClause of(
      @JsonProperty(value = "value", required = true) final long value,
      @JsonProperty(value = "timeUnit", required = true) final String timeUnit
  ) {
    return new WindowTimeClause(value, TimeUnit.valueOf(timeUnit));
  }

  public WindowTimeClause(final long value, final TimeUnit unit) {
    this.value = value;
    this.unit = requireNonNull(unit, "unit");
  }

  public Duration toDuration() {
    return Duration.ofMillis(unit.toMillis(value));
  }

  public long getValue() {
    return value;
  }

  public TimeUnit getTimeUnit() {
    return unit;
  }

  public String toString() {
    return value + " " + unit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, unit);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final WindowTimeClause otherClause = (WindowTimeClause) o;
    return otherClause.value == value && otherClause.unit == unit;
  }
}
