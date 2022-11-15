/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import java.util.Objects;
import java.util.Optional;

@Immutable
public final class TimestampColumn {
  private final ColumnName column;
  private final Optional<String> format;

  @JsonCreator
  public TimestampColumn(
      @JsonProperty(value = "column", required = true) final ColumnName column,
      @JsonProperty("format") final Optional<String> format
  ) {
    this.column = Objects.requireNonNull(column, "column");
    this.format = Objects.requireNonNull(format, "format");
  }

  public ColumnName getColumn() {
    return column;
  }

  public Optional<String> getFormat() {
    return format;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TimestampColumn that = (TimestampColumn) o;
    return Objects.equals(column, that.column)
        && Objects.equals(format, that.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, format);
  }

  @Override
  public String toString() {
    return "TimestampColumn{" + "column=" + column
        + ", format=" + format
        + '}';
  }
}
