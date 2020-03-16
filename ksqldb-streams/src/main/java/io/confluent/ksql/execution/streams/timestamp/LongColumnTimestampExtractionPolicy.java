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

package io.confluent.ksql.execution.streams.timestamp;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import java.util.Objects;

@Immutable
public class LongColumnTimestampExtractionPolicy implements TimestampExtractionPolicy {

  private final ColumnName timestampField;

  public LongColumnTimestampExtractionPolicy(final ColumnName timestampField) {
    Objects.requireNonNull(timestampField, "timestampField can't be null");
    this.timestampField = timestampField;
  }

  @Override
  public KsqlTimestampExtractor create(final int columnIndex) {
    return new LongTimestampExtractor(columnIndex);
  }

  @Override
  public ColumnName getTimestampField() {
    return timestampField;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestampField);
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LongColumnTimestampExtractionPolicy)) {
      return false;
    }
    final LongColumnTimestampExtractionPolicy otherPolicy
        = (LongColumnTimestampExtractionPolicy) other;
    return Objects.equals(otherPolicy.timestampField, timestampField);
  }
}
