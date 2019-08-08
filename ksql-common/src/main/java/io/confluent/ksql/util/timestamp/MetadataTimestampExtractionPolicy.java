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

package io.confluent.ksql.util.timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;

@Immutable
public class MetadataTimestampExtractionPolicy implements TimestampExtractionPolicy {
  private final TimestampExtractor timestampExtractor;

  @JsonCreator
  public MetadataTimestampExtractionPolicy() {
    this(new FailOnInvalidTimestamp());
  }

  public MetadataTimestampExtractionPolicy(final TimestampExtractor timestampExtractor) {
    this.timestampExtractor = timestampExtractor;
  }

  @Override
  public TimestampExtractor create(final int columnIndex) {
    return timestampExtractor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getClass(), timestampExtractor.getClass());
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof MetadataTimestampExtractionPolicy)) {
      return false;
    }

    final MetadataTimestampExtractionPolicy that = (MetadataTimestampExtractionPolicy)other;
    return timestampExtractor.getClass() == that.timestampExtractor.getClass();
  }
}
