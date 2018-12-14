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

package io.confluent.ksql.util.timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class StringTimestampExtractionPolicy implements TimestampExtractionPolicy {

  private final String timestampField;
  private final String format;

  @JsonCreator
  public StringTimestampExtractionPolicy(
      @JsonProperty("timestampField") final String timestampField,
      @JsonProperty("format") final String format) {
    Objects.requireNonNull(timestampField, "timestampField can't be null");
    Objects.requireNonNull(format, "format can't be null");
    this.timestampField = timestampField;
    this.format = format;
  }

  @Override
  public TimestampExtractor create(final int timestampColumnIndex) {
    return new StringTimestampExtractor(format, timestampColumnIndex);
  }

  @Override
  public String timestampField() {
    return timestampField;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestampField, format);
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StringTimestampExtractionPolicy)) {
      return false;
    }
    final StringTimestampExtractionPolicy otherPolicy
        = (StringTimestampExtractionPolicy) other;
    return Objects.equals(otherPolicy.timestampField, timestampField)
        && Objects.equals(otherPolicy.format, format);
  }
}
