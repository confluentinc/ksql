/**
 * Copyright 2018 Confluent Inc.
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
}
