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

public class LongColumnTimestampExtractionPolicy implements TimestampExtractionPolicy {

  private final String timestampField;

  @JsonCreator
  public LongColumnTimestampExtractionPolicy(
      @JsonProperty("timestampField") final String timestampField) {
    Objects.requireNonNull(timestampField, "timestampField can't be null");
    this.timestampField = timestampField;
  }

  @Override
  public TimestampExtractor create(final int columnIndex) {
    return new LongTimestampExtractor(columnIndex);
  }

  @Override
  public String timestampField() {
    return timestampField;
  }
}
