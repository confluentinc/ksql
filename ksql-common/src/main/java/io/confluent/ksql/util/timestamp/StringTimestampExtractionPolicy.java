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

import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Objects;

import io.confluent.ksql.util.KsqlConfig;

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
  public void applyTo(final KsqlConfig config,
                      final Map<String, Object> newStreamProperties) {
    newStreamProperties.put(
        KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX,
        config.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX)
    );
    newStreamProperties.put(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        StringTimestampExtractor.class
    );
    newStreamProperties.put(KsqlConfig.STRING_TIMESTAMP_FORMAT, format);
  }

  @Override
  public String timestampField() {
    return timestampField;
  }
}
