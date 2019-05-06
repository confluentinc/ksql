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
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;

@Immutable
public class MetadataTimestampExtractionPolicy implements TimestampExtractionPolicy {

  @JsonCreator
  public MetadataTimestampExtractionPolicy(){}

  @Override
  public TimestampExtractor create(final int columnIndex) {
    return new FailOnInvalidTimestamp();
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    return other instanceof MetadataTimestampExtractionPolicy;
  }
}
