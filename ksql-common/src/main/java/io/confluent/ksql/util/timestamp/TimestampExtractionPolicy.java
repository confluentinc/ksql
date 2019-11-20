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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.ColumnRef;
import org.apache.kafka.streams.processor.TimestampExtractor;

@Immutable
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.PROPERTY
)
@JsonSubTypes({
    @Type(value = MetadataTimestampExtractionPolicy.class, name = "metadatav1"),
    @Type(value = StringTimestampExtractionPolicy.class, name = "stringColumnv1"),
    @Type(value = LongColumnTimestampExtractionPolicy.class, name = "longColumnv1")
})
public interface TimestampExtractionPolicy {

  TimestampExtractor create(int columnIndex);

  default ColumnRef getTimestampField() {
    return null;
  }
}
