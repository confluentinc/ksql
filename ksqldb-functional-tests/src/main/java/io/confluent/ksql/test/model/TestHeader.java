/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.common.header.Header;

@JsonSerialize(using = TestHeader.Serializer.class)
public class TestHeader implements Header {
  private final String key;
  private final byte[] value;

  public TestHeader(
      @JsonProperty("KEY") final String key,
      @JsonProperty("VALUE") final byte[] value
  ) {
    this.key = requireNonNull(key, "key");
    this.value = value == null ? null : Arrays.copyOf(value, value.length);
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public byte[] value() {
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TestHeader that = (TestHeader) o;
    return key.equals(that.key)
        && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  public static class Serializer extends JsonSerializer<TestHeader> {

    @Override
    public void serialize(
        final TestHeader record,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializerProvider
    ) throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeObjectField("KEY", record.key);
      if (record.value == null) {
        jsonGenerator.writeNullField("VALUE");
      } else {
        jsonGenerator.writeBinaryField("VALUE", record.value);
      }
      jsonGenerator.writeEndObject();
    }
  }
}
