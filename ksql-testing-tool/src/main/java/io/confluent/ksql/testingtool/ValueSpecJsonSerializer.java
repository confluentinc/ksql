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


package io.confluent.ksql.testingtool;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class ValueSpecJsonSerializer implements Serializer<Object> {
  @Override
  public void close() {
  }

  @Override
  public void configure(final Map<String, ?> properties, final boolean b) {
  }

  @Override
  public byte[] serialize(final String topicName, final Object spec) {
    if (spec == null) {
      return null;
    }
    try {
      return new ObjectMapper().writeValueAsBytes(spec);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}