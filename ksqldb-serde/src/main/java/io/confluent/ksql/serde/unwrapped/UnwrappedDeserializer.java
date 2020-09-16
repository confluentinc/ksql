/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde.unwrapped;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public class UnwrappedDeserializer implements Deserializer<List<?>> {

  private final Deserializer<?> inner;

  public UnwrappedDeserializer(
      final Deserializer<?> inner
  ) {
    this.inner = requireNonNull(inner, "inner");
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    inner.configure(configs, isKey);
  }

  @Override
  public List<?> deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    final Object single = inner.deserialize(topic, bytes);
    return Collections.singletonList(single);
  }

  @Override
  public List<?> deserialize(final String topic, final Headers headers, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    final Object single = inner.deserialize(topic, headers, bytes);
    return Collections.singletonList(single);
  }

  @Override
  public void close() {
    inner.close();
  }
}

