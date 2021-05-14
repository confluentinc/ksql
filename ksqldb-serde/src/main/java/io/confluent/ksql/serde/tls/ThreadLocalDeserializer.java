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

package io.confluent.ksql.serde.tls;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;

public class ThreadLocalDeserializer<T> implements Deserializer<T> {

  private final ThreadLocalCloseable<Deserializer<T>> deserializer;

  public ThreadLocalDeserializer(final Supplier<Deserializer<T>> initialValueSupplier) {
    deserializer = new ThreadLocalCloseable<>(initialValueSupplier);
  }

  @Override
  public void configure(final Map<String, ?> properties, final boolean isKey) {
    deserializer.get().configure(properties, isKey);
  }

  @Override
  public T deserialize(final String topicName, final byte[] record) {
    return deserializer.get().deserialize(topicName, record);
  }

  @Override
  public void close() {
    deserializer.close();
  }
}
