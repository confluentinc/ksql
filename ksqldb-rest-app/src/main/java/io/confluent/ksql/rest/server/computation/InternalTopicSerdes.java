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

package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import java.io.IOException;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public final class InternalTopicSerdes {

  private static final ObjectMapper MAPPER = PlanJsonMapper.INSTANCE.get();

  private InternalTopicSerdes() {
  }

  public static <T> Serializer<T> serializer() {
    return new InternalTopicSerializer<>();
  }

  public static <T> Deserializer<T> deserializer(final Class<T> clazz) {
    return new InternalTopicDeserializer<>(clazz);
  }

  private static final class InternalTopicSerializer<T> implements Serializer<T> {
    public byte[] serialize(final String topic, final T obj) {
      try {
        return MAPPER.writeValueAsBytes(obj);
      } catch (final IOException e) {
        throw new SerializationException(e);
      }
    }
  }

  private static final class InternalTopicDeserializer<T> implements Deserializer<T> {
    private final Class<T> clazz;

    private InternalTopicDeserializer(final Class<T> clazz) {
      this.clazz = Objects.requireNonNull(clazz, "clazz");
    }

    public T deserialize(final String topic, final byte[] serialized) {
      try {
        return MAPPER.readValue(serialized, clazz);
      } catch (final IOException e) {
        throw new SerializationException(e);
      }
    }
  }
}
