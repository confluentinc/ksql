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

package io.confluent.ksql.serde.voids;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serde for handling voids.
 *
 * <p>Unlike the serde returned by {@link Serdes#Void()}, the deserializer returned here will not
 * throw if it encounters non-null data to deserialize. This means the deserializer can be used
 * where the source record contains a key or value, but the user does not want to deserialize it.
 */
public final class KsqlVoidSerde<T> extends WrapperSerde<T> {

  public KsqlVoidSerde() {
    super(new LaxVoidSerializer<>(), new LaxVoidDeserializer<>());
  }

  public static final class LaxVoidSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(final String s, final T t) {
      return null;
    }
  }

  public static final class LaxVoidDeserializer<T> implements Deserializer<T> {

    @Override
    public T deserialize(final String topic, final byte[] data) {
      return null;
    }
  }
}
