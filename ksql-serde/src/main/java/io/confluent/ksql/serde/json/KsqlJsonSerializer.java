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

package io.confluent.ksql.serde.json;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlJsonSerializer implements Serializer<Struct> {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlJsonSerializer.class);

  private final Schema schema;
  private final JsonConverter jsonConverter;
  private final Function<Struct, Object> preprocessor;

  public KsqlJsonSerializer(final Schema schema) {
    this.schema = Objects.requireNonNull(schema, "schema").schema();
    this.jsonConverter = new JsonConverter();
    this.jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);

    this.preprocessor = schema.type() == Type.STRUCT
        ? t -> t
        : KsqlJsonSerializer::toSingleField;
  }

  @Override
  public void configure(final Map<String, ?> props, final boolean isKey) {
  }

  @Override
  public byte[] serialize(final String topic, final Struct data) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Serializing row. topic:{}, row:{}", topic, data);
    }

    if (data == null) {
      return null;
    }

    final Object toSerialize = preprocessor.apply(data);

    try {
      return jsonConverter.fromConnectData(topic, schema, toSerialize);
    } catch (final Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }

  @Override
  public void close() {
  }

  private static Object toSingleField(final Struct struct) {
    if (struct.schema().fields().size() != 1) {
      throw new SerializationException("Expected to serialize JSON value or array not JSON object");
    }

    return struct.get(struct.schema().fields().get(0));
  }
}