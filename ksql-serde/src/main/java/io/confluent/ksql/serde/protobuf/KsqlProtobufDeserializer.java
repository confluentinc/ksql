/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import com.google.protobuf.MessageOrBuilder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.protobuf.transformer.ProtobufTransformer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;

public class KsqlProtobufDeserializer implements Deserializer<GenericRow> {

  private static final Logger logger = LoggerFactory.getLogger(KsqlProtobufDeserializer.class);
  private final ProtobufTransformer protobufTransformer;

  private Class protobufType = null;
  private Method parseFromMethod = null;

  /**
   * Default constructor needed by Kafka
   */
  public KsqlProtobufDeserializer(final Schema schema, final boolean isInternal) {
    this.protobufTransformer = new ProtobufTransformer(schema);
  }

  @Override
  public void configure(final Map<String, ?> map, boolean b) {
    logger.info("Got properties: {}", map);
    // Get class from config
    final String classStr = (String) map.get(KsqlProtobufTopicSerDe.CONFIG_PROTOBUF_CLASS);
    try {
      protobufType = Class.forName(classStr);
      parseFromMethod = protobufType.getMethod("parseFrom", byte[].class);

    } catch (ClassNotFoundException | NoSuchMethodException e) {
      // TODO handle.
      logger.error("Caught exception: {}", e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try {
      // Parse Protobuf from the bytes
      final MessageOrBuilder message
        = (MessageOrBuilder) parseFromMethod.invoke(protobufType, bytes);
      return protobufTransformer.convert(message);
    } catch (Exception e) {
      throw new SerializationException(
        "KsqlJsonDeserializer failed to deserialize data for topic: " + topic,
        e
      );
    }
  }

  @Override
  public void close() {

  }
}