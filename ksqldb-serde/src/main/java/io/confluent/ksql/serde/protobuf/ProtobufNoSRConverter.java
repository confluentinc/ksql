/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.ProtobufSchemaAndValue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

public class ProtobufNoSRConverter implements Converter {

  private static final Serializer serializer = new Serializer();
  private static final Deserializer deserializer = new Deserializer();
  private final Schema schema;

  private ProtobufData protobufData;

  public ProtobufNoSRConverter(final Schema schema) {
    this.schema = schema;
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    this.protobufData = new ProtobufData(new ProtobufDataConfig(configs));
  }

  @Override
  public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
    try {
      final ProtobufSchemaAndValue schemaAndValue = protobufData.fromConnectData(schema, value);
      final Object v = schemaAndValue.getValue();
      if (v == null) {
        return null;
      } else if (v instanceof Message) {
        return serializer.serialize((Message) v);
      } else {
        throw new DataException("Unsupported object of class " + v.getClass().getName());
      }
    } catch (SerializationException e) {
      throw new DataException(String.format(
          "Failed to serialize Protobuf data from topic %s :",
          topic
      ), e);
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Protobuf data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  @Override
  public SchemaAndValue toConnectData(final String topic, final byte[] value) {
    try {
      final ProtobufSchema protobufSchema = protobufData.fromConnectSchema(schema);
      final Object deserialized = deserializer.deserialize(value, protobufSchema);

      if (deserialized == null) {
        return SchemaAndValue.NULL;
      } else {
        if (deserialized instanceof Message) {
          return protobufData.toConnectData(protobufSchema, (Message) deserialized);
        }
        throw new DataException(String.format(
            "Unsupported type %s returned during deserialization of topic %s ",
            deserialized.getClass().getName(),
            topic
        ));
      }
    } catch (SerializationException e) {
      throw new DataException(String.format(
          "Failed to deserialize data for topic %s to Protobuf: ",
          topic
      ), e);
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Protobuf data from topic %s : %s", topic, e.getMessage())
      );
    }
  }

  @VisibleForTesting
  public static class Serializer {
    public byte[] serialize(final Message value) {
      if (value == null) {
        return null;
      }

      try {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        value.writeTo(out);
        final byte[] bytes = out.toByteArray();
        out.close();
        return bytes;
      } catch (IOException | RuntimeException e) {
        throw new SerializationException("Error serializing Protobuf message", e);
      }
    }
  }



  @VisibleForTesting
  public static class Deserializer {
    public Object deserialize(final byte[] payload, final ProtobufSchema schema) {
      if (payload == null) {
        return null;
      }

      try {
        final ByteBuffer buffer = ByteBuffer.wrap(payload);

        final Descriptor descriptor = schema.toDescriptor();
        if (descriptor == null) {
          throw new SerializationException("Could not find descriptor with name " + schema.name());
        }
        final int length = buffer.limit();
        final int start = buffer.position() + buffer.arrayOffset();

        return DynamicMessage.parseFrom(descriptor,
            new ByteArrayInputStream(buffer.array(), start, length)
        );

      } catch (IOException | RuntimeException e) {
        throw new SerializationException("Error deserializing Protobuf message for schema "
            + schema, e);
      }
    }
  }

}
