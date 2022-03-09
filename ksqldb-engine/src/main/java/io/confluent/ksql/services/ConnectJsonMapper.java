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

package io.confluent.ksql.services;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.json.KsqlTypesSerializationModule;
import io.confluent.ksql.json.StructSerializationModule;
import java.io.IOException;
import java.util.Locale;
import org.apache.kafka.connect.runtime.rest.entities.PluginInfo;

/**
 * Json Mapper used by Connect integration.
 */
public enum ConnectJsonMapper {

  INSTANCE;

  private final ObjectMapper mapper = buildMapper();

  final ObjectMapper buildMapper() {
    final SimpleModule module = new SimpleModule();
    module.addSerializer(PluginInfo.class, new PluginInfoJsonSerializer());

    return new ObjectMapper()
        .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
        .registerModule(new Jdk8Module())
        .registerModule(new StructSerializationModule())
        .registerModule(new KsqlTypesSerializationModule())
        .registerModule(module)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
        .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
  }

  public ObjectMapper get() {
    return mapper.copy();
  }

  static class PluginInfoJsonSerializer extends JsonSerializer<PluginInfo> {
    @Override
    public void serialize(final PluginInfo value, final JsonGenerator gen,
        final SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("class", value.className());
      gen.writeStringField("type", value.type().toUpperCase(Locale.ROOT));
      gen.writeStringField("version", value.version());
      gen.writeEndObject();
    }
  }
}