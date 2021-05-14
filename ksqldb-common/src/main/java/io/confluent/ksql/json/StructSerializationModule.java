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

package io.confluent.ksql.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;

public class StructSerializationModule extends SimpleModule {

  private static final JsonConverter jsonConverter = new JsonConverter();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public StructSerializationModule() {
    super();
    jsonConverter.configure(ImmutableMap.of(
        JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false,
        JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()
    ), false);
    addSerializer(Struct.class, new StructSerializationModule.Serializer());
  }

  static class Serializer extends JsonSerializer<Struct> {

    @Override
    public void serialize(
        final Struct struct,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializerProvider
    ) throws IOException {
      struct.validate();
      jsonGenerator.writeObject(
          objectMapper.readTree(jsonConverter.fromConnectData("", struct.schema(), struct)));
    }
  }

}