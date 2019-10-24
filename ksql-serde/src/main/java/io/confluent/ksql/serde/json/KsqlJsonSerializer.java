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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlJsonSerializer implements Serializer<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlJsonSerializer.class);

  private final PersistenceSchema physicalSchema;
  private final JsonConverter jsonConverter;

  public KsqlJsonSerializer(final PersistenceSchema physicalSchema) {
    this.jsonConverter = new JsonConverter();
    this.jsonConverter.configure(ImmutableMap.of(
        JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false,
        JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()
    ), false);
    this.physicalSchema = JsonSerdeUtils.validateSchema(physicalSchema);
  }

  @Override
  public void configure(final Map<String, ?> props, final boolean isKey) {
  }

  @Override
  public byte[] serialize(final String topic, final Object data) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Serializing row. topic:{}, row:{}", topic, data);
    }

    if (data == null) {
      return null;
    }

    try {
      return jsonConverter.fromConnectData(topic, physicalSchema.serializedSchema(), data);
    } catch (final Exception e) {
      throw new SerializationException("Error serializing JSON message for topic: " + topic, e);
    }
  }

  @Override
  public void close() {
  }
}