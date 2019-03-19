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

import static io.confluent.ksql.logging.processing.ProcessingLoggerUtil.join;

import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.util.SerdeUtils;
import io.confluent.ksql.util.KsqlConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;

@Immutable
public class KsqlJsonTopicSerDe extends KsqlTopicSerDe {

  public KsqlJsonTopicSerDe() {
    super(DataSource.DataSourceSerDe.JSON);
  }


  @Override
  public Serde<GenericRow> getGenericRowSerde(final Schema schema, final KsqlConfig ksqlConfig,
      final boolean isInternal,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext) {
    final Map<String, Object> serdeProps = new HashMap<>();
    serdeProps.put("JsonPOJOClass", GenericRow.class);

    final Serializer<GenericRow> genericRowSerializer = new KsqlJsonSerializer(schema);
    genericRowSerializer.configure(serdeProps, false);

    final Deserializer<GenericRow> genericRowDeserializer = new KsqlJsonDeserializer(
        schema,
        isInternal,
        processingLogContext.getLoggerFactory().getLogger(
            join(loggerNamePrefix, SerdeUtils.DESERIALIZER_LOGGER_NAME))
    );

    genericRowDeserializer.configure(serdeProps, false);

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }
}
