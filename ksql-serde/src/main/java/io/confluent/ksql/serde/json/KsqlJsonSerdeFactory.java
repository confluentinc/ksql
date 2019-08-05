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

import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

@Immutable
public class KsqlJsonSerdeFactory implements KsqlSerdeFactory {

  @Override
  public void validate(final PersistenceSchema schema) {
    // Supports all types
  }

  @Override
  public Serde<Object> createSerde(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    return Serdes.serdeFrom(
        new KsqlJsonSerializer(schema),
        new KsqlJsonDeserializer(schema)
    );
  }
}
