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
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;

@Immutable
public class KsqlJsonSerdeFactory extends KsqlSerdeFactory {

  public KsqlJsonSerdeFactory() {
    super(Format.JSON);
  }

  @Override
  public void validate(final ConnectSchema schema) {
    // Supports all types
  }

  @Override
  protected Serializer<Object> createSerializer(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final Serializer<Object> serializer = new KsqlJsonSerializer(schema);
    serializer.configure(Collections.emptyMap(), false);
    return serializer;
  }

  @Override
  protected Deserializer<Object> createDeserializer(
      final PersistenceSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final Deserializer<Object> deserializer = new KsqlJsonDeserializer(schema);
    deserializer.configure(Collections.emptyMap(), false);
    return deserializer;
  }
}
