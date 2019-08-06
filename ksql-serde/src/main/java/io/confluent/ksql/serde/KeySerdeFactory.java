/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Factory of key serde instances.
 */
public interface KeySerdeFactory {

  /**
   * Create a key serde.
   *
   * @param format the format required.
   * @param schema the schema of the serialized form.
   * @param ksqlConfig the system config.
   * @param schemaRegistryClientFactory supplier of SR client.
   * @param loggerNamePrefix processing logger name prefix
   * @param processingLogContext processing logger context.
   * @return the value serde.
   */
  KeySerde<Struct> create(
      FormatInfo format,
      PersistenceSchema schema,
      KsqlConfig ksqlConfig,
      Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      String loggerNamePrefix,
      ProcessingLogContext processingLogContext
  );

  /**
   * Create a windowed key serde.
   *
   * @param format the format required.
   * @param window the window info.
   * @param schema the schema of the serialized form.
   * @param ksqlConfig the system config.
   * @param schemaRegistryClientFactory supplier of SR client.
   * @param loggerNamePrefix processing logger name prefix
   * @param processingLogContext processing logger context.
   * @return the value serde.
   */
  KeySerde<Windowed<Struct>> create(
      FormatInfo format,
      WindowInfo window,
      PersistenceSchema schema,
      KsqlConfig ksqlConfig,
      Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      String loggerNamePrefix,
      ProcessingLogContext processingLogContext
  );
}
