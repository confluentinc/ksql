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
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;

interface SerdeFactories {

  /**
   * Create {@link Serde} for supported KSQL formats.
   *
   * @param format required format.
   * @param schema persitence schema
   * @param ksqlConfig system config.
   * @param schemaRegistryClientFactory the sr client factory.
   * @param type the value type.
   * @param <T> the value type.
   */
  <T> Serde<T> create(
      FormatInfo format,
      PersistenceSchema schema,
      KsqlConfig ksqlConfig,
      Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      Class<T> type
  );
}
