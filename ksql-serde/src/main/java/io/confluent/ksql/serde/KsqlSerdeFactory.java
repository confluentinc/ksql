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

package io.confluent.ksql.serde;

import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;

@Immutable
public interface KsqlSerdeFactory {

  /**
   * @return the format this serde factory supports
   */
  Format getFormat();

  /**
   * Validate the serde factory can handle the supplied {@code schema}.
   *
   * @param schema the schema to validate.
   */
  void validate(PersistenceSchema schema);

  /**
   * Create the serde.
   *
   * @param schema the persistence schema, i.e. the physical schema of the data on-disk.
   * @param ksqlConfig the config to use.
   * @param schemaRegistryClientFactory the schema registry client to use.
   */
  Serde<Object> createSerde(
      PersistenceSchema schema,
      KsqlConfig ksqlConfig,
      Supplier<SchemaRegistryClient> schemaRegistryClientFactory);
}
