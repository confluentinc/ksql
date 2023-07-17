/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import java.util.List;
import java.util.Map;

/**
 * Type for converting between ksqlDB and Schema Registry schema types.
 */
public interface SchemaTranslator {

  /**
   * The name of the {@code Format} specification as defined by the Confluent Schema Registry.
   *
   * <p>This should match the value returned by {@link ParsedSchema#name()}. Note that this value is
   * <i>case-sensitive</i>.
   *
   * @return the name of this Format
   */
  String name();

  /**
   * Configure the schema translator
   * @param configs Configs to configure
   */
  void configure(Map<String, ?> configs);

  /**
   * Converts the {@link ParsedSchema} returned by Confluent Schema Registry into a list of column
   * names and types.
   *
   * @param schema the {@code ParsedSchema} returned from Schema Registry
   * @param serdeFeatures serde features associated with the request
   * @param isKey whether the schema being translated is a key schema
   * @return the list of columns the schema defines
   */
  List<SimpleColumn> toColumns(ParsedSchema schema, SerdeFeatures serdeFeatures, boolean isKey);

  /**
   * Converts a {@link PersistenceSchema} into a Schema Registry's {@link ParsedSchema}.
   *
   * @param schema the ksql schema
   * @return the {@code ParsedSchema} which will be added to the Schema Registry
   */
  ParsedSchema toParsedSchema(PersistenceSchema schema);
}
