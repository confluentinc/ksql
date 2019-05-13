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

package io.confluent.ksql.schema.persistence;

import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;

/**
 * Factory for {@link PersistenceSchemas}.
 */
public final class PersistenceSchemasFactory {

  private PersistenceSchemasFactory() {
  }

  public static PersistenceSchemas from(
      final KsqlSchema ksqlSchema,
      final KsqlConfig config
  ) {
    final ConnectSchema schema = ksqlSchema.getSchema();

    final PersistenceSchema serializerSchema = getValueSchema(schema, config);

    return PersistenceSchemas.of(serializerSchema);
  }

  private static PersistenceSchema getValueSchema(
      final ConnectSchema schema,
      final KsqlConfig ksqlConfig
  ) {
    if (schema.fields().size() != 1
        || ksqlConfig.getBoolean(KsqlConfig.KSQL_WRAP_SINGLE_VALUES)
    ) {
      return PersistenceSchema.of(schema);
    }

    final Field onlyField = schema.fields().get(0);
    return PersistenceSchema.of((ConnectSchema) onlyField.schema());
  }
}
