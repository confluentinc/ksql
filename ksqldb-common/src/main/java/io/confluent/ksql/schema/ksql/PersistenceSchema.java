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

package io.confluent.ksql.schema.ksql;

import static io.confluent.ksql.serde.SerdeFeature.UNWRAP_SINGLES;
import static io.confluent.ksql.serde.SerdeFeature.WRAP_SINGLES;
import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.serde.EnabledSerdeFeatures;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Objects;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema.Type;

/**
 * Type-safe schema used purely for persistence.
 *
 * <p>There are a lot of different schema types in KSQL. This is a wrapper around the connect
 * schema type used to indicate the schema is for use only for persistence, i.e. it is a
 * schema that represents how parts of a row should be serialized, or are serialized, e.g. the
 * Kafka message's value or key.
 *
 * <p>Having a specific type allows code to be more type-safe when it comes to dealing with
 * different schema types.
 */
@Immutable
public final class PersistenceSchema {

  @EffectivelyImmutable
  private static final SqlSchemaFormatter FORMATTER =
      new SqlSchemaFormatter(word -> false, Option.APPEND_NOT_NULL);

  private final ConnectSchema schema;
  private final EnabledSerdeFeatures features;

  /**
   * Build a persistence schema from the logical key or value schema.
   *
   * @param schema the schema ksql uses internally, i.e. STRUCT schema.
   * @param features the serder features used for persistence.
   * @return the persistence schema.
   */
  public static PersistenceSchema from(
      final ConnectSchema schema,
      final EnabledSerdeFeatures features
  ) {
    return new PersistenceSchema(schema, features);
  }

  private PersistenceSchema(
      final ConnectSchema ksqlSchema,
      final EnabledSerdeFeatures features
  ) {
    this.features = requireNonNull(features, "features");
    this.schema = requireNonNull(ksqlSchema, "ksqlSchema");

    if (ksqlSchema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("Expected STRUCT schema type");
    }

    if (features.enabled(WRAP_SINGLES) || features.enabled(UNWRAP_SINGLES)) {
      if (ksqlSchema.fields().size() != 1) {
        throw new IllegalArgumentException("Unwrapping only valid for single field");
      }
    }
  }

  public EnabledSerdeFeatures features() {
    return features;
  }

  public ConnectSchema connectSchema() {
    return schema;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PersistenceSchema that = (PersistenceSchema) o;
    return Objects.equals(features, that.features)
        && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(features, schema);
  }

  @Override
  public String toString() {
    return "Persistence{"
        + "schema=" + FORMATTER.format(schema)
        + ", features=" + features
        + '}';
  }
}
