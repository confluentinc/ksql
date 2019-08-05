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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
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

  private static final SqlSchemaFormatter FORMATTER =
      new SqlSchemaFormatter(word -> false, Option.APPEND_NOT_NULL);

  private final boolean unwrapped;
  private final ConnectSchema ksqlSchema;
  private final ConnectSchema serializedSchema;

  /**
   * Build a persistence schema from the logical key or value schema.
   *
   * @param ksqlSchema the schema ksql uses internally, i.e. STRUCT schema.
   * @param unwrapSingle flag indicating if the serialized form is unwrapped.
   * @return the persistence schema.
   */
  public static PersistenceSchema from(final ConnectSchema ksqlSchema, final boolean unwrapSingle) {
    return new PersistenceSchema(ksqlSchema, unwrapSingle);
  }

  private PersistenceSchema(final ConnectSchema ksqlSchema, final boolean unwrapSingle) {
    this.unwrapped = unwrapSingle;
    this.ksqlSchema = Objects.requireNonNull(ksqlSchema, "ksqlSchema");

    if (ksqlSchema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("Expected STRUCT schema type");
    }

    final boolean singleField = ksqlSchema.fields().size() == 1;
    if (unwrapSingle && !singleField) {
      throw new IllegalArgumentException("Unwrapping only valid for single field");
    }

    this.serializedSchema = unwrapSingle
        ? (ConnectSchema) ksqlSchema.fields().get(0).schema()
        : ksqlSchema;
  }

  public boolean isUnwrapped() {
    return unwrapped;
  }

  /**
   * The schema used internally by KSQL.
   *
   * <p>This schema will _always_ be a struct.
   *
   * @return logical schema.
   */
  public ConnectSchema ksqlSchema() {
    return ksqlSchema;
  }

  /**
   * @return schema of serialized form
   */
  public ConnectSchema serializedSchema() {
    return serializedSchema;
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
    return unwrapped == that.unwrapped
        && Objects.equals(serializedSchema, that.serializedSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(unwrapped, serializedSchema);
  }

  @Override
  public String toString() {
    return "Persistence{"
        + "schema=" + FORMATTER.format(serializedSchema)
        + ", unwrapped=" + unwrapped
        + '}';
  }
}
