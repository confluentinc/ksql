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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import java.util.Objects;
import org.apache.kafka.connect.data.ConnectSchema;

/**
 * Type-safe schema used purely for persistence.
 *
 * <p>There are a lot of different schema types in KSQL. This is a wrapper around the connect
 * schema type used to indicate the schema is for use only in persistence. Allowing code to be more
 * type-safe when it comes to dealing with different schema types.
 */
@Immutable
public final class PersistenceSchema {

  private final ConnectSchema connectSchema;

  private PersistenceSchema(final ConnectSchema schema) {
    this.connectSchema = Objects.requireNonNull(schema, "schema");
  }

  public static PersistenceSchema of(final ConnectSchema schema) {
    return new PersistenceSchema(schema);
  }

  public ConnectSchema getConnectSchema() {
    return connectSchema;
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
    return Objects.equals(connectSchema, that.connectSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectSchema);
  }

  @Override
  public String toString() {
    return "Persistence{" + SqlSchemaFormatter.STRICT.format(connectSchema) + '}';
  }
}
