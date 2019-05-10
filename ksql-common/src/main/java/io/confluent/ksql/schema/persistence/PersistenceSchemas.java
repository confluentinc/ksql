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
import io.confluent.ksql.schema.ksql.KsqlSchema;
import java.util.Objects;

/**
 * Pojo for holding schemas used for persistence.
 *
 * <p>While {@link KsqlSchema} represents the logical schema used by KSQL. The schema used to
 * persist keys and values may be different.  The logical schema is always a STRUCT, where as
 * persistence schema may be a STRUCT, or if persisting a single unwrapped field, it may be a
 * primitive, array or map.
 */
@Immutable
public final class PersistenceSchemas {

  private final PersistenceSchema valueSchema;

  public static PersistenceSchemas of(final PersistenceSchema valueSchema) {
    return new PersistenceSchemas(valueSchema);
  }

  private PersistenceSchemas(final PersistenceSchema valueSchema) {
    this.valueSchema = Objects.requireNonNull(valueSchema, "valueSchema");
  }

  public PersistenceSchema valueSchema() {
    return valueSchema;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PersistenceSchemas that = (PersistenceSchemas) o;
    return Objects.equals(valueSchema, that.valueSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueSchema);
  }

  @Override
  public String toString() {
    return "{"
        + "valueSchema=" + valueSchema
        + '}';
  }
}
