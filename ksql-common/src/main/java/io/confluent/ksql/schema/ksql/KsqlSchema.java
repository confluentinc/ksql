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

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

/**
 * Immutable KSQL logical schema.
 *
 * <p>KSQL's logical schema uses the Connect {@link org.apache.kafka.connect.data.Schema}
 * interface. The interface has two main implementations: a mutable {@link
 * org.apache.kafka.connect.data.SchemaBuilder} and an immutable {@link
 * org.apache.kafka.connect.data.ConnectSchema}.
 *
 * <p>The purpose of this class is two fold:
 * <ul>
 * <li>First, to ensure the schemas used to hold the KSQL logical model are always immutable</li>
 * <li>Second, to provide a KSQL specific immutable schema type, rather than have the code
 * use {@code ConnectSchema}, which can be confusing as {@code ConnectSchema} is also used in the
 * serde code.</li>
 * </ul>
 */
@Immutable
public final class KsqlSchema {

  private final Schema schema;

  public static KsqlSchema of(final Schema schema) {
    return new KsqlSchema(schema);
  }

  private KsqlSchema(final Schema schema) {
    this.schema = requireNonNull(schema, "schema").schema();
    throwIfMutable(this.schema);
  }

  public Schema getSchema() {
    return schema;
  }

  private static void throwIfMutable(final Schema schema) {
    if (!(schema instanceof ConnectSchema)) {
      throw new IllegalArgumentException("Mutable schema found: " + schema);
    }

    if (schema.type() == Type.STRUCT) {
      schema.fields().forEach(field -> throwIfMutable(field.schema()));
    }

    if (schema.type() == Type.MAP) {
      throwIfMutable(schema.keySchema());
    }

    if (schema.type() == Type.MAP || schema.type() == Type.ARRAY) {
      throwIfMutable(schema.valueSchema());
    }
  }
}
