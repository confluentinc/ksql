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

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.schema.persistence.PersistenceSchema;
import io.confluent.ksql.schema.persistence.PersistenceSchemas;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;

/**
 * Tuple of a Ksql Logical schema and serde options.
 */
@Immutable
public final class KsqlSchemaWithOptions {

  private final KsqlSchema logicalSchema;
  private final ImmutableSet<SerdeOption> serdeOptions;
  private final PersistenceSchemas physicalSchema;

  public static KsqlSchemaWithOptions of(
      final KsqlSchema logicalSchema,
      final Set<SerdeOption> serdeOptions
  ) {
    return new KsqlSchemaWithOptions(logicalSchema, serdeOptions);
  }

  public KsqlSchema getLogicalSchema() {
    return logicalSchema;
  }

  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  public PersistenceSchemas getPhysicalSchema() {
    return physicalSchema;
  }

  private KsqlSchemaWithOptions(
      final KsqlSchema logicalSchema,
      final Set<SerdeOption> serdeOptions
  ) {
    this.logicalSchema = requireNonNull(logicalSchema, "logicalSchema");
    this.serdeOptions = ImmutableSet.copyOf(requireNonNull(serdeOptions, "serdeOptions"));
    this.physicalSchema = buildPhysical(logicalSchema, serdeOptions);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlSchemaWithOptions that = (KsqlSchemaWithOptions) o;
    return Objects.equals(logicalSchema, that.logicalSchema)
        && Objects.equals(serdeOptions, that.serdeOptions)
        && Objects.equals(physicalSchema, that.physicalSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalSchema, serdeOptions);
  }

  @Override
  public String toString() {
    return "KsqlSchemaWithOptions{"
        + "logicalSchema=" + logicalSchema
        + ", serdeOptions=" + serdeOptions
        + '}';
  }

  private static PersistenceSchemas buildPhysical(
      final KsqlSchema ksqlSchema,
      final Set<SerdeOption> serdeOptions
  ) {
    final ConnectSchema schema = ksqlSchema.getSchema();

    final PersistenceSchema serializerSchema = buildValuePhysical(schema, serdeOptions);

    return PersistenceSchemas.of(serializerSchema);
  }

  private static PersistenceSchema buildValuePhysical(
      final ConnectSchema schema,
      final Set<SerdeOption> serdeOptions
  ) {
    final boolean singleField = schema.fields().size() == 1;
    final boolean unwrapSingle = serdeOptions.contains(SerdeOption.UNWRAP_SINGLE_VALUES);

    if (unwrapSingle && !singleField) {
      throw new KsqlException("'" + DdlConfig.WRAP_SINGLE_VALUE + "' "
          + "is only valid for single-field value schemas");
    }

    if (!singleField || !unwrapSingle) {
      return PersistenceSchema.of(schema);
    }

    final Field onlyField = schema.fields().get(0);
    return PersistenceSchema.of((ConnectSchema) onlyField.schema().schema());
  }
}
