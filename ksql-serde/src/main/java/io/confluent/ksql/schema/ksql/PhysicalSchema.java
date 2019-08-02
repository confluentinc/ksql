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
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.connect.data.ConnectSchema;

/**
 * Physical KSQL schema.
 *
 * <p/>The physical KSQL schema is a combination of a logical schema and the serialization options
 * used to control the serialized form.
 */
@Immutable
public final class PhysicalSchema {

  private final LogicalSchema logicalSchema;
  private final ImmutableSet<SerdeOption> serdeOptions;
  private final PersistenceSchema keySchema;
  private final PersistenceSchema valueSchema;

  public static PhysicalSchema from(
      final LogicalSchema logicalSchema,
      final Set<SerdeOption> serdeOptions
  ) {
    return new PhysicalSchema(logicalSchema, serdeOptions);
  }

  /**
   * @return the logical schema used to build this physical schema.
   */
  public LogicalSchema logicalSchema() {
    return logicalSchema;
  }

  /**
   * @return the serde options of this physical schema.
   */
  public Set<SerdeOption> serdeOptions() {
    return serdeOptions;
  }

  /**
   * @return the physical key schema.
   */
  public PersistenceSchema keySchema() {
    return keySchema;
  }

  /**
   * @return the physical value schema.
   */
  public PersistenceSchema valueSchema() {
    return valueSchema;
  }

  private PhysicalSchema(
      final LogicalSchema logicalSchema,
      final Set<SerdeOption> serdeOptions
  ) {
    this.logicalSchema = requireNonNull(logicalSchema, "logicalSchema");
    this.serdeOptions = ImmutableSet.copyOf(requireNonNull(serdeOptions, "serdeOptions"));
    this.keySchema = buildKeyPhysical(logicalSchema.keySchema());
    this.valueSchema = buildValuePhysical(logicalSchema.valueSchema(), serdeOptions);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PhysicalSchema that = (PhysicalSchema) o;
    return Objects.equals(logicalSchema, that.logicalSchema)
        && Objects.equals(serdeOptions, that.serdeOptions)
        && Objects.equals(valueSchema, that.valueSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalSchema, serdeOptions);
  }

  @Override
  public String toString() {
    return "PhysicalSchema{"
        + "logicalSchema=" + logicalSchema
        + ", serdeOptions=" + serdeOptions
        + '}';
  }

  private static PersistenceSchema buildKeyPhysical(
      final ConnectSchema keyConnectSchema
  ) {
    return PersistenceSchema.from(keyConnectSchema, false);
  }

  private static PersistenceSchema buildValuePhysical(
      final ConnectSchema valueConnectSchema,
      final Set<SerdeOption> serdeOptions
  ) {
    final boolean singleField = valueConnectSchema.fields().size() == 1;
    final boolean unwrapSingle = serdeOptions.contains(SerdeOption.UNWRAP_SINGLE_VALUES);
    if (unwrapSingle && !singleField) {
      throw new KsqlException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE + "' "
          + "is only valid for single-field value schemas");
    }

    return PersistenceSchema.from(valueConnectSchema, unwrapSingle);
  }
}
