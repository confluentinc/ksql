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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.serde.SerdeFeatures;
import java.util.List;
import java.util.Objects;

/**
 * Type-safe schema used purely for persistence.
 *
 * <p>There are a lot of different schema types in KSQL. A {@code PersistenceSchema} is a
 * combination of a list of columns representing either the columns in either the key or value of a
 * {@link LogicalSchema}, and a set of enabled {@link io.confluent.ksql.serde.SerdeFeature
 * SerdeFeatures} that control how the columns should be serialized, i.e. it is a schema that
 * represents how parts of a row should be serialized, or are serialized, e.g. the Kafka message's
 * value or key.
 */
@Immutable
public final class PersistenceSchema {

  private final ImmutableList<SimpleColumn> columns;
  private final SerdeFeatures features;

  /**
   * Build a persistence schema from the logical key or value schema.
   *
   * @param columns the list of columns to be serialized.
   * @param features the serder features used for persistence.
   * @return the persistence schema.
   */
  public static PersistenceSchema from(
      final List<? extends SimpleColumn> columns,
      final SerdeFeatures features
  ) {
    return new PersistenceSchema(columns, features);
  }

  private PersistenceSchema(
      final List<? extends SimpleColumn> columns,
      final SerdeFeatures features
  ) {
    this.features = requireNonNull(features, "features");
    this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns"));

    if (features.enabled(WRAP_SINGLES) || features.enabled(UNWRAP_SINGLES)) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("Unwrapping only valid for single field");
      }
    }
  }

  public SerdeFeatures features() {
    return features;
  }

  public List<SimpleColumn> columns() {
    return columns;
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
        && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(features, columns);
  }

  @Override
  public String toString() {
    return "Persistence{"
        + "columns=" + columns
        + ", features=" + features
        + '}';
  }


}
