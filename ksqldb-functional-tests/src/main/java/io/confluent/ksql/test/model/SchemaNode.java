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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.SerdeFeature;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Used to track the schema and serde features used for each topic in a plan.
 */
public class SchemaNode {

  private final String logicalSchema;
  private final ImmutableSet<SerdeFeature> keyFeatures;
  private final ImmutableSet<SerdeFeature> valueFeatures;

  @SuppressWarnings("unused") // Invoked by Jackson via reflection
  @JsonCreator
  public static SchemaNode create(
      @JsonProperty(value = "schema", required = true) final String logicalSchema,
      @JsonProperty("keyFeatures") final Optional<Set<SerdeFeature>> keyFeatures,
      @JsonProperty("valueFeatures") final Optional<Set<SerdeFeature>> valueFeatures
  ) {
    return new SchemaNode(
        logicalSchema,
        keyFeatures.orElseGet(ImmutableSet::of),
        valueFeatures.orElseGet(ImmutableSet::of)
    );
  }

  public SchemaNode(
      final String logicalSchema,
      final Set<SerdeFeature> keyFeatures,
      final Set<SerdeFeature> valueFeatures
  ) {
    this.logicalSchema = requireNonNull(logicalSchema, "logicalSchema");
    this.keyFeatures = ImmutableSet.copyOf(requireNonNull(keyFeatures, "keyFeatures"));
    this.valueFeatures = ImmutableSet.copyOf(requireNonNull(valueFeatures, "valueFeatures"));
  }

  public String getSchema() {
    return logicalSchema;
  }

  @JsonInclude(Include.NON_EMPTY)
  public Set<SerdeFeature> getKeyFeatures() {
    return keyFeatures;
  }

  @JsonInclude(Include.NON_EMPTY)
  public Set<SerdeFeature> getValueFeatures() {
    return valueFeatures;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SchemaNode that = (SchemaNode) o;
    return logicalSchema.equals(that.logicalSchema)
        && keyFeatures.equals(that.keyFeatures)
        && valueFeatures.equals(that.valueFeatures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalSchema, keyFeatures, valueFeatures);
  }

  @Override
  public String toString() {
    return "SchemaNode{"
        + "logicalSchema='" + logicalSchema + '\''
        + ", keyFeatures=" + keyFeatures
        + ", valueFeatures=" + valueFeatures
        + '}';
  }

  public static SchemaNode fromPhysicalSchema(final PhysicalSchema physicalSchema) {
    return new SchemaNode(
        physicalSchema.logicalSchema().toString(),
        physicalSchema.keySchema().features().all(),
        physicalSchema.valueSchema().features().all()
    );
  }
}
