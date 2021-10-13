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

package io.confluent.ksql.serde;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.CompatibleElement;
import java.util.Arrays;
import java.util.Set;

/**
 * Optional features a serde may support
 */
public enum SerdeFeature implements CompatibleElement<SerdeFeature> {

  /**
   * The format supports interaction with the Confluent Schema Registry.
   *
   * <p>Indicates whether or not a format can support CREATE statements that
   * omit the table elements and instead determine the schema from a Confluent Schema Registry
   * query.
   */
  SCHEMA_INFERENCE,

  /**
   * If the data being serialized contains only a single column, persist it wrapped within an
   * envelope of some kind, e.g. an Avro record, or JSON object.
   *
   * <p>If not set, any single column will be persisted using the default mechanism of the format.
   *
   * @see SerdeFeature#UNWRAP_SINGLES
   */
  WRAP_SINGLES("UNWRAP_SINGLES"),

  /**
   * If the key/value being serialized contains only a single column, persist it as an anonymous
   * value.
   *
   * <p>If not set, any single column will be persisted using the default mechanism of the format.
   *
   * @see SerdeFeature#WRAP_SINGLES
   */
  UNWRAP_SINGLES("WRAP_SINGLES");

  private final ImmutableSet<String> unvalidated;
  private ImmutableSet<SerdeFeature> incompatibleWith;

  SerdeFeature(final String... incompatibleWith) {
    this.unvalidated = ImmutableSet.copyOf(incompatibleWith);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "incompatibleWith is ImmutableSet")
  @Override
  public Set<SerdeFeature> getIncompatibleWith() {
    return incompatibleWith;
  }

  @SuppressWarnings("UnstableApiUsage")
  private void validate() {
    this.incompatibleWith = unvalidated.stream()
        .map(SerdeFeature::valueOf)
        .collect(ImmutableSet.toImmutableSet());
  }

  static {
    Arrays.stream(SerdeFeature.values()).forEach(SerdeFeature::validate);
  }
}
