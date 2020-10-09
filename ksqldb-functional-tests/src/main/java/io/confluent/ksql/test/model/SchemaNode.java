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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Objects;
import java.util.Optional;

/**
 * Used to track the schema and serde features used for each topic in a plan.
 */
public class SchemaNode {

  private final String logicalSchema;
  private final Optional<KeyFormat> keyFormat;
  private final Optional<ValueFormat> valueFormat;

  @SuppressWarnings("unused") // Invoked by Jackson via reflection
  @JsonCreator
  public static SchemaNode create(
      @JsonProperty(value = "schema", required = true) final String logicalSchema,
      @JsonProperty("keyFormat") final Optional<KeyFormat> keyFormat,
      @JsonProperty("valueFormat") final Optional<ValueFormat> valueFormat
  ) {
    return new SchemaNode(logicalSchema, keyFormat, valueFormat);
  }

  public SchemaNode(
      final String logicalSchema,
      final Optional<KeyFormat> keyFormat,
      final Optional<ValueFormat> valueFormat
  ) {
    this.logicalSchema = requireNonNull(logicalSchema, "logicalSchema");
    this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = requireNonNull(valueFormat, "valueFormat");
  }

  public String getSchema() {
    return logicalSchema;
  }

  public Optional<KeyFormat> getKeyFormat() {
    return keyFormat;
  }

  public Optional<ValueFormat> getValueFormat() {
    return valueFormat;
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
        && keyFormat.equals(that.keyFormat)
        && valueFormat.equals(that.valueFormat);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalSchema, keyFormat, valueFormat);
  }

  @Override
  public String toString() {
    return "SchemaNode{"
        + "logicalSchema='" + logicalSchema + '\''
        + ", keyFormat=" + keyFormat
        + ", valueFormat=" + valueFormat
        + '}';
  }

  public static SchemaNode fromSchemaInfo(final QuerySchemas.SchemaInfo schemaInfo) {
    return new SchemaNode(
        schemaInfo.schema().toString(),
        schemaInfo.keyFormat(),
        schemaInfo.valueFormat()
    );
  }
}
