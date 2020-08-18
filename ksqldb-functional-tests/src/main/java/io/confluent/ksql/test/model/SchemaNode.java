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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import java.util.Objects;
import java.util.Set;

public class SchemaNode {

  private String logicalSchema;
  private Set<SerdeOption> serdeOptions;

  public SchemaNode(
      @JsonProperty("schema") final String logicalSchema,
      @JsonProperty("serdeOptions") final Set<SerdeOption> serdeOptions
  ) {
    this.logicalSchema = Objects.requireNonNull(logicalSchema, "logicalSchema");
    this.serdeOptions = Objects.requireNonNull(serdeOptions, "serdeOptions");
  }

  @JsonProperty("schema")
  public String getLogicalSchema() {
    return logicalSchema;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
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
        && serdeOptions.equals(that.serdeOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logicalSchema, serdeOptions);
  }

  @Override
  public String toString() {
    return "SchemaNode{"
            + "logicalSchema='" + logicalSchema + '\''
            + ", serdeOptions=" + serdeOptions
            + '}';
  }

  public static SchemaNode fromPhysicalSchema(final PhysicalSchema physicalSchema) {
    return new SchemaNode(
        physicalSchema.logicalSchema().toString(),
        physicalSchema.serdeOptions()
    );
  }
}
