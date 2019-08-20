/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;

@Immutable
public class DefaultExecutionStepProperties implements ExecutionStepProperties {
  private final String id;
  private final LogicalSchema schema;

  public DefaultExecutionStepProperties(final String id, final LogicalSchema schema) {
    this.id = Objects.requireNonNull(id, "id");
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DefaultExecutionStepProperties that = (DefaultExecutionStepProperties) o;
    return Objects.equals(id, that.id)
        && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, schema);
  }

  @Override
  public String toString() {
    return "ExecutionStepProperties{"
        + "id='" + id + '\''
        + ", schema=" + schema
        + '}';
  }
}
