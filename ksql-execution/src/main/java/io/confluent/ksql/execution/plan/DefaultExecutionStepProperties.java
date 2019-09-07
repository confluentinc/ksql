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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;

@Immutable
public class DefaultExecutionStepProperties implements ExecutionStepProperties {
  private static final String CONTEXT = "context";
  private static final String SCHEMA = "schema";

  @JsonProperty(CONTEXT)
  private final QueryContext queryContext;
  @JsonProperty(SCHEMA)
  private final LogicalSchema schema;

  @JsonCreator
  public DefaultExecutionStepProperties(
      @JsonProperty(SCHEMA) final LogicalSchema schema,
      @JsonProperty(CONTEXT) final QueryContext queryContext) {
    this.queryContext = Objects.requireNonNull(queryContext, "queryContext");
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public String getId() {
    return queryContext.toString();
  }

  @Override
  public QueryContext getQueryContext() {
    return queryContext;
  }

  @Override
  public ExecutionStepProperties withQueryContext(final QueryContext queryContext) {
    return new DefaultExecutionStepProperties(schema, queryContext);
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
    return Objects.equals(queryContext, that.queryContext)
        && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryContext, schema);
  }

  @Override
  public String toString() {
    return "ExecutionStepProperties{"
        + "queryContext='" + queryContext.toString() + '\''
        + ", schema=" + schema
        + '}';
  }
}
