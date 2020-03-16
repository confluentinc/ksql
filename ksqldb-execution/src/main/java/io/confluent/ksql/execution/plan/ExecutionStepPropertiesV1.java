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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.context.QueryContext;
import java.util.Objects;

@Immutable
public final class ExecutionStepPropertiesV1 implements ExecutionStepProperties {
  private final QueryContext queryContext;

  public ExecutionStepPropertiesV1(
      @JsonProperty(value = "queryContext", required = true) final QueryContext queryContext) {
    this.queryContext = Objects.requireNonNull(queryContext, "queryContext");
  }

  @JsonIgnore
  public String getId() {
    return queryContext.toString();
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExecutionStepPropertiesV1 that = (ExecutionStepPropertiesV1) o;
    return Objects.equals(queryContext, that.queryContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryContext);
  }

  @Override
  public String toString() {
    return "ExecutionStepProperties{"
        + "queryContext='" + queryContext.toString() + '\''
        + '}';
  }
}
