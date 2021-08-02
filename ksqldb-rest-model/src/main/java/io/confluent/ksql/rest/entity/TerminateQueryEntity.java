/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TerminateQueryEntity extends KsqlEntity {

  private final String queryId;
  private final boolean wasTerminated;

  @JsonCreator
  public TerminateQueryEntity(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("queryId") final String queryId,
      @JsonProperty("wasTerminated") final boolean wasTerminated
  ) {
    super(statementText);
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.wasTerminated =
        wasTerminated;
  }

  public String getQueryId() {
    return queryId;
  }

  public boolean getWasTerminated() {
    return wasTerminated;
  }

  @Override
  public String toString() {
    return "TerminateQueryEntity{"
        + "queryId='" + queryId + '\''
        + "wasTerminatedLocally='" + wasTerminated + '\''
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TerminateQueryEntity)) {
      return false;
    }
    final TerminateQueryEntity that = (TerminateQueryEntity) o;
    return Objects.equals(getQueryId(), that.getQueryId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getQueryId());
  }
}
