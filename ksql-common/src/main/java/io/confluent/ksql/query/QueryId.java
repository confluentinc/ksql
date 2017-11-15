/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.query;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;

import java.util.Objects;

@JsonSubTypes({})
public class QueryId {
  private final long id;
  private final int childId;

  public QueryId(long id) {
    this(id, 0);
  }

  @JsonCreator
  public QueryId(
      @JsonProperty("id")long id,
      @JsonProperty("childId") int childId) {
    this.id = id;
    this.childId  = childId;
  }

  public QueryId nextChildId() {
    return new QueryId(id, childId + 1);
  }

  public String toString() {
    return String.format("%d-%d", id, childId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QueryId queryId = (QueryId) o;
    return id == queryId.id &&
        childId == queryId.childId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, childId);
  }

  public static QueryId fromString(String queryId) {
    final String[] parts = queryId.split("-");
    return new QueryId(Long.parseLong(parts[0]), Integer.parseInt(parts[1]));
  }
}
