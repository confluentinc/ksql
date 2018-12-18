/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class QueryId {
  private final String id;

  @JsonCreator
  public QueryId(@JsonProperty("id") final String id) {
    Objects.requireNonNull(id, "id can't be null");
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public String toString() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryId)) {
      return false;
    }
    final QueryId queryId = (QueryId) o;
    return Objects.equals(id, queryId.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
