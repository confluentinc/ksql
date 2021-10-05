/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Queries extends KsqlEntity {
  private final List<RunningQuery> queries;

  @JsonCreator
  public Queries(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("queries") final Collection<RunningQuery> queries
  ) {
    super(statementText);
    this.queries = ImmutableList.copyOf(queries);
  }

  public List<RunningQuery> getQueries() {
    return new ArrayList<>(queries);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Queries)) {
      return false;
    }
    final Queries that = (Queries) o;
    return Objects.equals(getQueries(), that.getQueries());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getQueries());
  }
}
