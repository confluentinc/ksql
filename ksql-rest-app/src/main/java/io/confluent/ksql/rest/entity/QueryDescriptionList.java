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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryDescriptionList extends KsqlEntity {
  private final List<QueryDescription> queryDescriptions;

  @JsonCreator
  public QueryDescriptionList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("queryDescriptions") final List<QueryDescription> queryDescriptions
  ) {
    super(statementText);
    this.queryDescriptions = queryDescriptions;
  }

  public List<QueryDescription> getQueryDescriptions() {
    return queryDescriptions;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryDescriptionList)) {
      return false;
    }
    final QueryDescriptionList that = (QueryDescriptionList) o;
    return Objects.equals(queryDescriptions, that.queryDescriptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryDescriptions);
  }
}
