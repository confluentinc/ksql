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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the arguments to a query stream request
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryStreamArgs {

  public final String sql;
  public final Map<String, Object> properties;
  public final Map<String, Object> sessionVariables;
  public final Map<String, Object> requestProperties;

  public QueryStreamArgs(final @JsonProperty(value = "sql", required = true) String sql,
      final @JsonProperty(value = "properties")
          Map<String, Object> properties,
      final @JsonProperty(value = "sessionVariables")
          Map<String, Object> sessionVariables,
      final @JsonProperty(value = "requestProperties")
          Map<String, Object> requestProperties) {
    this.sql = Objects.requireNonNull(sql);
    this.properties = properties == null ? Collections.emptyMap() : properties;
    this.sessionVariables = sessionVariables == null
        ? Collections.emptyMap()
        : sessionVariables;
    this.requestProperties = requestProperties == null
        ? Collections.emptyMap()
        : requestProperties;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof QueryStreamArgs)) {
      return false;
    }

    final QueryStreamArgs that = (QueryStreamArgs) o;
    return Objects.equals(sql, that.sql)
        && Objects.equals(properties, that.properties)
        && Objects.equals(requestProperties, that.requestProperties)
        && Objects.equals(sessionVariables, that.sessionVariables);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, properties, requestProperties,
        sessionVariables);
  }

  @Override
  public String toString() {
    return "QueryStreamArgs{"
        + "sql='" + sql + '\''
        + ", properties=" + properties
        + ", sessionVariables=" + sessionVariables
        + ", requestProperties=" + requestProperties
        + '}';
  }
}
