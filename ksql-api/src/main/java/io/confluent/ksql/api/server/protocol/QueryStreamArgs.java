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

package io.confluent.ksql.api.server.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.json.JsonObject;
import java.util.Map;

/**
 * Represents the arguments to a query stream request
 */
public class QueryStreamArgs {

  public final String sql;
  public final Boolean push;
  public final JsonObject properties;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public QueryStreamArgs(final @JsonProperty(value = "sql", required = true) String sql,
      final @JsonProperty(value = "push", required = true) Boolean push,
      final @JsonProperty(value = "properties")
          Map<String, Object> properties) {
    this.sql = sql;
    this.push = push;
    this.properties = properties == null ? new JsonObject() : new JsonObject(properties);
  }

  @Override
  public String toString() {
    return "QueryStreamArgs{"
        + "sql='" + sql + '\''
        + ", push=" + push
        + ", properties=" + properties
        + '}';
  }
}
