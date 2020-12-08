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

package io.confluent.ksql.rest.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A command which is executed locally and possibly requiring cleanup next time the server is
 * restarted.
 */
public class LocalCommand {

  private final String queryApplicationId;
  private final Type type;

  @JsonCreator
  public LocalCommand(
      @JsonProperty("type") final Type type,
      @JsonProperty("queryApplicationId") final String queryApplicationId
  ) {
    this.type = type;
    this.queryApplicationId = queryApplicationId;
  }

  public String getQueryApplicationId() {
    return queryApplicationId;
  }

  public Type getType() {
    return type;
  }

  public enum Type {
    TRANSIENT_QUERY
  }
}
