/*
 * Copyright 2019 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class ServerClusterId {
  private static final String UNUSED_ID = "";
  private static final String KAFKA_CLUSTER = "kafka-cluster";
  private static final String KSQL_CLUSTER = "ksql-cluster";

  @JsonProperty("id")
  private final String id;

  @JsonProperty("scope")
  private final Map<String, String> scope;

  @JsonCreator
  ServerClusterId(
      @JsonProperty("id") final String id,
      @JsonProperty("scope") final Map<String, String> scope
  ) {
    this.id = id;
    this.scope = scope;
  }

  public static ServerClusterId of(final String kafkaClusterId, final String ksqlClusterId) {
    return new ServerClusterId(UNUSED_ID, ImmutableMap.of(
        KAFKA_CLUSTER, kafkaClusterId,
        KSQL_CLUSTER, ksqlClusterId
    ));
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ServerClusterId that = (ServerClusterId) o;
    return Objects.equals(id, id)
        && Objects.equals(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, scope);
  }
}
