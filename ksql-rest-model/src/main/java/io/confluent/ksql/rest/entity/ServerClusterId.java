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
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@Immutable
public final class ServerClusterId {
  private static final String KAFKA_CLUSTER = "kafka-cluster";
  private static final String KSQL_CLUSTER = "ksql-cluster";

  private static final String id = "";
  private final Map<String, String> scope;

  @JsonCreator
  ServerClusterId(
      @JsonProperty("scope") final Map<String, String> scope
  ) {
    this.scope = ImmutableMap.copyOf(Objects.requireNonNull(scope, "scope"));
  }

  public static ServerClusterId of(final String kafkaClusterId, final String ksqlClusterId) {
    return new ServerClusterId(ImmutableMap.of(
        KAFKA_CLUSTER, kafkaClusterId,
        KSQL_CLUSTER, ksqlClusterId
    ));
  }

  public String getId() {
    return id;
  }

  public Map<String, String> getScope() {
    return scope;
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
