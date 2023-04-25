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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("unused") // Invoked via reflection
@JsonIgnoreProperties(ignoreUnknown = true)
@Immutable
public final class ServerClusterId {

  // ID is unused for now, but it might be used later to include a URL that joins both, kafka and
  // ksql, clusters names into one single string. This one URL string will be easier to pass
  // through authorization commands to authorize access to this KSQL cluster.
  private static final String DEFAULT_ID = "";
  private static final String KAFKA_CLUSTER = "kafka-cluster";
  private static final String KSQL_CLUSTER = "ksql-cluster";

  private final String id;
  private final Scope scope;

  ServerClusterId(@JsonProperty("scope") final Scope scope) {
    this.id = DEFAULT_ID;
    this.scope = requireNonNull(scope, "scope");
  }

  public static ServerClusterId of(final String kafkaClusterId, final String ksqlClusterId) {
    return new ServerClusterId(
        new Scope(
            // 'path' is unused for now, but it might be used by Cloud environments that specify
            // which account organization this cluster belongs to.
            Collections.emptyList(),
            ImmutableMap.of(
                KAFKA_CLUSTER, kafkaClusterId,
                KSQL_CLUSTER, ksqlClusterId
            )
        ));
  }

  public String getId() {
    return id;
  }

  public Scope getScope() {
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
    return Objects.equals(id, that.id)
        && Objects.equals(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, scope);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @Immutable
  public static final class Scope {

    private final ImmutableList<String> path;
    private final ImmutableMap<String, String> clusters;

    public Scope(
        @JsonProperty(value = "path", required = true) final List<String> path,
        @JsonProperty(value = "clusters", required = true) final Map<String, String> clusters
    ) {
      this.path = ImmutableList.copyOf(requireNonNull(path, "path"));
      this.clusters = ImmutableMap.copyOf(requireNonNull(clusters, "clusters"));
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "path is ImmutableList")
    public List<String> getPath() {
      return path;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "clusters is ImmutableMap")
    public Map<String, String> getClusters() {
      return clusters;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Scope scope = (Scope) o;
      return Objects.equals(path, scope.path)
          && Objects.equals(clusters, scope.clusters);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, clusters);
    }
  }
}
