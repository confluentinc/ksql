/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.UUID;

public final class QueryGuid {
  private final String clusterNamespace;
  private final String queryUuid;
  private final String anonQueryUuid;
  private final LocalDateTime timeOfCreation;

  public QueryGuid(final String namespace, final String nonAnonQuery, final String anonQuery) {
    this.clusterNamespace = namespace;
    this.queryUuid = computeQueryId(nonAnonQuery, clusterNamespace);
    this.anonQueryUuid = computeQueryId(anonQuery, "");
    this.timeOfCreation = LocalDateTime.now();
  }

  public String getClusterNamespace() {
    return this.clusterNamespace;
  }

  public String getQueryUuid() {
    return this.queryUuid;
  }

  public String getAnonQueryUuid() {
    return this.anonQueryUuid;
  }

  public LocalDateTime getTimeOfCreation() {
    return this.timeOfCreation;
  }

  private static String computeQueryId(final String query, final String namespace) {
    final String genericQuery = getGenericQueryForm(query);
    final String namespacePlusQuery = namespace + genericQuery;

    return UUID.nameUUIDFromBytes(namespacePlusQuery.getBytes(StandardCharsets.UTF_8)).toString();
  }

  private static String getGenericQueryForm(final String query) {
    return query.replaceAll("[\\n\\t ]", "");
  }
}
