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
import java.util.UUID;

public final class QueryGuid {
  private final String namespace;
  private final String queryGuid;
  private final String structuralGuid;

  public QueryGuid(final String namespace, final String nonAnonQuery, final String anonQuery) {
    this.namespace = namespace;
    this.queryGuid = computeQueryGuid(nonAnonQuery, namespace);
    this.structuralGuid = computeQueryGuid(anonQuery, "");
  }

  private static String computeQueryGuid(final String query, final String namespace) {
    final String genericQuery = getGenericQueryForm(query);
    final String namespacePlusQuery = namespace + genericQuery;

    return UUID.nameUUIDFromBytes(namespacePlusQuery.getBytes(StandardCharsets.UTF_8)).toString();
  }

  private static String getGenericQueryForm(final String query) {
    return query.replaceAll("[\\n\\t ]", "");
  }

  public String getQueryGuid() {
    return this.queryGuid;
  }

  public String getStructuralGuid() {
    return this.structuralGuid;
  }

  public String getNamespace() {
    return namespace;
  }
}
