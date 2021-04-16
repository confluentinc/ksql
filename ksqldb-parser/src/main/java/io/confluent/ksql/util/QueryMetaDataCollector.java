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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.joda.time.DateTime;

public final class QueryMetaDataCollector {

  private QueryMetaDataCollector() {}

  public static ImmutableMap<String, String> buildMetaData(
      final KsqlConfig config,
      final String nonAnonQuery,
      final String anonQuery) {

    // get metadata from control plane
    final Map<String, String> metaData = new HashMap<>();
    final String clusterNamespace = config
        .getString(KsqlConfig.KSQL_CCLOUD_QUERYANONYMIZER_CLUSTER_NAMESPACE);

    metaData.put("cluster_namespace", clusterNamespace);
    metaData.put("id", getQueryId(nonAnonQuery, clusterNamespace));
    metaData.put("structurally_similar_id", getStructurallySimilarId(anonQuery, clusterNamespace));
    metaData.put("time_of_creation", DateTime.now().toString());

    return ImmutableMap.copyOf(metaData);
  }

  private static String getQueryId(final String query, final String namespace) {
    final String genericQuery = getGenericQueryForm(query);
    final String namespacePlusQuery = namespace + genericQuery;

    return UUID.nameUUIDFromBytes(namespacePlusQuery.getBytes()).toString();
  }

  private static String getStructurallySimilarId(final String query, final String namespace) {
    final String namespacePlusQuery = namespace + query;

    return UUID.nameUUIDFromBytes(namespacePlusQuery.getBytes()).toString();
  }

  private static String getGenericQueryForm(final String query) {
    return query.replaceAll("[\\n\\t ]", "");
  }
}
