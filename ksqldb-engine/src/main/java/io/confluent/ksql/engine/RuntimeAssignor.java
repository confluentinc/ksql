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

package io.confluent.ksql.engine;

import static io.confluent.ksql.util.QueryApplicationId.buildSharedRuntimeId;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RuntimeAssignor {
  private final Map<String, Collection<SourceName>> runtimesToSources;
  private final Map<QueryId, String> idToRuntime;

  public RuntimeAssignor(final KsqlConfig config) {
    runtimesToSources = new HashMap<>();
    idToRuntime = new HashMap<>();
    final int runtimes = config.getInt(KsqlConfig.KSQL_SHARED_RUNTIMES_COUNT);
    for (int i = 0; i < runtimes; i++) {
      final String runtime = buildSharedRuntimeId(config, true, i);
      runtimesToSources.put(runtime, new HashSet<>());
    }
  }

  private RuntimeAssignor(final RuntimeAssignor other) {
    this.runtimesToSources = new HashMap<>();
    this.idToRuntime = new HashMap<>(other.idToRuntime);
    for (Map.Entry<String, Collection<SourceName>> runtime
        : other.runtimesToSources.entrySet()) {
      this.runtimesToSources.put(runtime.getKey(), new HashSet<>(runtime.getValue()));
    }
  }

  public RuntimeAssignor createSandbox() {
    return new RuntimeAssignor(this);
  }

  public String getRuntimeAndMaybeAddRuntime(final QueryId queryId,
                                             final Collection<SourceName> sources,
                                             final KsqlConfig config) {
    if (idToRuntime.containsKey(queryId)) {
      return idToRuntime.get(queryId);
    }
    final List<String> possibleRuntimes = runtimesToSources.entrySet()
        .stream()
        .filter(t -> t.getValue().stream().noneMatch(sources::contains))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    final String runtime;
    if (possibleRuntimes.isEmpty()) {
      runtime = makeNewRuntime(config);
    } else {
      runtime = possibleRuntimes.get(Math.abs(queryId.hashCode() % possibleRuntimes.size()));
    }
    runtimesToSources.get(runtime).addAll(sources);
    idToRuntime.put(queryId, runtime);
    return runtime;
  }

  public void dropQuery(final PersistentQueryMetadata queryMetadata) {
    if (queryMetadata instanceof BinPackedPersistentQueryMetadataImpl) {
      runtimesToSources.get(queryMetadata.getQueryApplicationId())
          .removeAll(queryMetadata.getSourceNames());
      idToRuntime.remove(queryMetadata.getQueryId());
    }
  }


  private String makeNewRuntime(final KsqlConfig config) {
    final String runtime = buildSharedRuntimeId(config, true, runtimesToSources.size());
    runtimesToSources.put(runtime, new HashSet<>());
    return runtime;
  }

  public void rebuildAssignment(final Collection<PersistentQueryMetadata> queries) {
    for (PersistentQueryMetadata queryMetadata: queries) {
      if (queryMetadata instanceof BinPackedPersistentQueryMetadataImpl) {
        runtimesToSources.put(queryMetadata.getQueryApplicationId(),
            queryMetadata.getSourceNames());
        idToRuntime.put(queryMetadata.getQueryId(), queryMetadata.getQueryApplicationId());
      }
    }
  }

  public Map<String, Collection<SourceName>> getRuntimesToSources() {
    return ImmutableMap.copyOf(runtimesToSources);
  }

  public Map<QueryId, String> getIdToRuntime() {
    return ImmutableMap.copyOf(idToRuntime);
  }
}
