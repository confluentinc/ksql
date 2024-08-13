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
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeAssignor {
  private final Map<String, Collection<String>> runtimesToSourceTopics;
  private final Map<QueryId, String> idToRuntime;
  private final Logger log = LoggerFactory.getLogger(RuntimeAssignor.class);
  private final int numDefaultRuntimes;

  public RuntimeAssignor(final KsqlConfig config) {
    runtimesToSourceTopics = new HashMap<>();
    idToRuntime = new HashMap<>();
    numDefaultRuntimes = config.getInt(KsqlConfig.KSQL_SHARED_RUNTIMES_COUNT);
    for (int i = 0; i < numDefaultRuntimes; i++) {
      final String runtime = buildSharedRuntimeId(config, true, i);
      runtimesToSourceTopics.put(runtime, new HashSet<>());
    }
  }

  private RuntimeAssignor(final RuntimeAssignor other) {
    numDefaultRuntimes = other.numDefaultRuntimes;
    this.runtimesToSourceTopics = new HashMap<>();
    this.idToRuntime = new HashMap<>(other.idToRuntime);
    for (Map.Entry<String, Collection<String>> runtime
        : other.runtimesToSourceTopics.entrySet()) {
      this.runtimesToSourceTopics.put(runtime.getKey(), new HashSet<>(runtime.getValue()));
    }
  }

  public RuntimeAssignor createSandbox() {
    return new RuntimeAssignor(this);
  }

  public String getRuntimeAndMaybeAddRuntime(final QueryId queryId,
                                             final Collection<String> sourceTopics,
                                             final KsqlConfig config) {
    if (idToRuntime.containsKey(queryId)) {
      return idToRuntime.get(queryId);
    }
    final List<String> possibleRuntimes = runtimesToSourceTopics.entrySet()
        .stream()
        .filter(t -> t.getValue().stream().noneMatch(sourceTopics::contains))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    final String runtime;
    if (possibleRuntimes.isEmpty()) {
      runtime = makeNewRuntime(config);
    } else {
      runtime = possibleRuntimes.get(Math.abs(queryId.hashCode() % possibleRuntimes.size()));
    }
    runtimesToSourceTopics.get(runtime).addAll(sourceTopics);
    idToRuntime.put(queryId, runtime);
    log.info("Assigning query {} to runtime {}", queryId, runtime);
    return runtime;
  }

  public void dropQuery(final PersistentQueryMetadata queryMetadata) {
    if (queryMetadata instanceof BinPackedPersistentQueryMetadataImpl) {
      if (idToRuntime.containsKey(queryMetadata.getQueryId())) {
        log.info("Unassigning query {} from runtime {}",
            queryMetadata.getQueryId(),
            idToRuntime.get(queryMetadata.getQueryId()));
      } else {
        log.warn("Dropping an unassigned query {}, this should"
            + " only possible with Gen 1 queries", queryMetadata);
      }
      runtimesToSourceTopics.get(queryMetadata.getQueryApplicationId())
          .removeAll(queryMetadata.getSourceTopicNames());
      idToRuntime.remove(queryMetadata.getQueryId());
      if (runtimesToSourceTopics.get(queryMetadata.getQueryApplicationId()).isEmpty()
          && runtimesToSourceTopics.size() > numDefaultRuntimes) {
        runtimesToSourceTopics.remove(queryMetadata.getQueryApplicationId());
        log.info("Removing runtime {} form selection of possible runtimes",
            queryMetadata.getQueryApplicationId());
      }
    }
  }


  private String makeNewRuntime(final KsqlConfig config) {
    final String runtime = buildSharedRuntimeId(config, true, runtimesToSourceTopics.size());
    runtimesToSourceTopics.put(runtime, new HashSet<>());
    return runtime;
  }

  public void rebuildAssignment(final Collection<PersistentQueryMetadata> queries) {
    final Set<QueryId> gen1Queries = new HashSet<>();
    for (PersistentQueryMetadata queryMetadata: queries) {
      if (queryMetadata instanceof BinPackedPersistentQueryMetadataImpl) {
        if (!runtimesToSourceTopics.containsKey(queryMetadata.getQueryApplicationId())) {
          runtimesToSourceTopics.put(queryMetadata.getQueryApplicationId(), new HashSet<>());
        }
        runtimesToSourceTopics.get(queryMetadata.getQueryApplicationId())
            .addAll(queryMetadata.getSourceTopicNames());
        idToRuntime.put(queryMetadata.getQueryId(), queryMetadata.getQueryApplicationId());
      } else {
        gen1Queries.add(queryMetadata.getQueryId());
      }
    }
    if (!idToRuntime.isEmpty()) {
      log.info("The current assignment of queries to Gen 2 runtimes is: {}",
          idToRuntime.entrySet()
              .stream()
              .map(e -> e.getKey() + "->" + e.getValue())
              .collect(Collectors.joining(", ")));
    } else {
      if (gen1Queries.size() == queries.size()) {
        log.info("There are no queries assigned to Gen 2 runtimes yet.");
      } else {
        log.error("Gen 2 queries are not getting assigned correctly, this should not be possible");
      }
    }
    if (!gen1Queries.isEmpty()) {
      log.info("Currently there are {} queries running on the Gen 1 runtime which are: {}",
          gen1Queries.size(),
          gen1Queries);
    }
  }

  public Map<String, Collection<String>> getRuntimesToSourceTopics() {
    return ImmutableMap.copyOf(runtimesToSourceTopics);
  }

  public Map<QueryId, String> getIdToRuntime() {
    return ImmutableMap.copyOf(idToRuntime);
  }
}
