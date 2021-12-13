package io.confluent.ksql.engine;

import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;

import static io.confluent.ksql.util.QueryApplicationId.buildSharedRuntimeId;

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
    this(config, 8);
  }

  public RuntimeAssignor(final KsqlConfig config, final int runtimes) {
    runtimesToSources = new HashMap<>();
    idToRuntime= new HashMap<>();
    for(int i=0; i < runtimes; i++) {
      String runtime = buildSharedRuntimeId(config, true, i);
      runtimesToSources.put(runtime, new HashSet<>());
    }
  }

  private RuntimeAssignor(final RuntimeAssignor other) {
    this.runtimesToSources = new HashMap<>();
    this.idToRuntime = new HashMap<>();
    for (Map.Entry<String, Collection<SourceName>> runtime : other.runtimesToSources.entrySet()) {
      this.runtimesToSources.put(runtime.getKey(), new HashSet<>(runtime.getValue()));
    }
    this.idToRuntime.putAll(other.idToRuntime);
  }

  public RuntimeAssignor createSandbox() {
    return new RuntimeAssignor(this);
  }

  public String getRuntime(final QueryId queryId, final Collection<SourceName> sources, final KsqlConfig config) {
    if (idToRuntime.containsKey(queryId)) {
      return idToRuntime.get(queryId);
    }
    String runtime;
    final List<String> possibleRuntimes = runtimesToSources.entrySet()
        .stream()
        .filter(t -> t.getValue().stream().noneMatch(sources::contains))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    if (possibleRuntimes.isEmpty()) {
      runtime = makeNewRuntime(config);
      runtimesToSources.put(runtime, sources);
      idToRuntime.put(queryId, runtime);
      return runtime;
    }
    runtime = possibleRuntimes.get(Math.abs(queryId.hashCode()) % possibleRuntimes.size());
    runtimesToSources.get(runtime).addAll(sources);
    idToRuntime.put(queryId, runtime);
    return runtime;
  }

  public void dropQuery(final PersistentQueryMetadata queryMetadata) {
    if (queryMetadata instanceof BinPackedPersistentQueryMetadataImpl) {
      runtimesToSources.get(queryMetadata.getQueryApplicationId()).removeAll(queryMetadata.getSourceNames());
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
        runtimesToSources.put(queryMetadata.getQueryApplicationId(), queryMetadata.getSourceNames());
        idToRuntime.put(queryMetadata.getQueryId(), queryMetadata.getQueryApplicationId());
      }
    }
  }

  public void close() {
    runtimesToSources.clear();
    idToRuntime.clear();
  }

  public Map<String, Collection<SourceName>> getRuntimesToSources() {
    return runtimesToSources;
  }

  public Map<QueryId, String> getIdToRuntime() {
    return idToRuntime;
  }
}
