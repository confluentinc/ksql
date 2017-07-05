/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql;

import org.apache.kafka.streams.StreamsConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

public class KsqlContext {

  final KsqlEngine ksqlEngine;
  private static final String APPLICATION_ID_OPTION_DEFAULT = "ksql_standalone_cli";
  private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT = "localhost:9092";


  public KsqlContext() {
    Map<String, Object> streamsProperties = new HashMap<>();
    streamsProperties
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT);
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_OPTION_DEFAULT);
    ksqlEngine = new KsqlEngine(new MetaStoreImpl(), streamsProperties);
  }

  public KsqlContext(Map<String, Object> streamsProperties) {
    if (!streamsProperties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
      streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_OPTION_DEFAULT);
    }
    if (!streamsProperties.containsKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      streamsProperties
          .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT);
    }
    ksqlEngine = new KsqlEngine(new MetaStoreImpl(), streamsProperties);
  }

  public void sql(String sql) throws Exception {
    List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
        false, sql, Collections.emptyMap());
    for (QueryMetadata queryMetadata: queryMetadataList) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueryMetadata.getKafkaStreams().start();
        ksqlEngine.getPersistentQueries()
            .put(persistentQueryMetadata.getId(), persistentQueryMetadata);
      }
    }
  }

  public void terminateQuery(long queryId) {
    if (!ksqlEngine.getPersistentQueries().containsKey(queryId)) {
      throw new KsqlException(String.format("Invalid query id. Query id, %d, does not exist.",
                                            queryId));
    }
    PersistentQueryMetadata persistentQueryMetadata = ksqlEngine
        .getPersistentQueries().get(queryId);
    persistentQueryMetadata.getKafkaStreams().close();
    ksqlEngine.getPersistentQueries().remove(queryId);
  }

  public Map<Long, PersistentQueryMetadata> getRunningQueries() {
    return ksqlEngine.getPersistentQueries();
  }
}
