/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

public class StandaloneExecutor {

  private static final Logger log = LoggerFactory.getLogger(StandaloneExecutor.class);

  KsqlEngine ksqlEngine;

  public StandaloneExecutor(Map streamProperties) {
    MetaStore metaStore = new MetaStoreImpl();
    ksqlEngine = new KsqlEngine(metaStore, streamProperties);
  }

  public void executeStatements(String queries) throws Exception {
    List<Pair<String, Query>> queryList = ksqlEngine.parseQueries(queries,
                                                                       Collections.emptyMap());
    List<QueryMetadata> queryMetadataList = ksqlEngine.planQueries(
        false, queryList, Collections.emptyMap());
    for (QueryMetadata queryMetadata: queryMetadataList) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueryMetadata.getKafkaStreams().start();
      } else {
        System.err.println("Ignoring statemenst: " + queryMetadata.getStatementString());
        System.err.println("Only CREATE statements can run in KSQL embedded mode.");
        log.warn("Ignoring statemenst: {}", queryMetadata.getStatementString());
        log.warn("Only CREATE statements can run in KSQL embedded mode.");
      }
    }
  }

}
