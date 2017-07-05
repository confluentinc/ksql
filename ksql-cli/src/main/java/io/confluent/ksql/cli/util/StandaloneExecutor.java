/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.util;


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
  KsqlEngine ksqlEngine;
  public StandaloneExecutor(Map StreamProperties) {
    MetaStore metaStore = new MetaStoreImpl();
    ksqlEngine = new KsqlEngine(metaStore, StreamProperties);
  }

  public void executeStatements(String queries) throws Exception {
    List<Pair<String, Query>> queryList = ksqlEngine.buildQueryAstList(queries,
                                                                       Collections.emptyMap());
    List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueriesFromAsts(false, queryList,
                                                                        Collections.emptyMap());
    for (QueryMetadata queryMetadata: queryMetadataList) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueryMetadata.getKafkaStreams().start();
      }
    }
  }

}
