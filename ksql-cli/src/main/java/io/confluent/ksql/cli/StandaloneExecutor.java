/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.cli;


import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.tree.Statement;

public class StandaloneExecutor {

  private static final Logger log = LoggerFactory.getLogger(StandaloneExecutor.class);

  KsqlEngine ksqlEngine;

  public StandaloneExecutor(Map streamProperties) {
    KsqlConfig ksqlConfig = new KsqlConfig(streamProperties);
    ksqlEngine = new KsqlEngine(ksqlConfig, new KafkaTopicClientImpl(ksqlConfig, AdminClient.create(ksqlConfig.getKsqlConfigProps())));
  }

  public void executeStatements(String queries) throws Exception {
    MetaStore tempMetaStore = ksqlEngine.getMetaStore().clone();
    List<Pair<String, Statement>> queryList = ksqlEngine.parseQueries(queries,
                                                                      Collections.emptyMap(),
                                                                      tempMetaStore);
    List<QueryMetadata> queryMetadataList = ksqlEngine.planQueries(
        false, queryList, new HashMap<>(), tempMetaStore);
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
