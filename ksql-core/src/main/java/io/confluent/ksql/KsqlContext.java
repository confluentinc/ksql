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

package io.confluent.ksql;

import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

public class KsqlContext {

  private static final Logger log = LoggerFactory.getLogger(KsqlContext.class);
  final KsqlEngine ksqlEngine;
  private static final String APPLICATION_ID_OPTION_DEFAULT = "ksql_standalone_cli";
  private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT = "localhost:9092";

  public KsqlContext() {
    this(null);
  }

  /**
   * Create a KSQL context object with the given properties.
   * A KSQL context has it's own metastore valid during the life of the object.
   *
   * @param streamsProperties
   */
  public KsqlContext(Map<String, Object> streamsProperties) {
    if (streamsProperties == null) {
      streamsProperties = new HashMap<>();
    }
    if (!streamsProperties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
      streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_OPTION_DEFAULT);
    }
    if (!streamsProperties.containsKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT);
    }
    KsqlConfig ksqlConfig = new KsqlConfig(streamsProperties);
    ksqlEngine = new KsqlEngine(ksqlConfig, new KafkaTopicClientImpl(ksqlConfig));
  }

  /**
   * Execute the ksql statement in this context.
   *
   * @param sql
   * @throws Exception
   */
  public void sql(String sql) throws Exception {
    List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
        false, sql, Collections.emptyMap());
    for (QueryMetadata queryMetadata: queryMetadataList) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueryMetadata.start();
        ksqlEngine.getPersistentQueries()
            .put(persistentQueryMetadata.getId(), persistentQueryMetadata);
      } else {
        System.err.println("Ignoring statemenst: " + sql);
        System.err.println("Only CREATE statements can run in KSQL embedded mode.");
        log.warn("Ignoring statemenst: {}", sql);
        log.warn("Only CREATE statements can run in KSQL embedded mode.");
      }
    }
  }

  /**
   * Terminate a query with the given id.
   *
   * @param queryId
   */
  public void terminateQuery(long queryId) {
    if (!ksqlEngine.getPersistentQueries().containsKey(queryId)) {
      throw new KsqlException(String.format("Invalid query id. Query id, %d, does not exist.",
                                            queryId));
    }
    PersistentQueryMetadata persistentQueryMetadata = ksqlEngine
        .getPersistentQueries().get(queryId);
    persistentQueryMetadata.closeAndCleanUp();
    ksqlEngine.getPersistentQueries().remove(queryId);
  }

  public Map<Long, PersistentQueryMetadata> getRunningQueries() {
    return ksqlEngine.getPersistentQueries();
  }
}
