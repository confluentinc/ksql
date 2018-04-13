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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

public class KsqlContext {

  private static final Logger log = LoggerFactory.getLogger(KsqlContext.class);
  private final KsqlEngine ksqlEngine;
  private static final String APPLICATION_ID_OPTION_DEFAULT = "ksql_standalone_cli";
  private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT = "localhost:9092";
  private final AdminClient adminClient;
  private final KafkaTopicClient topicClient;

  public static KsqlContext create(KsqlConfig ksqlConfig) {
    return create(ksqlConfig, null);
  }

  public static KsqlContext create(
      KsqlConfig ksqlConfig,
      SchemaRegistryClient schemaRegistryClient
  ) {
    if (ksqlConfig == null) {
      ksqlConfig = new KsqlConfig(Collections.emptyMap());
    }
    Map<String, Object> streamsProperties = ksqlConfig.getKsqlStreamConfigProps();
    if (!streamsProperties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
      streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_OPTION_DEFAULT);
    }
    if (!streamsProperties.containsKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      streamsProperties.put(
          StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
          KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT
      );
    }
    AdminClient adminClient = AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps());
    KafkaTopicClient topicClient = new KafkaTopicClientImpl(adminClient);
    if (schemaRegistryClient == null) {
      return new KsqlContext(adminClient, topicClient, new KsqlEngine(ksqlConfig, topicClient));
    } else {
      return new KsqlContext(
          adminClient,
          topicClient,
          new KsqlEngine(
              ksqlConfig,
              topicClient,
              schemaRegistryClient,
              new MetaStoreImpl()
          )
      );
    }

  }


  /**
   * Create a KSQL context object with the given properties.
   * A KSQL context has it's own metastore valid during the life of the object.
   */
  KsqlContext(
      final AdminClient adminClient,
      final KafkaTopicClient topicClient,
      final KsqlEngine ksqlEngine
  ) {
    this.adminClient = adminClient;
    this.topicClient = topicClient;
    this.ksqlEngine = ksqlEngine;
  }

  public MetaStore getMetaStore() {
    return ksqlEngine.getMetaStore();
  }

  /**
   * Execute the ksql statement in this context.
   */
  public void sql(String sql) throws Exception {
    List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(sql, Collections
        .emptyMap());

    for (QueryMetadata queryMetadata : queryMetadataList) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueryMetadata.getKafkaStreams().start();
        ksqlEngine.getPersistentQueries()
            .put(persistentQueryMetadata.getQueryId(), persistentQueryMetadata);
      } else {
        System.err.println("Ignoring statemenst: " + sql);
        System.err.println("Only CREATE statements can run in KSQL embedded mode.");
        log.warn("Ignoring statemenst: {}", sql);
        log.warn("Only CREATE statements can run in KSQL embedded mode.");
      }
    }
  }

  public Set<QueryMetadata> getRunningQueries() {
    return ksqlEngine.getLivePersistentQueries();
  }

  public void close() throws IOException {
    ksqlEngine.close();
    topicClient.close();
    adminClient.close();
  }

}
