/*
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

import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

public class KsqlContext {

  private static final Logger log = LoggerFactory.getLogger(KsqlContext.class);
  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT = "localhost:9092";

  public static KsqlContext create(KsqlConfig ksqlConfig) {
    return create(ksqlConfig, null);
  }

  public static KsqlContext create(
      KsqlConfig ksqlConfig,
      SchemaRegistryClient schemaRegistryClient
  ) {
    return create(ksqlConfig, schemaRegistryClient, new DefaultKafkaClientSupplier());
  }

  public static KsqlContext create(
      KsqlConfig ksqlConfig,
      SchemaRegistryClient schemaRegistryClient,
      KafkaClientSupplier clientSupplier
  ) {
    if (ksqlConfig == null) {
      ksqlConfig = new KsqlConfig(Collections.emptyMap());
    }
    if (!ksqlConfig.getKsqlStreamConfigProps().containsKey(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      ksqlConfig = ksqlConfig.cloneWithPropertyOverwrite(
          Collections.singletonMap(
              StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT));
    }

    final KsqlEngine engine = new KsqlEngine(
        new KafkaTopicClientImpl(ksqlConfig.getKsqlAdminClientConfigProps()),
        schemaRegistryClient == null
            ? new KsqlSchemaRegistryClientFactory(ksqlConfig).create() : schemaRegistryClient,
        clientSupplier
    );

    return new KsqlContext(ksqlConfig, engine);
  }

  /**
   * Create a KSQL context object with the given properties.
   * A KSQL context has it's own metastore valid during the life of the object.
   */
  KsqlContext(final KsqlConfig ksqlConfig, final KsqlEngine ksqlEngine) {
    this.ksqlConfig = ksqlConfig;
    this.ksqlEngine = ksqlEngine;
  }

  public MetaStore getMetaStore() {
    return ksqlEngine.getMetaStore();
  }

  /**
   * Execute the ksql statement in this context.
   */
  public void sql(String sql) {
    sql(sql, Collections.emptyMap());
  }

  public void sql(String sql, Map<String, Object> overriddenProperties) {
    List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
        sql, ksqlConfig, overriddenProperties);

    for (QueryMetadata queryMetadata : queryMetadataList) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueryMetadata.getKafkaStreams().start();
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

  public void close() {
    ksqlEngine.close();
  }
}
