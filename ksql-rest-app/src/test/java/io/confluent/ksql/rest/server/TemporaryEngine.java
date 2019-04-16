/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.test.commons.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import io.confluent.rest.RestConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.rules.ExternalResource;

public class TemporaryEngine extends ExternalResource {

  public static final Schema SCHEMA =
      SchemaBuilder.struct().field("val", Schema.OPTIONAL_STRING_SCHEMA).build();

  private MutableMetaStore metaStore;

  private KsqlConfig ksqlConfig;
  private KsqlEngine engine;
  private ServiceContext serviceContext;

  @Override
  protected void before() throws Throwable {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    serviceContext = (TestServiceContext.create());
    engine = (KsqlEngineTestUtil.createKsqlEngine(getServiceContext(), metaStore));

    final Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    configMap.put("ksql.command.topic.suffix", "commands");
    configMap.put(RestConfig.LISTENERS_CONFIG, "http://localhost:8088");
    ksqlConfig = (new KsqlConfig(configMap));
  }

  @Override
  protected void after() {
    engine.close();
    serviceContext.close();
  }

  @SuppressWarnings("unchecked")
  public <T extends StructuredDataSource<?>> T givenSource(
      final DataSource.DataSourceType type,
      final String name) {
    final KsqlTopic topic = givenKsqlTopic(name);
    givenKafkaTopic(name);

    final StructuredDataSource<?> source;
    switch (type) {
      case KSTREAM:
        source =
            new KsqlStream<>(
                "statement", name, SCHEMA, Optional.of(SCHEMA.field("val")),
                new MetadataTimestampExtractionPolicy(), topic, Serdes::String);
        break;
      case KTABLE:
        source =
            new KsqlTable<>(
                "statement", name, SCHEMA, Optional.of(SCHEMA.field("val")),
                new MetadataTimestampExtractionPolicy(), topic, Serdes::String);
        break;
      case KTOPIC:
      default:
        throw new IllegalArgumentException(type.toString());
    }
    metaStore.putSource(source);

    return (T) source;
  }

  public KsqlTopic givenKsqlTopic(String name) {
    final KsqlTopic topic = new KsqlTopic(name, name, new KsqlJsonTopicSerDe(), false);
    givenKafkaTopic(name);
    metaStore.putTopic(topic);
    return topic;
  }

  public void givenKafkaTopic(final String name) {
    ((FakeKafkaTopicClient) getServiceContext().getTopicClient())
        .preconditionTopicExists(name, 1, (short) 1, Collections.emptyMap());
  }

  public ConfiguredStatement<?> configure(final String sql) {
    return ConfiguredStatement.of(
        getEngine().prepare(new DefaultKsqlParser().parse(sql).get(0)),
        new HashMap<>(),
        ksqlConfig);
  }

  @SuppressWarnings("SameParameterValue")
  public PersistentQueryMetadata givenPersistentQuery(final String id) {
    final PersistentQueryMetadata metadata = mock(PersistentQueryMetadata.class);
    when(metadata.getQueryId()).thenReturn(new QueryId(id));
    when(metadata.getSinkNames()).thenReturn(ImmutableSet.of(id));
    when(metadata.getResultSchema()).thenReturn(SCHEMA);

    return metadata;
  }

  public KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public KsqlEngine getEngine() {
    return engine;
  }

  public ServiceContext getServiceContext() {
    return serviceContext;
  }
}
