/**
 * Copyright 2018 Confluent Inc.
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

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import java.util.List;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class QueryEngineTest {

  private final KafkaTopicClient topicClient = new FakeKafkaTopicClient();
  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final KsqlConfig ksqlConfig
      = new KsqlConfig(ImmutableMap.of(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
  private final KsqlEngine ksqlEngine = new KsqlEngine(
      topicClient,
      schemaRegistryClient,
      metaStore,
      ksqlConfig);


  @Test
  public void shouldThrowExpectedExceptionForDuplicateTable() {
    final QueryEngine queryEngine = new QueryEngine(ksqlEngine,
        new CommandFactories(topicClient, schemaRegistryClient, true));
    try {
      final List<PreparedStatement> statementList = ksqlEngine.parseQueries(
          "CREATE TABLE FOO AS SELECT * FROM TEST2; CREATE TABLE BAR WITH (KAFKA_TOPIC='FOO') AS SELECT * FROM TEST2;", metaStore.clone());
      queryEngine.buildLogicalPlans(metaStore, statementList, ksqlConfig);
      Assert.fail();
    } catch (final KsqlException e) {
      assertThat(e.getMessage(), equalTo("Cannot create the stream/table. The output topic FOO is already used by FOO"));
    }

  }

  @Test
  public void shouldThrowExpectedExceptionForDuplicateStream() {
    final QueryEngine queryEngine = new QueryEngine(ksqlEngine,
        new CommandFactories(topicClient, schemaRegistryClient, true));
    try {
      final List<PreparedStatement> statementList = ksqlEngine.parseQueries(
          "CREATE STREAM FOO AS SELECT * FROM ORDERS; CREATE STREAM BAR WITH (KAFKA_TOPIC='FOO') AS SELECT * FROM ORDERS;", metaStore.clone());
      queryEngine.buildLogicalPlans(metaStore, statementList, ksqlConfig);
      Assert.fail();
    } catch (final KsqlException e) {
      assertThat(e.getMessage(), equalTo("Cannot create the stream/table. The output topic FOO is already used by FOO"));
    }

  }

}
