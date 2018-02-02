/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.ksql;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.physical.KafkaStreamsBuilder;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.serde.delimited.KsqlDelimitedTopicSerDe;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class QueryTranslationTest {

  private final MetaStore metaStore = new MetaStoreImpl();
  private final LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder(metaStore);
  private final KafkaStreamsBuilderStub kafkaStreamsBuilder = new KafkaStreamsBuilderStub();
  private final Map<String, Object> config = new HashMap<String, Object>() {{
    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    put("application.id", "KSQL-TEST");
    put("commit.interval.ms", 0);
    put("cache.max.bytes.buffering", 0);
    put("auto.offset.reset", "earliest");
  }};
  private final Properties streamsProperties = new Properties();
  private final ConsumerRecordFactory<String, String> recordFactory =
      new ConsumerRecordFactory<>(
          Serdes.String().serializer(),
          Serdes.String().serializer());

  private PhysicalPlanBuilder physicalPlanBuilder;

  @Before
  public void before() {
    streamsProperties.putAll(config);
    createPhysicalPlanBuilder();
    setupMetaStore();
  }

  private void setupMetaStore() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("ROWTIME", SchemaBuilder.INT64_SCHEMA)
        .field("ROWKEY", SchemaBuilder.STRING_SCHEMA)
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.BOOLEAN_SCHEMA);

    final String entityName = "TEST";
    final KsqlTopic
        ksqlTopic =
        new KsqlTopic(entityName, entityName.toLowerCase(), new KsqlDelimitedTopicSerDe());

    final KsqlStream stream = new KsqlStream("sqlexpression",
        entityName,
        schemaBuilder.build(),
        schemaBuilder.field("COL0"),
        null,
        ksqlTopic);

    metaStore.putTopic(ksqlTopic);
    metaStore.putSource(stream);

  }

  private void createPhysicalPlanBuilder() {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final FunctionRegistry functionRegistry = new FunctionRegistry();
    physicalPlanBuilder = new PhysicalPlanBuilder(streamsBuilder,
        new KsqlConfig(config),
        new FakeKafkaTopicClient(),
        new MetastoreUtil(),
        functionRegistry,
        Collections.emptyMap(),
        false,
        metaStore,
        new MockSchemaRegistryClient(),
        kafkaStreamsBuilder
    );
  }

  private TopologyTestDriver buildStreamsTopology(final String query) throws Exception {
    final PlanNode logical = logicalPlanBuilder.buildLogicalPlan(query);
    physicalPlanBuilder.buildPhysicalPlan(new Pair<>(query, logical));
    return new TopologyTestDriver(kafkaStreamsBuilder.topology, streamsProperties, 0);
  }

  // query
  // topic
  // inputs
  @Test
  public void should() throws Exception {
    final TopologyTestDriver testDriver
        = buildStreamsTopology("create stream s1 as SELECT col1 FROM test WHERE col0 > 100;");
    testDriver.pipeInput(recordFactory.create("test", "", "0,v,v2,2.7,false"));

    final ProducerRecord<String, String> result = testDriver.readOutput("s1",
        Serdes.String().deserializer(),
        Serdes.String().deserializer());
    assertThat(result, nullValue());

    testDriver.pipeInput(recordFactory.create("test", "", "100,v,v2,2.7,false"));
    assertThat(testDriver.readOutput("s1",
        Serdes.String().deserializer(),
        Serdes.String().deserializer()), nullValue());

    testDriver.pipeInput(recordFactory.create("test", "", "101,v,v2,2.7,false"));
    assertThat(testDriver.readOutput("s1",
        Serdes.String().deserializer(),
        Serdes.String().deserializer()), nullValue());
  }


  class KafkaStreamsBuilderStub implements KafkaStreamsBuilder {

    private Topology topology;

    @Override
    public KafkaStreams buildKafkaStreams(StreamsBuilder builder, StreamsConfig conf) {
      topology = builder.build();
      return new KafkaStreams(builder.build(), conf);
    }
  }


}
