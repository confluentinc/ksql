/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.physical;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.QueryMetadata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class PhysicalPlanBuilderTest {
  private final String simpleSelectFilter = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
  private PhysicalPlanBuilder physicalPlanBuilder;
  private MetaStore metaStore = MetaStoreFixture.getNewMetaStore();
  private LogicalPlanBuilder planBuilder;
  private KsqlConfig ksqlConfig;

  // Test implementation of KafkaStreamsBuilder that tracks calls and returned values
  class TestKafkaStreamsBuilder implements KafkaStreamsBuilder {
    class Call {
      public StreamsBuilder builder;
      public StreamsConfig config;
      KafkaStreams kafkaStreams;

      private Call(StreamsBuilder builder, StreamsConfig config, KafkaStreams kafkaStreams) {
        this.builder = builder;
        this.config = config;
        this.kafkaStreams = kafkaStreams;
      }
    }

    private List<Call> calls = new LinkedList<>();

    @Override
    public KafkaStreams buildKafkaStreams(StreamsBuilder builder, StreamsConfig conf) {
      KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), conf);
      calls.add(new Call(builder, conf, kafkaStreams));
      return kafkaStreams;
    }

    List<Call> getCalls() {
      return calls;
    }
  }

  private TestKafkaStreamsBuilder testKafkaStreamsBuilder;

  @Before
  public void before() {
    testKafkaStreamsBuilder = new TestKafkaStreamsBuilder();
    physicalPlanBuilder = buildPhysicalPlanBuilder(Collections.emptyMap());
    planBuilder = new LogicalPlanBuilder(metaStore);
  }

  private PhysicalPlanBuilder buildPhysicalPlanBuilder(Map<String, Object> overrideProperties) {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final FunctionRegistry functionRegistry = new FunctionRegistry();
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    ksqlConfig = new KsqlConfig(configMap);
    return new PhysicalPlanBuilder(streamsBuilder,
        ksqlConfig,
        new FakeKafkaTopicClient(),
        functionRegistry,
        overrideProperties,
        false,
        metaStore,
        new MockSchemaRegistryClient(),
        testKafkaStreamsBuilder
    );

  }

  private QueryMetadata buildPhysicalPlan(final String query) throws Exception {
    final PlanNode logical = planBuilder.buildLogicalPlan(query);
    return physicalPlanBuilder.buildPhysicalPlan(new Pair<>(query, logical));
  }

  @Test
  public void shouldHaveKStreamDataSource() throws Exception {
    final QueryMetadata metadata = buildPhysicalPlan(simpleSelectFilter);
    assertThat(metadata.getDataSourceType(), equalTo(DataSource.DataSourceType.KSTREAM));
  }

  @Test
  public void shouldHaveOutputNode() throws Exception {
    final QueryMetadata queryMetadata = buildPhysicalPlan(simpleSelectFilter);
    assertThat(queryMetadata.getOutputNode(), instanceOf(KsqlBareOutputNode.class));
  }

  @Test
  public void shouldCreateExecutionPlan() throws Exception {
    String queryString = "SELECT col0, sum(col3), count(col3) FROM test1 "
        + "WHERE col0 > 100 GROUP BY col0;";
    final QueryMetadata metadata = buildPhysicalPlan(queryString);
    final String planText = metadata.getExecutionPlan();
    String[] lines = planText.split("\n");
    Assert.assertEquals(lines[0], " > [ SINK ] Schema: [COL0 : INT64 , KSQL_COL_1 : FLOAT64 "
        + ", KSQL_COL_2 : INT64].");
    Assert.assertEquals("\t\t > [ AGGREGATE ] Schema: [KSQL_INTERNAL_COL_0 : INT64 , KSQL_INTERNAL_COL_1 : FLOAT64 , KSQL_AGG_VARIABLE_0 : FLOAT64 , KSQL_AGG_VARIABLE_1 : INT64].", lines[1]);
    Assert.assertEquals("\t\t\t\t > [ PROJECT ] Schema: [KSQL_INTERNAL_COL_0 : INT64 , KSQL_INTERNAL_COL_1 : FLOAT64 , KSQL_INTERNAL_COL_2 : FLOAT64 , KSQL_INTERNAL_COL_3 : FLOAT64].", lines[2]);
    Assert.assertEquals("\t\t\t\t\t\t > [ FILTER ] Schema: [TEST1.ROWTIME : INT64 , TEST1.ROWKEY : INT64 , TEST1.COL0 : INT64 , TEST1.COL1 : STRING , TEST1.COL2 : STRING , TEST1.COL3 : FLOAT64 , TEST1.COL4 : ARRAY , TEST1.COL5 : MAP].", lines[3]);
    Assert.assertEquals("\t\t\t\t\t\t\t\t > [ SOURCE ] Schema: [TEST1.ROWTIME : INT64 , TEST1.ROWKEY : INT64 , TEST1.COL0 : INT64 , TEST1.COL1 : STRING , TEST1.COL2 : STRING , TEST1.COL3 : FLOAT64 , TEST1.COL4 : ARRAY , TEST1.COL5 : MAP].", lines[4]);
  }

  @Test
  public void shouldReturnCreatedKafkaStream() throws Exception {
    final QueryMetadata queryMetadata = buildPhysicalPlan(simpleSelectFilter);
    List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    Assert.assertEquals(1, calls.size());
    Assert.assertSame(calls.get(0).kafkaStreams, queryMetadata.getKafkaStreams());
  }

  @Test
  public void shouldAddMetricsInterceptors() throws Exception {
    buildPhysicalPlan(simpleSelectFilter);

    List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    Assert.assertEquals(1, calls.size());
    StreamsConfig config = calls.get(0).config;

    Object val = config.originals().get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    List<String> consumerInterceptors = (List<String>) val;
    Assert.assertEquals(1, consumerInterceptors.size());
    Assert.assertEquals(ConsumerCollector.class, Class.forName(consumerInterceptors.get(0)));

    val = config.originals().get(
        StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    List<String> producerInterceptors = (List<String>) val;
    Assert.assertEquals(1, producerInterceptors.size());
    Assert.assertEquals(ProducerCollector.class, Class.forName(producerInterceptors.get(0)));
  }

  public static class DummyConsumerInterceptor implements ConsumerInterceptor {
    public ConsumerRecords onConsume(ConsumerRecords consumerRecords) {
      return consumerRecords;
    }
    public void close() {  }
    public void onCommit(Map map) {  }
    public void configure(Map<String, ?> map) {  }
  }

  public static class DummyProducerInterceptor implements ProducerInterceptor {
    public void onAcknowledgement(RecordMetadata rm, Exception e) {}
    public ProducerRecord onSend(ProducerRecord producerRecords) {
      return producerRecords;
    }
    public void close() {  }
    public void configure(Map<String, ?> map) {  }
  }

  @Test
  public void shouldAddMetricsInterceptorsToExistingList() throws Exception {
    // Initialize override properties with lists for producer/consumer interceptors
    Map<String, Object> overrideProperties = new HashMap<>();
    List<String> consumerInterceptors = new LinkedList<>();
    consumerInterceptors.add(DummyConsumerInterceptor.class.getName());
    overrideProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        consumerInterceptors);
    List<String> producerInterceptors = new LinkedList<>();
    producerInterceptors.add(DummyProducerInterceptor.class.getName());
    overrideProperties.put(StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        producerInterceptors);
    physicalPlanBuilder = buildPhysicalPlanBuilder(overrideProperties);

    buildPhysicalPlan(simpleSelectFilter);

    List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    Assert.assertEquals(1, calls.size());
    StreamsConfig config = calls.get(0).config;

    Object val = config.originals().get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    consumerInterceptors = (List<String>) val;
    Assert.assertEquals(2, consumerInterceptors.size());
    Assert.assertEquals(DummyConsumerInterceptor.class.getName(), consumerInterceptors.get(0));
    Assert.assertEquals(ConsumerCollector.class, Class.forName(consumerInterceptors.get(1)));

    val = config.originals().get(
        StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    producerInterceptors = (List<String>) val;
    Assert.assertEquals(2, producerInterceptors.size());
    Assert.assertEquals(DummyProducerInterceptor.class.getName(), producerInterceptors.get(0));
    Assert.assertEquals(ProducerCollector.class, Class.forName(producerInterceptors.get(1)));
  }

  @Test
  public void shouldAddMetricsInterceptorsToExistingString() throws Exception {
    // Initialize override properties with class name strings for producer/consumer interceptors
    Map<String, Object> overrideProperties = new HashMap<>();
    overrideProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        DummyConsumerInterceptor.class.getName());
    overrideProperties.put(StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        DummyProducerInterceptor.class.getName());
    physicalPlanBuilder = buildPhysicalPlanBuilder(overrideProperties);

    buildPhysicalPlan(simpleSelectFilter);

    List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    Assert.assertEquals(1, calls.size());
    StreamsConfig config = calls.get(0).config;

    Object val = config.originals().get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    List<String> consumerInterceptors = (List<String>) val;
    Assert.assertEquals(2, consumerInterceptors.size());
    Assert.assertEquals(DummyConsumerInterceptor.class.getName(), consumerInterceptors.get(0));
    Assert.assertEquals(ConsumerCollector.class, Class.forName(consumerInterceptors.get(1)));

    val = config.originals().get(
        StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    List<String> producerInterceptors = (List<String>) val;
    Assert.assertEquals(2, producerInterceptors.size());
    Assert.assertEquals(DummyProducerInterceptor.class.getName(), producerInterceptors.get(0));
    Assert.assertEquals(ProducerCollector.class, Class.forName(producerInterceptors.get(1)));
  }

  public static class DummyConsumerInterceptor2 implements ConsumerInterceptor {
    public ConsumerRecords onConsume(ConsumerRecords consumerRecords) {
      return consumerRecords;
    }
    public void close() {  }
    public void onCommit(Map map) {  }
    public void configure(Map<String, ?> map) {  }
  }

  @Test
  public void shouldAddMetricsInterceptorsToExistingStringList() throws Exception {
    // Initialize override properties with class name strings for producer/consumer interceptors
    Map<String, Object> overrideProperties = new HashMap<>();
    String consumerInterceptorStr = DummyConsumerInterceptor.class.getName()
        + " , " + DummyConsumerInterceptor2.class.getName();
    overrideProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        consumerInterceptorStr);
    physicalPlanBuilder = buildPhysicalPlanBuilder(overrideProperties);

    buildPhysicalPlan(simpleSelectFilter);

    List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    Assert.assertEquals(1, calls.size());
    StreamsConfig config = calls.get(0).config;

    Object val = config.originals().get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    List<String> consumerInterceptors = (List<String>) val;
    Assert.assertEquals(3, consumerInterceptors.size());
    Assert.assertEquals(DummyConsumerInterceptor.class.getName(), consumerInterceptors.get(0));
    Assert.assertEquals(DummyConsumerInterceptor2.class.getName(), consumerInterceptors.get(1));
    Assert.assertEquals(ConsumerCollector.class, Class.forName(consumerInterceptors.get(2)));
  }
  @Test
  public void shouldCreateExpectedServiceId() {
    String serviceId = physicalPlanBuilder.getServiceId();
    assertThat(serviceId, equalTo(KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX
                                  + KsqlConfig.KSQL_SERVICE_ID_DEFAULT));
  }
}
