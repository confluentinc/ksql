/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlTopicInfo;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SimpleFunctionInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import io.confluent.rest.RestConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

public abstract class CustomExecutorsTest {

  static final Schema SCHEMA =
      SchemaBuilder.struct().field("val", Schema.OPTIONAL_STRING_SCHEMA).build();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private KsqlEngine realEngine;
  private MutableMetaStore metaStore;

  KsqlConfig ksqlConfig;
  KsqlEngine engine;
  ServiceContext serviceContext;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    serviceContext = TestServiceContext.create();
    engine = KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
    realEngine = engine;

    final Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    configMap.put("ksql.command.topic.suffix", "commands");
    configMap.put(RestConfig.LISTENERS_CONFIG, "http://localhost:8088");
    ksqlConfig = new KsqlConfig(configMap);
  }

  @After
  public void tearDown() {
    realEngine.close();
    serviceContext.close();
  }


  @SuppressWarnings("unchecked")
  <T extends StructuredDataSource> T givenSource(
      final DataSource.DataSourceType type,
      final String name) {
    final KsqlTopic topic = givenKsqlTopic(name);
    givenKafkaTopic(name);

    final StructuredDataSource source;
    switch (type) {
      case KSTREAM:
        source =
            new KsqlStream<>(
                "statement", name, SCHEMA, SCHEMA.field("val"),
                new MetadataTimestampExtractionPolicy(), topic, Serdes.String());
        break;
      case KTABLE:
        source =
            new KsqlTable<>(
                "statement", name, SCHEMA, SCHEMA.field("val"),
                new MetadataTimestampExtractionPolicy(), topic, "store", Serdes.String());
        break;
      case KTOPIC:
      default:
        throw new IllegalArgumentException(type.toString());
    }
    metaStore.putSource(source);

    return (T) source;
  }

  KsqlTopic givenKsqlTopic(String name) {
    final KsqlTopic topic = new KsqlTopic(name, name, new KsqlJsonTopicSerDe(), false);
    givenKafkaTopic(name);
    metaStore.putTopic(topic);
    return topic;
  }

  void givenKafkaTopic(final String name) {
    ((FakeKafkaTopicClient) serviceContext.getTopicClient())
        .preconditionTopicExists(name, 1, (short) 1, Collections.emptyMap());
  }

  PreparedStatement prepare(final String sql) {
    return engine.prepare(new DefaultKsqlParser().parse(sql).get(0));
  }

  @SuppressWarnings("SameParameterValue")
  PersistentQueryMetadata givenPersistentQuery(final String id) {
    final PersistentQueryMetadata metadata = mock(PersistentQueryMetadata.class);
    when(metadata.getQueryId()).thenReturn(new QueryId(id));
    when(metadata.getSinkNames()).thenReturn(ImmutableSet.of(id));
    when(metadata.getResultSchema()).thenReturn(SCHEMA);

    return metadata;
  }

}
