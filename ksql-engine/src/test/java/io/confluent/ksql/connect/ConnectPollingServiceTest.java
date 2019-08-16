/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectPollingServiceTest {

  private final Node node = new Node(1, "localhost", 1234);

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KsqlExecutionContext executionContext;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;

  private MockAdminClient adminClient;
  private ConnectPollingService pollingService;
  private Set<String> subjects;
  private MutableMetaStore metaStore;

  @Before
  public void setUp() throws Exception {
    adminClient = new MockAdminClient(Collections.singletonList(node), node);
    subjects = new HashSet<>();
    metaStore = MetaStoreFixture.getNewMetaStore(functionRegistry);

    when(executionContext.getMetaStore()).thenReturn(metaStore);
    when(executionContext.getServiceContext()).thenReturn(serviceContext);

    when(serviceContext.getAdminClient()).thenReturn(adminClient);
    when(serviceContext.getSchemaRegistryClient()).thenReturn(schemaRegistryClient);

    when(schemaRegistryClient.getAllSubjects()).thenReturn(subjects);
  }

  @Test
  public void shouldCreateSourceFromConnector() {
    // Given:
    final CreateSource[] ref = new CreateSource[]{null};
    givenTopic("foo");
    givenSubject("foo");
    givenPollingService(cs -> ref[0] = cs);
    givenConnector("foo");

    // When:
    pollingService.drainQueue();
    pollingService.runOneIteration();

    // Then:
    assertThat(
        SqlFormatter.formatSql(ref[0]),
        is("CREATE TABLE IF NOT EXISTS FOO "
            + "WITH ("
            + "KAFKA_TOPIC='foo', "
            + "KEY='key', "
            + "SOURCE_CONNECTOR='connector', "
            + "VALUE_FORMAT='AVRO');"));
  }

  @Test
  public void shouldNotCreateSourceFromConnectorWithoutTopicMatch() {
    // Given:
    final CreateSource[] ref = new CreateSource[]{null};
    givenTopic("foo");
    givenSubject("foo");
    givenPollingService(cs -> ref[0] = cs);
    givenConnector("bar");

    // When:
    pollingService.drainQueue();
    pollingService.runOneIteration();

    // Then:
    assertThat(ref[0], nullValue());
  }

  @Test
  public void shouldNotCreateSourceFromConnectorWithoutSubjectMatch() {
    // Given:
    final CreateSource[] ref = new CreateSource[]{null};
    givenTopic("foo");
    givenPollingService(cs -> ref[0] = cs);
    givenConnector("foo");

    // When:
    pollingService.drainQueue();
    pollingService.runOneIteration();

    // Then:
    assertThat(ref[0], nullValue());
  }

  @Test
  public void shouldNotCreateSourceForAlreadyRegisteredSource() {
    // Given:
    final CreateSource[] ref = new CreateSource[]{null};
    givenTopic("foo");
    givenSubject("foo");
    givenPollingService(cs -> ref[0] = cs);
    givenConnector("foo");

    final DataSource<?> source = mock(DataSource.class);
    when(source.getName()).thenReturn("FOO");
    metaStore.putSource(source);

    // When:
    pollingService.drainQueue();
    pollingService.runOneIteration();

    // Then:
    assertThat(ref[0], nullValue());
  }

  @Test
  public void shouldNotPollIfNoRegisteredConnectors() {
    givenPollingService(foo -> {});

    // When:
    pollingService.runOneIteration();

    // Then:
    verifyZeroInteractions(serviceContext);
  }

  @Test(timeout = 30_000L)
  public void shouldImmediatelyShutdown() {
    pollingService = new ConnectPollingService(executionContext, foo -> {}, 60);

    // When:
    pollingService.startAsync().awaitRunning();
    pollingService.stopAsync().awaitTerminated();

    // Then: (test immediately stops)
  }

  private void givenTopic(final String topicName) {
    adminClient.addTopic(
        false,
        topicName,
        ImmutableList.of(
            new TopicPartitionInfo(0, node, ImmutableList.of(), ImmutableList.of())),
        ImmutableMap.of());
  }

  private void givenPollingService(final Consumer<CreateSource> callback) {
    pollingService = new ConnectPollingService(executionContext, callback);
  }

  private void givenConnector(final String topic) {
    pollingService.addConnector(
        new Connector(
            "connector",
            foo -> Objects.equals(foo, topic),
            foo -> topic,
            DataSourceType.KTABLE,
            "key"
    ));
  }

  private void givenSubject(final String topicName) {
    subjects.add(topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }

}