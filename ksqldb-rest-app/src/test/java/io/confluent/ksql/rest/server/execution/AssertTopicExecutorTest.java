/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.AssertTopic;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.AssertTopicEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class AssertTopicExecutorTest {
  @Mock
  private KsqlExecutionContext engine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private TopicDescription topicDescription;
  @Mock
  private TopicPartitionInfo partition;
  private final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());

  @Before
  public void setUp() {
    when(engine.getKsqlConfig()).thenReturn(ksqlConfig);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists("topicName"))
        .thenReturn(true);
    when(topicClient.describeTopic("topicName"))
        .thenReturn(topicDescription);
    when(topicDescription.partitions())
        .thenReturn(ImmutableList.of(partition));
    when(partition.replicas())
        .thenReturn(ImmutableList.of());
  }

  @Test
  public void shouldAssertTopic() {
    // Given:
    final Map<String, Literal> configs = ImmutableMap.of(
        "partitions", new IntegerLiteral(1), "replicas", new IntegerLiteral(0));
    final AssertTopic assertTopic = new AssertTopic(Optional.empty(), "topicName", configs, Optional.empty(), true);
    final ConfiguredStatement<AssertTopic> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertTopic),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = AssertTopicExecutor
        .execute(statement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    assertThat(((AssertTopicEntity) entity.get()).getTopicName(), is("topicName"));
    assertThat(((AssertTopicEntity) entity.get()).getExists(), is(true));
  }

  @Test
  public void shouldFailToAssertNonExistingTopic() {
    // Given:
    final Map<String, Literal> configs = ImmutableMap.of(
        "partitions", new IntegerLiteral(1), "replicas", new IntegerLiteral(0));
    final AssertTopic assertTopic = new AssertTopic(Optional.empty(), "fakeTopic", configs, Optional.empty(), true);
    final ConfiguredStatement<AssertTopic> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertTopic),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class,
        () -> AssertTopicExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(417));
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Topic fakeTopic does not exist"));
  }

  @Test
  public void shouldFailToAssertWrongConfigs() {
    // Given:
    final Map<String, Literal> configs = ImmutableMap.of(
        "partitions", new IntegerLiteral(10), "replicas", new IntegerLiteral(10), "abc", new IntegerLiteral(23));
    final AssertTopic assertTopic = new AssertTopic(Optional.empty(), "topicName", configs, Optional.empty(), true);
    final ConfiguredStatement<AssertTopic> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertTopic),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class,
        () -> AssertTopicExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(417));
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Mismatched configuration for topic topicName: For config partitions, expected 10 got 1\n"
        + "Mismatched configuration for topic topicName: For config replicas, expected 10 got 0\n"
        + "Cannot assert unknown topic property: abc"));
  }

  @Test
  public void shouldAssertTopicNotExists() {
    // Given:
    final Map<String, Literal> configs = ImmutableMap.of(
        "partitions", new IntegerLiteral(1), "replicas", new IntegerLiteral(0));
    final AssertTopic assertTopic = new AssertTopic(Optional.empty(), "fakeTopic", configs, Optional.empty(), false);
    final ConfiguredStatement<AssertTopic> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertTopic),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = AssertTopicExecutor
        .execute(statement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    assertThat(((AssertTopicEntity) entity.get()).getTopicName(), is("fakeTopic"));
    assertThat(((AssertTopicEntity) entity.get()).getExists(), is(false));
  }

  @Test
  public void shouldFailToAssertTopicNotExists() {
    // Given:
    final Map<String, Literal> configs = ImmutableMap.of(
        "partitions", new IntegerLiteral(1), "replicas", new IntegerLiteral(0));
    final AssertTopic assertTopic = new AssertTopic(Optional.empty(), "topicName", configs, Optional.empty(), false);
    final ConfiguredStatement<AssertTopic> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertTopic),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class,
        () -> AssertTopicExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(417));
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Topic topicName exists"));
  }
}
