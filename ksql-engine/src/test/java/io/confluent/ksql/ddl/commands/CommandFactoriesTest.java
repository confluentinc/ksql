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

package io.confluent.ksql.ddl.commands;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DDLStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;

import static org.easymock.EasyMock.anyString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class CommandFactoriesTest {

  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);
  private final CommandFactories commandFactories = new CommandFactories(topicClient);
  private final HashMap<String, Expression> properties = new HashMap<>();

  @Before
  public void before() {
    properties.put(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"));
    properties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic"));
    EasyMock.expect(topicClient.isTopicExists(anyString())).andReturn(true);
    EasyMock.replay(topicClient);
  }

  @Test
  public void shouldCreateDDLCommandForRegisterTopic() {
    final DDLCommand result = commandFactories.create(
        new RegisterTopic(QualifiedName.of("blah"),
            true, properties),
        Collections.emptyMap());
    assertThat(result, instanceOf(RegisterTopicCommand.class));
  }

  @Test
  public void shouldCreateCommandForCreateStream() {
    final DDLCommand result = commandFactories.create(
        new CreateStream(QualifiedName.of("foo"),
            Collections.emptyList(), true, properties),
        Collections.emptyMap());

    assertThat(result, instanceOf(CreateStreamCommand.class));
  }

  @Test
  public void shouldCreateCommandForCreateTable() {
    final DDLCommand result = commandFactories.create(
        new CreateTable(QualifiedName.of("foo"),
            Collections.emptyList(), true, properties),
        Collections.emptyMap());

    assertThat(result, instanceOf(CreateTableCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropStream() {
    final DDLCommand result = commandFactories.create(
        new DropStream(QualifiedName.of("foo"), true),
        Collections.emptyMap());
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropTable() {
    final DDLCommand result = commandFactories.create(
        new DropTable(QualifiedName.of("foo"), true),
        Collections.emptyMap());
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropTopic() {
    final DDLCommand result = commandFactories.create(
        new DropTopic(QualifiedName.of("foo"), true),
        Collections.emptyMap());
    assertThat(result, instanceOf(DropTopicCommand.class));
  }

  @Test
  public void shouldCreateCommandForSetProperty() {
    final DDLCommand result = commandFactories.create(
        new SetProperty(Optional.empty(), "prop", "value"),
        new HashMap<>());
    assertThat(result, instanceOf(SetPropertyCommand.class));
  }

  @Test(expected = KsqlException.class)
  public void shouldThowKsqlExceptionIfCommandFactoryNotFound() {
    commandFactories.create(new DDLStatement() {}, Collections.emptyMap());
  }
}