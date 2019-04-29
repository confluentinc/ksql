/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.ddl.commands;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class CommandFactoriesTest {

  private static final java.util.Map<String, Object> NO_PROPS = Collections.emptyMap();
  private static final String sqlExpression = "sqlExpression";
  private static final List<TableElement> SOME_ELEMENTS = ImmutableList.of(
      new TableElement("bob", PrimitiveType.of(SqlType.STRING)));

  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);
  private final ServiceContext serviceContext = EasyMock.createNiceMock(ServiceContext.class);
  private final CommandFactories commandFactories = new CommandFactories(serviceContext);
  private final HashMap<String, Expression> properties = new HashMap<>();


  @Before
  public void before() {
    expect(serviceContext.getTopicClient())
        .andReturn(topicClient)
        .anyTimes();

    expect(serviceContext.getSchemaRegistryClient())
        .andReturn(EasyMock.createMock(SchemaRegistryClient.class))
        .anyTimes();

    properties.put(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"));
    properties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic"));
    EasyMock.expect(topicClient.isTopicExists(anyString())).andReturn(true);
    EasyMock.replay(topicClient, serviceContext);
  }

  @Test
  public void shouldCreateDDLCommandForRegisterTopic() {
    final DdlCommand result = commandFactories.create(
        sqlExpression, new RegisterTopic(QualifiedName.of("blah"),
            true, properties), NO_PROPS);

    assertThat(result, instanceOf(RegisterTopicCommand.class));
  }

  @Test
  public void shouldCreateCommandForCreateStream() {
    final DdlCommand result = commandFactories.create(
        sqlExpression, new CreateStream(QualifiedName.of("foo"),
            SOME_ELEMENTS, true, properties),
        NO_PROPS);

    assertThat(result, instanceOf(CreateStreamCommand.class));
  }

  @Test
  public void shouldCreateCommandForCreateTable() {
    final HashMap<String, Expression> tableProperties = validTableProps();

    final DdlCommand result = commandFactories
        .create(sqlExpression, createTable(tableProperties),
            NO_PROPS);

    assertThat(result, instanceOf(CreateTableCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropStream() {
    final DdlCommand result = commandFactories.create(sqlExpression,
        new DropStream(QualifiedName.of("foo"), true, true),
        NO_PROPS
    );
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropTable() {
    final DdlCommand result = commandFactories.create(sqlExpression,
        new DropTable(QualifiedName.of("foo"), true, true),
        NO_PROPS
    );
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropTopic() {
    final DdlCommand result = commandFactories.create(sqlExpression,
        new DropTopic(QualifiedName.of("foo"), true),
        NO_PROPS
    );
    assertThat(result, instanceOf(DropTopicCommand.class));
  }

  @Test(expected = KsqlException.class)
  public void shouldThowKsqlExceptionIfCommandFactoryNotFound() {
    commandFactories.create(sqlExpression, new ExecutableDdlStatement() {},
        NO_PROPS);
  }

  private HashMap<String, Expression> validTableProps() {
    final HashMap<String, Expression> tableProperties = new HashMap<>(properties);
    tableProperties.put(DdlConfig.KEY_NAME_PROPERTY, new StringLiteral("COL1"));
    return tableProperties;
  }

  private static CreateTable createTable(final HashMap<String, Expression> tableProperties) {
    return new CreateTable(QualifiedName.of("foo"),
        ImmutableList.of(
            new TableElement("COL1", PrimitiveType.of(SqlType.BIGINT)),
            new TableElement("COL2", PrimitiveType.of(SqlType.STRING))),
        true, tableProperties);
  }

  private void givenTopicsDoNotExist() {
    EasyMock.reset(topicClient);
    EasyMock.expect(topicClient.isTopicExists(anyString())).andReturn(false);
    EasyMock.replay(topicClient);
  }
}