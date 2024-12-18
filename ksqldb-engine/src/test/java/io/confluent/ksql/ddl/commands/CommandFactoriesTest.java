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

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.ddl.commands.AlterSourceCommand;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.execution.ddl.commands.RegisterTypeCommand;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DropType;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandFactoriesTest {
  private static final ColumnConstraints PRIMARY_KEY_CONSTRAINT =
      new ColumnConstraints.Builder().primaryKey().build();

  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final SourceName TABLE_NAME = SourceName.of("tablename");
  private static final String sqlExpression = "sqlExpression";
  private static final TableElement ELEMENT1 =
      tableElement("bob", new Type(SqlTypes.STRING));
  private static final TableElements SOME_ELEMENTS = TableElements.of(ELEMENT1);
  private static final TableElements ELEMENTS_WITH_PK = TableElements.of(
      tableElement("k", new Type(SqlTypes.STRING), PRIMARY_KEY_CONSTRAINT),
      ELEMENT1
  );
  private static final String TOPIC_NAME = "some topic";
  private static final Map<String, Literal> MINIMUM_PROPS = ImmutableMap.of(
      CommonCreateConfigs.KEY_FORMAT_PROPERTY, new StringLiteral("KAFKA"),
      CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"),
      CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(TOPIC_NAME)
  );
  private static final String SOME_TYPE_NAME = "newtype";
  private static final Map<String, Object> OVERRIDES = ImmutableMap.of(
      KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
  );

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private MetaStore metaStore;
  @Mock
  private CreateSourceFactory createSourceFactory;
  @Mock
  private DropSourceFactory dropSourceFactory;
  @Mock
  private RegisterTypeFactory registerTypeFactory;
  @Mock
  private DropTypeFactory dropTypeFactory;
  @Mock
  private AlterSourceFactory alterSourceFactory;
  @Mock
  private CreateStreamCommand createStreamCommand;
  @Mock
  private CreateTableCommand createTableCommand;
  @Mock
  private DropSourceCommand dropSourceCommand;
  @Mock
  private RegisterTypeCommand registerTypeCommand;
  @Mock
  private DropTypeCommand dropTypeCommand;
  @Mock
  private AlterSourceCommand alterSourceCommand;

  private CommandFactories commandFactories;
  private KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
  private final CreateSourceProperties withProperties =
      CreateSourceProperties.from(MINIMUM_PROPS);

  @Before
  public void before() {
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(any())).thenReturn(true);
    when(createSourceFactory.createStreamCommand(any(), any()))
        .thenReturn(createStreamCommand);
    when(createSourceFactory.createTableCommand(any(CreateTable.class), any()))
        .thenReturn(createTableCommand);
    when(dropSourceFactory.create(any(DropStream.class))).thenReturn(dropSourceCommand);
    when(dropSourceFactory.create(any(DropTable.class))).thenReturn(dropSourceCommand);
    when(registerTypeFactory.create(any())).thenReturn(registerTypeCommand);
    when(dropTypeFactory.create(any())).thenReturn(dropTypeCommand);
    when(alterSourceFactory.create(any())).thenReturn(alterSourceCommand);

    givenCommandFactoriesWithMocks();
  }

  private void givenCommandFactories() {
    commandFactories = new CommandFactories(
        serviceContext,
        metaStore
    );
  }

  private void givenCommandFactoriesWithMocks() {
    commandFactories = new CommandFactories(
        createSourceFactory,
        dropSourceFactory,
        registerTypeFactory,
        dropTypeFactory,
        alterSourceFactory
    );
  }

  @Test
  public void shouldCreateCommandForCreateStream() {
    // Given:
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, false, true, withProperties, false);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, statement, SessionConfig.of(ksqlConfig, emptyMap()));

    assertThat(result, is(createStreamCommand));
    verify(createSourceFactory).createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldCreateCommandForCreateSourceStream() {
    // Given:
    final CreateStream statement = new CreateStream(SOME_NAME,
        TableElements.of(
            tableElement("COL1", new Type(SqlTypes.BIGINT)),
            tableElement("COL2", new Type(SqlTypes.STRING))),
        false, true, withProperties, true);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, statement, SessionConfig.of(ksqlConfig, emptyMap()));

    // Then:
    assertThat(result, is(createStreamCommand));
    verify(createSourceFactory).createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldCreateCommandForStreamWithOverriddenProperties() {
    // Given:
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, false, true, withProperties, false);

    // When:
    commandFactories.create(sqlExpression, statement, SessionConfig.of(ksqlConfig, OVERRIDES));

    verify(createSourceFactory).createStreamCommand(
        statement,
        ksqlConfig.cloneWithPropertyOverwrite(OVERRIDES));
  }

  @Test
  public void shouldCreateCommandForCreateTable() {
    // Given:
    final CreateTable statement = new CreateTable(SOME_NAME,
        TableElements.of(
            tableElement("COL1", new Type(SqlTypes.BIGINT)),
            tableElement("COL2", new Type(SqlTypes.STRING))),
        false, true, withProperties, false);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, statement, SessionConfig.of(ksqlConfig, emptyMap()));

    // Then:
    assertThat(result, is(createTableCommand));
    verify(createSourceFactory).createTableCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldCreateCommandForCreateSourceTable() {
    // Given:
    final CreateTable statement = new CreateTable(SOME_NAME,
        TableElements.of(
            tableElement("COL1", new Type(SqlTypes.BIGINT)),
            tableElement("COL2", new Type(SqlTypes.STRING))),
        false, true, withProperties, true);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, statement, SessionConfig.of(ksqlConfig, emptyMap()));

    // Then:
    assertThat(result, is(createTableCommand));
    verify(createSourceFactory).createTableCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldCreateCommandForCreateTableWithOverriddenProperties() {
    // Given:
    final CreateTable statement = new CreateTable(SOME_NAME,
        TableElements.of(
            tableElement("COL1", new Type(SqlTypes.BIGINT)),
            tableElement( "COL2", new Type(SqlTypes.STRING))),
        false, true, withProperties, false);

    // When:
    commandFactories.create(sqlExpression, statement, SessionConfig.of(ksqlConfig, OVERRIDES));

    // Then:
    verify(createSourceFactory).createTableCommand(
        statement,
        ksqlConfig.cloneWithPropertyOverwrite(OVERRIDES)
    );
  }

  @Test
  public void shouldCreateCommandForAlterSource() {
    // Given:
    final AlterSource ddlStatement = new AlterSource(SOME_NAME, DataSourceType.KSTREAM, new ArrayList<>());

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement,  SessionConfig.of(ksqlConfig, emptyMap()));

    // Then:
    assertThat(result, is(alterSourceCommand));
    verify(alterSourceFactory).create(ddlStatement);
  }

  @Test
  public void shouldCreateCommandForDropStream() {
    // Given:
    final DropStream ddlStatement = new DropStream(SOME_NAME, true, true);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement, SessionConfig.of(ksqlConfig, emptyMap()));

    // Then:
    assertThat(result, is(dropSourceCommand));
    verify(dropSourceFactory).create(ddlStatement);
  }

  @Test
  public void shouldCreateCommandForDropTable() {
    // Given:
    final DropTable ddlStatement = new DropTable(TABLE_NAME, true, true);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement, SessionConfig.of(ksqlConfig, emptyMap()));

    // Then:
    assertThat(result, is(dropSourceCommand));
    verify(dropSourceFactory).create(ddlStatement);
  }

  @Test
  public void shouldCreateCommandForRegisterType() {
    // Given:
    final RegisterType ddlStatement = new RegisterType(
        Optional.empty(),
        "alias",
        new Type(SqlStruct.builder().field("foo", SqlPrimitiveType.of(SqlBaseType.STRING)).build()),
        true
    );

    // When:
    final DdlCommand result = commandFactories.create(
        sqlExpression, ddlStatement, SessionConfig.of(ksqlConfig, emptyMap()));

    // Then:
    assertThat(result, is(registerTypeCommand));
    verify(registerTypeFactory).create(ddlStatement);
  }

  @Test
  public void shouldCreateDropType() {
    // Given:
    final DropType dropType = new DropType(Optional.empty(), SOME_TYPE_NAME, false);

    // When:
    final DropTypeCommand cmd = (DropTypeCommand) commandFactories.create(
        "sqlExpression",
        dropType,
        SessionConfig.of(ksqlConfig, emptyMap())
    );

    // Then:
    assertThat(cmd, is(dropTypeCommand));
    verify(dropTypeFactory).create(dropType);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnUnsupportedStatementType() {
    // Given:
    final ExecutableDdlStatement ddlStatement = new ExecutableDdlStatement() {
    };

    // Then:
    commandFactories.create(sqlExpression, ddlStatement, SessionConfig.of(ksqlConfig, emptyMap()));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromOverridesNotConfig() {
    // Given:
    givenCommandFactories();
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    );

    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, true, withProperties, false);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, SessionConfig.of(ksqlConfig, overrides));

    // Then:
    assertThat(cmd, is(instanceOf(CreateStreamCommand.class)));
    assertThat(((CreateStreamCommand) cmd).getFormats().getValueFeatures().all(),
        contains(SerdeFeature.UNWRAP_SINGLES));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromOverridesNotConfig() {
    // Given:
    givenCommandFactories();
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    );

    final DdlStatement statement =
        new CreateTable(SOME_NAME, ELEMENTS_WITH_PK,
            false, true, withProperties, false);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, SessionConfig.of(ksqlConfig, overrides));

    // Then:
    assertThat(cmd, is(instanceOf(CreateTableCommand.class)));
    assertThat(((CreateTableCommand) cmd).getFormats().getValueFeatures().all(),
        contains(SerdeFeature.UNWRAP_SINGLES));
  }

  private static TableElement tableElement(
      final String name,
      final Type type
  ) {
    return tableElement(name, type, ColumnConstraints.NO_COLUMN_CONSTRAINTS);
  }

  private static TableElement tableElement(
      final String name,
      final Type type,
      final ColumnConstraints constraints
  ) {
    final TableElement te = mock(TableElement.class, name);
    when(te.getName()).thenReturn(ColumnName.of(name));
    when(te.getType()).thenReturn(type);
    when(te.getConstraints()).thenReturn(constraints);
    return te;
  }
}