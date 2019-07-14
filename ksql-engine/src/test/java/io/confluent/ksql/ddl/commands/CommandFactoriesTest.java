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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandFactoriesTest {

  private static final QualifiedName SOME_NAME = QualifiedName.of("bob");
  private static final Map<String, Object> NO_PROPS = Collections.emptyMap();
  private static final String sqlExpression = "sqlExpression";
  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING)));

  private static final Map<String, Literal> MINIMIM_PROPS = ImmutableMap.of(
      CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"),
      CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic")
  );

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ServiceContext serviceContext;

  private CommandFactories commandFactories;

  private KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
  private CreateSourceProperties withProperties =
      CreateSourceProperties.from(MINIMIM_PROPS);


  @Before
  public void before() {
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(any())).thenReturn(true);

    commandFactories = new CommandFactories(serviceContext);
  }

  @Test
  public void shouldCreateCommandForCreateStream() {
    // Given:
    final CreateStream ddlStatement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement, ksqlConfig, NO_PROPS);

    assertThat(result, instanceOf(CreateStreamCommand.class));
  }

  @Test
  public void shouldCreateCommandForCreateTable() {
    // Given:
    final CreateTable ddlStatement = new CreateTable(SOME_NAME,
        TableElements.of(
            new TableElement(Namespace.VALUE, "COL1", new Type(SqlTypes.BIGINT)),
            new TableElement(Namespace.VALUE, "COL2", new Type(SqlTypes.STRING))),
        true, withProperties);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement, ksqlConfig, NO_PROPS);

    // Then:
    assertThat(result, instanceOf(CreateTableCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropStream() {
    // Given:
    final DropStream ddlStatement = new DropStream(SOME_NAME, true, true);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement, ksqlConfig, NO_PROPS);

    // Then:
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropTable() {
    // Given:
    final DropTable ddlStatement = new DropTable(SOME_NAME, true, true);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement, ksqlConfig, NO_PROPS);

    // Then:
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnUnsupportedStatementType() {
    // Given:
    final ExecutableDdlStatement ddlStatement = new ExecutableDdlStatement() {
    };

    // Then:
    commandFactories.create(sqlExpression, ddlStatement, ksqlConfig, NO_PROPS);
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromPropertiesNotConfigOrOverrides() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    );

    givenProperty(CommonCreateConfigs.WRAP_SINGLE_VALUE, new BooleanLiteral("false"));

    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, ksqlConfig, overrides);

    // Then:
    assertThat(cmd, is(instanceOf(CreateSourceCommand.class)));
    assertThat(((CreateSourceCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromOverridesNotConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    );

    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, ksqlConfig, overrides);

    // Then:
    assertThat(cmd, is(instanceOf(CreateSourceCommand.class)));
    assertThat(((CreateSourceCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, ksqlConfig, ImmutableMap.of());

    // Then:
    assertThat(cmd, is(instanceOf(CreateSourceCommand.class)));
    assertThat(((CreateSourceCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromDefaultConfig() {
    // Given:
    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, ksqlConfig, ImmutableMap.of());

    // Then:
    assertThat(cmd, is(instanceOf(CreateSourceCommand.class)));
    assertThat(((CreateSourceCommand) cmd).getSerdeOptions(),
        not(contains(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromPropertiesNotConfigOrOverrides() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    );

    givenProperty(CommonCreateConfigs.WRAP_SINGLE_VALUE, new BooleanLiteral("false"));

    final DdlStatement statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, ksqlConfig, overrides);

    // Then:
    assertThat(cmd, is(instanceOf(CreateSourceCommand.class)));
    assertThat(((CreateSourceCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromOverridesNotConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    );

    final DdlStatement statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, ksqlConfig, overrides);

    // Then:
    assertThat(cmd, is(instanceOf(CreateSourceCommand.class)));
    assertThat(((CreateSourceCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    final DdlStatement statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, ksqlConfig, ImmutableMap.of());

    // Then:
    assertThat(cmd, is(instanceOf(CreateSourceCommand.class)));
    assertThat(((CreateSourceCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromDefaultConfig() {
    // Given:
    final DdlStatement statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, ksqlConfig, ImmutableMap.of());

    // Then:
    assertThat(cmd, is(instanceOf(CreateSourceCommand.class)));
    assertThat(((CreateSourceCommand) cmd).getSerdeOptions(),
        not(contains(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  private void givenProperty(final String name, final Literal value) {
    final Map<String, Literal> props = withProperties.copyOfOriginalLiterals();
    props.put(name, value);
    withProperties = CreateSourceProperties.from(props);
  }
}