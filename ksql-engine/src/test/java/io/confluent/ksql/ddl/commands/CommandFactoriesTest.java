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

import static io.confluent.ksql.model.WindowType.HOPPING;
import static io.confluent.ksql.model.WindowType.SESSION;
import static io.confluent.ksql.model.WindowType.TUMBLING;
import static io.confluent.ksql.parser.tree.TableElement.Namespace.KEY;
import static io.confluent.ksql.parser.tree.TableElement.Namespace.VALUE;
import static io.confluent.ksql.serde.Format.AVRO;
import static io.confluent.ksql.serde.Format.JSON;
import static io.confluent.ksql.serde.Format.KAFKA;
import static io.confluent.ksql.util.SchemaUtil.ROWKEY_NAME;
import static io.confluent.ksql.util.SchemaUtil.ROWTIME_NAME;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.ddl.commands.CommandFactories.SerdeOptionsSupplier;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.execution.ddl.commands.RegisterTypeCommand;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DropType;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandFactoriesTest {

  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final SourceName TABLE_NAME = SourceName.of("tablename");
  private static final Map<String, Object> NO_PROPS = Collections.emptyMap();
  private static final String sqlExpression = "sqlExpression";
  private static final TableElement ELEMENT1 =
      tableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING));
  private static final TableElement ELEMENT2 =
      tableElement(Namespace.VALUE, "hojjat", new Type(SqlTypes.STRING));
  private static final TableElements SOME_ELEMENTS = TableElements.of(ELEMENT1);
  private static final TableElements TWO_ELEMENTS = TableElements.of(ELEMENT1, ELEMENT2);
  private static final String TOPIC_NAME = "some topic";
  private static final Map<String, Literal> MINIMIM_PROPS = ImmutableMap.of(
      CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"),
      CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(TOPIC_NAME)
  );
  private static final TableElements ONE_ELEMENT = TableElements.of(
      tableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING)));
  private static final Set<SerdeOption> SOME_SERDE_OPTIONS = ImmutableSet
      .of(SerdeOption.UNWRAP_SINGLE_VALUES);
  private static final String SOME_TYPE_NAME = "newtype";

  @Mock
  private KsqlStream ksqlStream;
  @Mock
  private KsqlTable ksqlTable;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private MetaStore metaStore;
  @Mock
  private SerdeOptionsSupplier serdeOptionsSupplier;
  @Mock
  private ValueSerdeFactory serdeFactory;
  @Mock
  private Serde<GenericRow> serde;

  private CommandFactories commandFactories;
  private KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
  private CreateSourceProperties withProperties =
      CreateSourceProperties.from(MINIMIM_PROPS);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(any())).thenReturn(true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(ksqlStream);
    when(metaStore.getSource(TABLE_NAME)).thenReturn(ksqlTable);
    when(ksqlStream.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(ksqlTable.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(serdeFactory.create(any(), any(), any(), any(), any(), any())).thenReturn(serde);

    givenCommandFactories();
  }

  private void givenCommandFactories() {
    commandFactories = new CommandFactories(
        serviceContext,
        metaStore
    );
  }

  private void givenCommandFactoriesWithMocks() {
    commandFactories = new CommandFactories(
        serviceContext,
        metaStore,
        serdeOptionsSupplier,
        serdeFactory
    );
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
            tableElement(Namespace.VALUE, "COL1", new Type(SqlTypes.BIGINT)),
            tableElement(Namespace.VALUE, "COL2", new Type(SqlTypes.STRING))),
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
    final DropTable ddlStatement = new DropTable(TABLE_NAME, true, true);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement, ksqlConfig, NO_PROPS);

    // Then:
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateCommandForRegisterType() {
    // Given:
    final RegisterType ddlStatement = new RegisterType(
        Optional.empty(),
        "alias",
        new Type(SqlStruct.builder().field("foo", SqlPrimitiveType.of(SqlBaseType.STRING)).build())
    );

    // When:
    final DdlCommand result = commandFactories.create(
        sqlExpression, ddlStatement, ksqlConfig, NO_PROPS);

    // Then:
    assertThat(result, instanceOf(RegisterTypeCommand.class));
  }

  @Test
  public void shouldCreateDropType() {
    // Given:
    final DropType dropType = new DropType(Optional.empty(), SOME_TYPE_NAME);

    // When:
    final DropTypeCommand cmd = (DropTypeCommand) commandFactories.create(
        "sqlExpression",
        dropType,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    assertThat(cmd.getTypeName(), equalTo(SOME_TYPE_NAME));
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
    assertThat(cmd, is(instanceOf(CreateStreamCommand.class)));
    assertThat(((CreateStreamCommand) cmd).getSerdeOptions(),
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
    assertThat(cmd, is(instanceOf(CreateStreamCommand.class)));
    assertThat(((CreateStreamCommand) cmd).getSerdeOptions(),
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
    assertThat(cmd, is(instanceOf(CreateStreamCommand.class)));
    assertThat(((CreateStreamCommand) cmd).getSerdeOptions(),
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
    assertThat(cmd, is(instanceOf(CreateStreamCommand.class)));
    assertThat(((CreateStreamCommand) cmd).getSerdeOptions(),
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
    assertThat(cmd, is(instanceOf(CreateTableCommand.class)));
    assertThat(((CreateTableCommand) cmd).getSerdeOptions(),
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
    assertThat(cmd, is(instanceOf(CreateTableCommand.class)));
    assertThat(((CreateTableCommand) cmd).getSerdeOptions(),
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
    assertThat(cmd, is(instanceOf(CreateTableCommand.class)));
    assertThat(((CreateTableCommand) cmd).getSerdeOptions(),
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
    assertThat(cmd, is(instanceOf(CreateTableCommand.class)));
    assertThat(((CreateTableCommand) cmd).getSerdeOptions(),
        not(contains(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldThrowOnNoElementsInCreateStream() {
    // Given:
    final DdlStatement statement
        = new CreateStream(SOME_NAME, TableElements.of(), true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The statement does not define any columns.");

    // When:
    commandFactories.create("expression", statement, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldThrowOnNoElementsInCreateTable() {
    // Given:
    final DdlStatement statement
        = new CreateTable(SOME_NAME, TableElements.of(), true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The statement does not define any columns.");

    // When:
    commandFactories.create("expression", statement, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldNotThrowWhenThereAreElementsInCreateStream() {
    // Given:
    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    commandFactories.create("expression", statement, ksqlConfig, emptyMap());

    // Then: not exception thrown
  }

  @Test
  public void shouldNotThrowWhenThereAreElementsInCreateTable() {
    // Given:
    final DdlStatement statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    commandFactories.create("expression", statement, ksqlConfig, emptyMap());

    // Then: not exception thrown
  }

  @Test
  public void shouldThrowIfTopicDoesNotExistForStream() {
    // Given:
    when(topicClient.isTopicExists(any())).thenReturn(false);
    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Kafka topic does not exist: " + TOPIC_NAME);

    // When:
    commandFactories.create("expression", statement, ksqlConfig, emptyMap());
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldNotThrowIfTopicDoesExist() {
    // Given:
    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    commandFactories.create("expression", statement, ksqlConfig, emptyMap());

    // Then:
    verify(topicClient).isTopicExists(TOPIC_NAME);
  }

  @Test
  public void shouldThrowIfKeyFieldNotInSchemaForStream() {
    // Given:
    givenProperty(CreateConfigs.KEY_NAME_PROPERTY, new StringLiteral("will-not-find-me"));
    final DdlStatement statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The KEY column set in the WITH clause does not exist in the schema: "
            + "'WILL-NOT-FIND-ME'");

    // When:
    commandFactories.create("expression", statement, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldThrowIfTimestampColumnDoesNotExistForStream() {
    // Given:
    givenProperty(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY,
        new StringLiteral("will-not-find-me")
    );
    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The TIMESTAMP column set in the WITH clause does not exist in the schema: "
            + "'WILL-NOT-FIND-ME'");

    // When:
    commandFactories.create("expression", statement, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldBuildSerdeOptionsForStream() {
    // Given:
    givenCommandFactoriesWithMocks();
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_ELEMENT, true, withProperties);
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
        .build();
    when(serdeOptionsSupplier.build(any(), any(), any(), any())).thenReturn(SOME_SERDE_OPTIONS);

    // When:
    final CreateStreamCommand cmd = (CreateStreamCommand) commandFactories.create(
        "expression",
        statement,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    verify(serdeOptionsSupplier).build(
        schema,
        statement.getProperties().getValueFormat(),
        statement.getProperties().getWrapSingleValues(),
        ksqlConfig
    );
    assertThat(cmd.getSerdeOptions(), is(SOME_SERDE_OPTIONS));
  }

  @Test
  public void shouldBuildSchemaWithImplicitKeyFieldForStream() {
    // Given:
    final CreateStream statement = new CreateStream(SOME_NAME, TWO_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand result = (CreateStreamCommand) commandFactories.create(
        "expression",
        statement,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    assertThat(result.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("hojjat"), SqlTypes.STRING)
        .build()
    ));
  }

  @Test
  public void shouldBuildSchemaWithExplicitKeyFieldForStream() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(
            tableElement(Namespace.KEY, "ROWKEY", new Type(SqlTypes.STRING)),
            ELEMENT1,
            ELEMENT2
        ),
        true,
        withProperties
    );

    // When:
    final CreateStreamCommand result = (CreateStreamCommand) commandFactories.create(
        "expression",
        statement,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    assertThat(result.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("hojjat"), SqlTypes.STRING)
        .build()
    ));
  }

  @Test
  public void shouldCreateSerdeToValidateValueFormatCanHandleValueSchemaForStream() {
    // Given:
    givenCommandFactoriesWithMocks();
    final CreateStream statement = new CreateStream(SOME_NAME, TWO_ELEMENTS, true, withProperties);
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("hojjat"), SqlTypes.STRING)
        .build();

    // When:
    commandFactories.create("expression", statement, ksqlConfig, emptyMap());

    // Then:
    verify(serdeFactory).create(
        FormatInfo.of(JSON, Optional.empty(), Optional.empty()),
        PersistenceSchema.from(schema.valueConnectSchema(), false),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    );
  }

  @Test
  public void shouldDefaultToKafkaKeySerdeForStream() {
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = (CreateStreamCommand) commandFactories.create(
        "expression",
        statement,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    assertThat(cmd.getTopic().getKeyFormat(), is(KeyFormat.nonWindowed(FormatInfo.of(KAFKA))));
  }

  @Test
  public void shouldHandleValueAvroSchemaNameForStream() {
    // Given:
    givenCommandFactoriesWithMocks();
    givenProperty("VALUE_FORMAT", new StringLiteral("Avro"));
    givenProperty("value_avro_schema_full_name", new StringLiteral("full.schema.name"));
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = (CreateStreamCommand) commandFactories.create(
        "expression",
        statement,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    assertThat(cmd.getTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(AVRO, Optional.of("full.schema.name"), Optional.empty()))));
  }

  @Test
  public void shouldHandleSessionWindowedKeyForStream() {
    // Given:
    givenProperty("window_type", new StringLiteral("session"));
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = (CreateStreamCommand) commandFactories.create(
        "expression",
        statement,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    assertThat(cmd.getTopic().getKeyFormat(), is(KeyFormat.windowed(
        FormatInfo.of(KAFKA),
        WindowInfo.of(SESSION, Optional.empty()))
    ));
  }

  @Test
  public void shouldHandleTumblingWindowedKeyForStream() {
    // Given:
    givenProperties(ImmutableMap.of(
        "window_type", new StringLiteral("tumbling"),
        "window_size", new StringLiteral("1 MINUTE")
    ));
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = (CreateStreamCommand) commandFactories.create(
        "expression",
        statement,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    assertThat(cmd.getTopic().getKeyFormat(), is(KeyFormat.windowed(
        FormatInfo.of(KAFKA),
        WindowInfo.of(TUMBLING, Optional.of(Duration.ofMinutes(1))))
    ));
  }

  @Test
  public void shouldHandleHoppingWindowedKeyForStream() {
    // Given:
    givenProperties(ImmutableMap.of(
        "window_type", new StringLiteral("Hopping"),
        "window_size", new StringLiteral("2 SECONDS")
    ));
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = (CreateStreamCommand) commandFactories.create(
        "expression",
        statement,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    assertThat(cmd.getTopic().getKeyFormat(), is(KeyFormat.windowed(
        FormatInfo.of(KAFKA),
        WindowInfo.of(HOPPING, Optional.of(Duration.ofSeconds(2))))
    ));
  }

  @Test
  public void shouldCreateDropSourceOnMissingSourceWithIfExistsForStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, false, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(null);

    // When:
    final DropSourceCommand cmd = (DropSourceCommand) commandFactories.create(
        "sqlExpression",
        dropStream,
        ksqlConfig,
        emptyMap()
    );

    // Then:
    assertThat(cmd.getSourceName(), equalTo(SourceName.of("bob")));
  }

  @Test
  public void shouldFailDropSourceOnMissingSourceWithNoIfExistsForStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, true, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(null);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Source bob does not exist.");

    // When:
    commandFactories.create("sqlExpression", dropStream, ksqlConfig, emptyMap());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFailDropSourceOnDropIncompatibleSourceForStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, false, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(ksqlTable);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Incompatible data source type is TABLE");

    // When:
    commandFactories.create("sqlExpression", dropStream, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldThrowOnRowTimeValueColumn() {
    // Given:
    final DdlStatement statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(Namespace.VALUE, ROWTIME_NAME.name(), new Type(SqlTypes.BIGINT))),
        true,
        withProperties
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'ROWTIME' is a reserved column name.");

    // When:
    commandFactories.create("sqlExpression", statement, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldThrowOnRowTimeKeyColumn() {
    // Given:
    final DdlStatement statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(Namespace.KEY, ROWTIME_NAME.name(), new Type(SqlTypes.BIGINT))),
        true,
        withProperties
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'ROWTIME' is a reserved column name.");

    // When:
    commandFactories.create("sqlExpression", statement, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldThrowOnRowKeyValueColumn() {
    // Given:
    final DdlStatement statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(VALUE, ROWKEY_NAME.name(), new Type(SqlTypes.STRING))),
        true,
        withProperties
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "'ROWKEY' is a reserved column name. It can only be used for KEY columns.");

    // When:
    commandFactories.create("sqlExpression", statement, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldNotThrowOnRowKeyKeyColumn() {
    // Given:
    final DdlStatement statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(KEY, ROWKEY_NAME.name(), new Type(SqlTypes.STRING))),
        true,
        withProperties
    );

    // When:
    commandFactories.create("sqlExpression", statement, ksqlConfig, emptyMap());

    // Then: did not throw
  }

  @Test
  public void shouldThrowOnRowKeyIfNotString() {
    // Given:
    final DdlStatement statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(KEY, ROWKEY_NAME.name(), new Type(SqlTypes.INTEGER))),
        true,
        withProperties
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'ROWKEY' is a KEY column with an unsupported type. "
        + "KSQL currently only supports KEY columns of type STRING.");

    // When:
    commandFactories.create("sqlExpression", statement, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldThrowOnKeyColumnThatIsNotCalledRowKey() {
    // Given:
    final DdlStatement statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(KEY, "someKey", new Type(SqlTypes.STRING))),
        true,
        withProperties
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'someKey' is an invalid KEY column name. "
        + "KSQL currently only supports KEY columns named ROWKEY.");

    // When:
    commandFactories.create("sqlExpression", statement, ksqlConfig, emptyMap());
  }

  private void givenProperty(final String name, final Literal value) {
    givenProperties(ImmutableMap.of(name, value));
  }

  private void givenProperties(final Map<String, Literal> properties) {
    final Map<String, Literal> props = withProperties.copyOfOriginalLiterals();
    props.putAll(properties);
    withProperties = CreateSourceProperties.from(props);
  }

  private static TableElement tableElement(
      final Namespace namespace,
      final String name,
      final Type type
  ) {
    final TableElement te = mock(TableElement.class, name);
    when(te.getName()).thenReturn(ColumnName.of(name));
    when(te.getType()).thenReturn(type);
    when(te.getNamespace()).thenReturn(namespace);
    return te;
  }
}