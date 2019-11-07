/*
 * Copyright 2019 Confluent Inc.
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
import io.confluent.ksql.ddl.commands.CreateSourceFactory.SerdeOptionsSupplier;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
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
public class CreateSourceFactoryTest {
  private static final SourceName SOME_NAME = SourceName.of("bob");
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

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SerdeOptionsSupplier serdeOptionsSupplier;
  @Mock
  private ValueSerdeFactory serdeFactory;
  @Mock
  private Serde<GenericRow> serde;

  private CreateSourceFactory createSourceFactory;
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
    when(serdeFactory.create(any(), any(), any(), any(), any(), any())).thenReturn(serde);

    givenCommandFactories();
  }

  private void givenCommandFactories() {
    createSourceFactory = new CreateSourceFactory(serviceContext);
  }

  private void givenCommandFactoriesWithMocks() {
    createSourceFactory = new CreateSourceFactory(
        serviceContext,
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
    final DdlCommand result = createSourceFactory
        .createStreamCommand(ddlStatement, ksqlConfig);

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
    final DdlCommand result = createSourceFactory
        .createTableCommand(ddlStatement, ksqlConfig);

    // Then:
    assertThat(result, instanceOf(CreateTableCommand.class));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromPropertiesNotConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    );

    givenProperty(CommonCreateConfigs.WRAP_SINGLE_VALUE, new BooleanLiteral("false"));

    final CreateStream statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = createSourceFactory
        .createStreamCommand(
            statement,
            ksqlConfig.cloneWithPropertyOverwrite(overrides)
        );

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

    final CreateStream statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = createSourceFactory
        .createStreamCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd, is(instanceOf(CreateStreamCommand.class)));
    assertThat(((CreateStreamCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromDefaultConfig() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = createSourceFactory
        .createStreamCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd, is(instanceOf(CreateStreamCommand.class)));
    assertThat(((CreateStreamCommand) cmd).getSerdeOptions(),
        not(contains(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromPropertiesNotConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true
    );

    givenProperty(CommonCreateConfigs.WRAP_SINGLE_VALUE, new BooleanLiteral("false"));

    final CreateTable statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = createSourceFactory
        .createTableCommand(
            statement,
            ksqlConfig.cloneWithPropertyOverwrite(overrides));

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

    final CreateTable statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = createSourceFactory
        .createTableCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd, is(instanceOf(CreateTableCommand.class)));
    assertThat(((CreateTableCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromDefaultConfig() {
    // Given:
    final CreateTable statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = createSourceFactory
        .createTableCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd, is(instanceOf(CreateTableCommand.class)));
    assertThat(((CreateTableCommand) cmd).getSerdeOptions(),
        not(contains(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldThrowOnNoElementsInCreateStream() {
    // Given:
    final CreateStream statement
        = new CreateStream(SOME_NAME, TableElements.of(), true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The statement does not define any columns.");

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldThrowOnNoElementsInCreateTable() {
    // Given:
    final CreateTable statement
        = new CreateTable(SOME_NAME, TableElements.of(), true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The statement does not define any columns.");

    // When:
    createSourceFactory.createTableCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldNotThrowWhenThereAreElementsInCreateStream() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);

    // Then: not exception thrown
  }

  @Test
  public void shouldNotThrowWhenThereAreElementsInCreateTable() {
    // Given:
    final CreateTable statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    createSourceFactory.createTableCommand(statement, ksqlConfig);

    // Then: not exception thrown
  }

  @Test
  public void shouldThrowIfTopicDoesNotExistForStream() {
    // Given:
    when(topicClient.isTopicExists(any())).thenReturn(false);
    final CreateStream statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Kafka topic does not exist: " + TOPIC_NAME);

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldNotThrowIfTopicDoesExist() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);

    // Then:
    verify(topicClient).isTopicExists(TOPIC_NAME);
  }

  @Test
  public void shouldThrowIfKeyFieldNotInSchemaForStream() {
    // Given:
    givenProperty(CreateConfigs.KEY_NAME_PROPERTY, new StringLiteral("`will-not-find-me`"));
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The KEY column set in the WITH clause does not exist in the schema: "
            + "'will-not-find-me'");

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldThrowIfTimestampColumnDoesNotExistForStream() {
    // Given:
    givenProperty(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY,
        new StringLiteral("`will-not-find-me`")
    );
    final CreateStream statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The TIMESTAMP column set in the WITH clause does not exist in the schema: "
            + "'will-not-find-me'");

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
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
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
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
    final CreateStreamCommand result = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
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
    final CreateStreamCommand result = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
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
    createSourceFactory.createStreamCommand(statement, ksqlConfig);

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
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
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
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
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
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
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
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
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
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(cmd.getTopic().getKeyFormat(), is(KeyFormat.windowed(
        FormatInfo.of(KAFKA),
        WindowInfo.of(HOPPING, Optional.of(Duration.ofSeconds(2))))
    ));
  }

  @Test
  public void shouldThrowOnRowTimeValueColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(Namespace.VALUE, ROWTIME_NAME.name(), new Type(SqlTypes.BIGINT))),
        true,
        withProperties
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'ROWTIME' is a reserved column name.");

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldThrowOnRowTimeKeyColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(Namespace.KEY, ROWTIME_NAME.name(), new Type(SqlTypes.BIGINT))),
        true,
        withProperties
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'ROWTIME' is a reserved column name.");

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldThrowOnRowKeyValueColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
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
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldNotThrowOnRowKeyKeyColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(KEY, ROWKEY_NAME.name(), new Type(SqlTypes.STRING))),
        true,
        withProperties
    );

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);

    // Then: did not throw
  }

  @Test
  public void shouldThrowOnRowKeyIfNotString() {
    // Given:
    final CreateStream statement = new CreateStream(
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
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldThrowOnKeyColumnThatIsNotCalledRowKey() {
    // Given:
    final CreateStream statement = new CreateStream(
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
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
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
