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
import static io.confluent.ksql.schema.ksql.ColumnMatchers.keyColumn;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
import static io.confluent.ksql.serde.Format.AVRO;
import static io.confluent.ksql.serde.Format.JSON;
import static io.confluent.ksql.serde.Format.KAFKA;
import static io.confluent.ksql.util.SchemaUtil.ROWKEY_NAME;
import static io.confluent.ksql.util.SchemaUtil.ROWTIME_NAME;
import static io.confluent.ksql.util.SchemaUtil.WINDOWEND_NAME;
import static io.confluent.ksql.util.SchemaUtil.WINDOWSTART_NAME;
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
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
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
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
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
import org.apache.kafka.connect.data.Struct;
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

  private static final TableElement EXPLICIT_ROWKEY =
      tableElement(Namespace.KEY, ROWKEY_NAME.name(), new Type(SqlTypes.INTEGER));

  private static final TableElement ELEMENT1 =
      tableElement(VALUE, "bob", new Type(SqlTypes.STRING));

  private static final TableElement ELEMENT2 =
      tableElement(VALUE, "hojjat", new Type(BIGINT));

  private static final TableElements ONE_ELEMENTS = TableElements.of(ELEMENT1);

  private static final TableElements TABLE_ELEMENTS =
      TableElements.of(EXPLICIT_ROWKEY, ELEMENT1, ELEMENT2);

  private static final LogicalSchema EXPECTED_SCHEMA = LogicalSchema.builder()
      .keyColumn(ROWKEY_NAME, SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("hojjat"), BIGINT)
      .build();

  private static final String TOPIC_NAME = "some topic";

  private static final Map<String, Literal> MINIMIM_PROPS = ImmutableMap.of(
      CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"),
      CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(TOPIC_NAME)
  );

  private static final Set<SerdeOption> SOME_SERDE_OPTIONS = ImmutableSet
      .of(SerdeOption.UNWRAP_SINGLE_VALUES);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SerdeOptionsSupplier serdeOptionsSupplier;
  @Mock
  private KeySerdeFactory keySerdeFactory;
  @Mock
  private ValueSerdeFactory valueSerdeFactory;
  @Mock
  private Serde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;

  private CreateSourceFactory createSourceFactory;
  private KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
  private CreateSourceProperties withProperties =
      CreateSourceProperties.from(MINIMIM_PROPS);

  @Before
  public void before() {
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(any())).thenReturn(true);
    when(keySerdeFactory.create(any(), any(), any(), any(), any(), any())).thenReturn(keySerde);
    when(valueSerdeFactory.create(any(), any(), any(), any(), any(), any())).thenReturn(valueSerde);

    givenCommandFactories();
  }

  private void givenCommandFactories() {
    createSourceFactory = new CreateSourceFactory(serviceContext);
  }

  private void givenCommandFactoriesWithMocks() {
    createSourceFactory = new CreateSourceFactory(
        serviceContext,
        serdeOptionsSupplier,
        keySerdeFactory,
        valueSerdeFactory
    );
  }

  @Test
  public void shouldCreateCommandForCreateStream() {
    // Given:
    final CreateStream ddlStatement =
        new CreateStream(SOME_NAME, TABLE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand result = createSourceFactory
        .createStreamCommand(ddlStatement, ksqlConfig);

    // Then:
    assertThat(result.getSourceName(), is(SOME_NAME));
    assertThat(result.getTopicName(), is(TOPIC_NAME));
  }

  @Test
  public void shouldCreateCommandForCreateTable() {
    // Given:
    final CreateTable ddlStatement = new CreateTable(SOME_NAME,
        TableElements.of(
            tableElement(VALUE, "COL1", new Type(BIGINT)),
            tableElement(VALUE, "COL2", new Type(SqlTypes.STRING))),
        true, withProperties);

    // When:
    final CreateTableCommand result = createSourceFactory
        .createTableCommand(ddlStatement, ksqlConfig);

    // Then:
    assertThat(result.getSourceName(), is(SOME_NAME));
    assertThat(result.getTopicName(), is(TOPIC_NAME));
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
        new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory
        .createStreamCommand(
            statement,
            ksqlConfig.cloneWithPropertyOverwrite(overrides)
        );

    // Then:
    assertThat(cmd.getFormats().getOptions(), contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory
        .createStreamCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getFormats().getOptions(), contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromDefaultConfig() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory
        .createStreamCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getFormats().getOptions(), not(contains(SerdeOption.UNWRAP_SINGLE_VALUES)));
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
        new CreateTable(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateTableCommand cmd = createSourceFactory
        .createTableCommand(
            statement,
            ksqlConfig.cloneWithPropertyOverwrite(overrides));

    // Then:
    assertThat(cmd.getFormats().getOptions(), contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    final CreateTable statement =
        new CreateTable(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateTableCommand cmd = createSourceFactory
        .createTableCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getFormats().getOptions(), contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromDefaultConfig() {
    // Given:
    final CreateTable statement =
        new CreateTable(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateTableCommand cmd = createSourceFactory
        .createTableCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getFormats().getOptions(), not(contains(SerdeOption.UNWRAP_SINGLE_VALUES)));
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
        new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);

    // Then: not exception thrown
  }

  @Test
  public void shouldNotThrowWhenThereAreElementsInCreateTable() {
    // Given:
    final CreateTable statement =
        new CreateTable(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    createSourceFactory.createTableCommand(statement, ksqlConfig);

    // Then: not exception thrown
  }

  @Test
  public void shouldThrowIfTopicDoesNotExistForStream() {
    // Given:
    when(topicClient.isTopicExists(any())).thenReturn(false);
    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

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
        new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);

    // Then:
    verify(topicClient).isTopicExists(TOPIC_NAME);
  }

  @Test
  public void shouldThrowIfKeyFieldNotInSchemaForStream() {
    // Given:
    givenProperty(CreateConfigs.KEY_NAME_PROPERTY, new StringLiteral("`will-not-find-me`"));
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

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
        new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

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
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);
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
    assertThat(cmd.getFormats().getOptions(), is(SOME_SERDE_OPTIONS));
  }

  @Test
  public void shouldBuildTimestampColumnForStream() {
    // Given:
    givenProperty(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY,
        new StringLiteral(quote(ELEMENT2.getName().name()))
    );
    final CreateStream statement =
        new CreateStream(SOME_NAME, TABLE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(
        cmd.getTimestampColumn(),
        is(Optional.of(
            new TimestampColumn(ColumnRef.of(ELEMENT2.getName()), Optional.empty()))
        )
    );
  }

  @Test
  public void shouldBuildTimestampColumnForTable() {
    // Given:
    givenProperty(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY,
        new StringLiteral(quote(ELEMENT2.getName().name()))
    );
    final CreateTable statement =
        new CreateTable(SOME_NAME, TABLE_ELEMENTS, true, withProperties);

    // When:
    final CreateTableCommand cmd = createSourceFactory.createTableCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(
        cmd.getTimestampColumn(),
        is(Optional.of(
            new TimestampColumn(ColumnRef.of(ELEMENT2.getName()), Optional.empty()))
        )
    );
  }

  @Test
  public void shouldBuildTimestampColumnWithFormat() {
    // Given:
    givenProperties(ImmutableMap.of(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY,
        new StringLiteral(quote(ELEMENT1.getName().name())),
        CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY,
        new StringLiteral("%s")
    ));
    final CreateStream statement =
        new CreateStream(SOME_NAME, TABLE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(
        cmd.getTimestampColumn(),
        is(Optional.of(
            new TimestampColumn(ColumnRef.of(ELEMENT1.getName()), Optional.of("%s")))
        )
    );
  }

  @Test
  public void shouldBuildSchemaWithImplicitKeyFieldForStream() {
    // Given:
    final CreateStream statement = new CreateStream(SOME_NAME, TABLE_ELEMENTS, true,
        withProperties);

    // When:
    final CreateStreamCommand result = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(result.getSchema(), is(EXPECTED_SCHEMA));
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
        .valueColumn(ColumnName.of("hojjat"), BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldValidateKeyFormatCanHandleKeySchema() {
    // Given:
    givenCommandFactoriesWithMocks();
    final CreateStream statement = new CreateStream(SOME_NAME, TABLE_ELEMENTS, true,
        withProperties);

    when(keySerdeFactory.create(
        FormatInfo.of(KAFKA, Optional.empty(), Optional.empty()),
        PersistenceSchema.from(EXPECTED_SCHEMA.keyConnectSchema(), false),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    )).thenThrow(new RuntimeException("Boom!"));

    // Expect:
    expectedException.expectMessage("Boom!");

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldCreateValueSerdeToValidateValueFormatCanHandleValueSchema() {
    // Given:
    givenCommandFactoriesWithMocks();
    final CreateTable statement = new CreateTable(SOME_NAME, TABLE_ELEMENTS, true,
        withProperties);

    when(valueSerdeFactory.create(
        FormatInfo.of(JSON, Optional.empty(), Optional.empty()),
        PersistenceSchema.from(EXPECTED_SCHEMA.valueConnectSchema(), false),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    )).thenThrow(new RuntimeException("Boom!"));

    // Expect:
    expectedException.expectMessage("Boom!");

    // When:
    createSourceFactory.createTableCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldDefaultToKafkaKeySerdeForStream() {
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(cmd.getFormats().getKeyFormat(), is(FormatInfo.of(KAFKA)));
    assertThat(cmd.getWindowInfo(), is(Optional.empty()));
  }

  @Test
  public void shouldHandleValueAvroSchemaNameForStream() {
    // Given:
    givenCommandFactoriesWithMocks();
    givenProperty("VALUE_FORMAT", new StringLiteral("Avro"));
    givenProperty("value_avro_schema_full_name", new StringLiteral("full.schema.name"));
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(
        cmd.getFormats().getValueFormat(),
        is(FormatInfo.of(AVRO, Optional.of("full.schema.name"), Optional.empty())));
  }

  @Test
  public void shouldHandleSessionWindowedKeyForStream() {
    // Given:
    givenProperty("window_type", new StringLiteral("session"));
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(cmd.getFormats().getKeyFormat(), is(FormatInfo.of(KAFKA)));
    assertThat(cmd.getWindowInfo(), is(Optional.of(WindowInfo.of(SESSION, Optional.empty()))));
  }

  @Test
  public void shouldHandleTumblingWindowedKeyForStream() {
    // Given:
    givenProperties(ImmutableMap.of(
        "window_type", new StringLiteral("tumbling"),
        "window_size", new StringLiteral("1 MINUTE")
    ));
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(cmd.getFormats().getKeyFormat(), is(FormatInfo.of(KAFKA)));
    assertThat(
        cmd.getWindowInfo(),
        is(Optional.of(WindowInfo.of(TUMBLING, Optional.of(Duration.ofMinutes(1)))))
    );
  }

  @Test
  public void shouldHandleHoppingWindowedKeyForStream() {
    // Given:
    givenProperties(ImmutableMap.of(
        "window_type", new StringLiteral("Hopping"),
        "window_size", new StringLiteral("2 SECONDS")
    ));
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_ELEMENTS, true, withProperties);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(cmd.getFormats().getKeyFormat(), is(FormatInfo.of(KAFKA)));
    assertThat(
        cmd.getWindowInfo(),
        is(Optional.of(WindowInfo.of(HOPPING, Optional.of(Duration.ofSeconds(2)))))
    );
  }

  @Test
  public void shouldThrowOnRowTimeValueColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(VALUE, ROWTIME_NAME.name(), new Type(BIGINT))),
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
        TableElements.of(tableElement(Namespace.KEY, ROWTIME_NAME.name(), new Type(BIGINT))),
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
  public void shouldThrowOnWindowStartValueColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(VALUE, WINDOWSTART_NAME.name(), new Type(BIGINT))),
        true,
        withProperties
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'WINDOWSTART' is a reserved column name.");

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);
  }

  @Test
  public void shouldThrowOnWindowEndValueColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(VALUE, WINDOWEND_NAME.name(), new Type(BIGINT))),
        true,
        withProperties
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("'WINDOWEND' is a reserved column name.");

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
  public void shouldAllowNonStringKeyColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(KEY, ROWKEY_NAME.name(), new Type(SqlTypes.INTEGER))),
        true,
        withProperties
    );

    // When:
    final CreateStreamCommand cmd = createSourceFactory
        .createStreamCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getSchema().key(), contains(
       keyColumn(ROWKEY_NAME, SqlTypes.INTEGER)
    ));
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

  private static String quote(final String identifier) {
    return String.format("`%s`", identifier);
  }
}
