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
import static io.confluent.ksql.schema.ksql.ColumnMatchers.keyColumn;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWTIME_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.WINDOWEND_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.WINDOWSTART_NAME;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
import static io.confluent.ksql.serde.FormatFactory.AVRO;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.ddl.commands.CreateSourceFactory.SerdeFeaturessSupplier;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.properties.with.SourcePropertiesUtil;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateSourceFactoryTest {
  private static final ColumnConstraints KEY_CONSTRAINT =
      new ColumnConstraints.Builder().key().build();

  private static final ColumnConstraints PRIMARY_KEY_CONSTRAINT =
      new ColumnConstraints.Builder().primaryKey().build();

  private static final SourceName SOME_NAME = SourceName.of("bob");

  private static final SourceName TABLE_NAME = SourceName.of("table_bob");

  private static final TableElement EXPLICIT_KEY =
      tableElement("k", new Type(SqlTypes.INTEGER), KEY_CONSTRAINT);

  private static final TableElement EXPLICIT_PRIMARY_KEY =
      tableElement("k", new Type(SqlTypes.INTEGER), PRIMARY_KEY_CONSTRAINT);

  private static final TableElement ELEMENT1 =
      tableElement("bob", new Type(SqlTypes.STRING));

  private static final TableElement ELEMENT2 =
      tableElement("hojjat", new Type(BIGINT));

  private static final TableElements ONE_KEY_ONE_VALUE = TableElements.of(
      EXPLICIT_KEY,
      ELEMENT1
  );

  private static final TableElements TABLE_ELEMENTS_1_VALUE =
      TableElements.of(EXPLICIT_PRIMARY_KEY, ELEMENT1);

  private static final TableElements TABLE_ELEMENTS =
      TableElements.of(EXPLICIT_PRIMARY_KEY, ELEMENT1, ELEMENT2);

  private static final TableElements STREAM_ELEMENTS =
      TableElements.of(EXPLICIT_KEY, ELEMENT1, ELEMENT2);

  private static final LogicalSchema EXPECTED_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("hojjat"), BIGINT)
      .build();

  private static final String TOPIC_NAME = "some topic";

  private static final Map<String, Literal> MINIMUM_PROPS = ImmutableMap.of(
      CommonCreateConfigs.KEY_FORMAT_PROPERTY, new StringLiteral("KAFKA"),
      CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"),
      CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(TOPIC_NAME)
  );

  private static final TimestampColumn TIMESTAMP_COLUMN =
      new TimestampColumn(ColumnName.of("TS"), Optional.empty());

  private static final KeyFormat SOME_KEY_FORMAT = KeyFormat.of(FormatInfo.of("JSON"),
      SerdeFeatures.of(), Optional.empty());
  private static final ValueFormat SOME_VALUE_FORMAT = ValueFormat.of(FormatInfo.of("JSON"),
      SerdeFeatures.of());

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SerdeFeaturessSupplier keyOptionsSupplier;
  @Mock
  private SerdeFeaturessSupplier valOptionsSupplier;
  @Mock
  private KeySerdeFactory keySerdeFactory;
  @Mock
  private ValueSerdeFactory valueSerdeFactory;
  @Mock
  private Serde<GenericKey> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private MetaStore metaStore;
  @Mock
  KsqlStream ksqlStream;
  @Mock
  KsqlTable ksqlTable;

  private CreateSourceFactory createSourceFactory;
  private KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
  private CreateSourceProperties withProperties =
      CreateSourceProperties.from(MINIMUM_PROPS);

  @Before
  public void before() {
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(any())).thenReturn(true);
    when(keySerdeFactory.create(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(keySerde);
    when(valueSerdeFactory.create(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(valueSerde);
    when(keyOptionsSupplier.build(any(), any(), any(), any()))
        .thenReturn(SerdeFeatures.of());
    when(valOptionsSupplier.build(any(), any(), any(), any()))
        .thenReturn(SerdeFeatures.of());
    when(metaStore.getSource(SOME_NAME)).thenReturn(ksqlStream);
    when(ksqlStream.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(metaStore.getSource(TABLE_NAME)).thenReturn(ksqlTable);
    when(ksqlTable.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    givenCommandFactories();
  }

  private void givenCommandFactories() {
    createSourceFactory = new CreateSourceFactory(serviceContext,metaStore);
  }

  private void givenCommandFactoriesWithMocks() {
    createSourceFactory = new CreateSourceFactory(
        serviceContext,
        keyOptionsSupplier,
        valOptionsSupplier,
        keySerdeFactory,
        valueSerdeFactory,
        metaStore
    );
  }

  @Test
  public void shouldCreateStreamCommandFromNodeOutput() {
    // Given:
    final KsqlTopic ksqlTopic = mock(KsqlTopic.class);
    when(ksqlTopic.getKafkaTopicName()).thenReturn(TOPIC_NAME);
    when(ksqlTopic.getKeyFormat()).thenReturn(SOME_KEY_FORMAT);
    when(ksqlTopic.getValueFormat()).thenReturn(SOME_VALUE_FORMAT);

    final KsqlStructuredDataOutputNode outputNode = mock(KsqlStructuredDataOutputNode.class);
    when(outputNode.getSinkName()).thenReturn(Optional.of(SOME_NAME));
    when(outputNode.getSchema()).thenReturn(EXPECTED_SCHEMA);
    when(outputNode.getTimestampColumn()).thenReturn(Optional.of(TIMESTAMP_COLUMN));
    when(outputNode.getKsqlTopic()).thenReturn(ksqlTopic);


    // When:
    final CreateStreamCommand result = createSourceFactory.createStreamCommand(outputNode);

    // Then:
    assertThat(result.getSourceName(), is(SOME_NAME));
    assertThat(result.getSchema(), is(EXPECTED_SCHEMA));
    assertThat(result.getTimestampColumn(), is(Optional.of(TIMESTAMP_COLUMN)));
    assertThat(result.getTopicName(), is(TOPIC_NAME));
    assertThat(result.getFormats(), is(Formats.from(ksqlTopic)));
    assertThat(result.getWindowInfo(), is(Optional.empty()));
    assertThat(result.isOrReplace(), is(false));
  }

  @Test
  public void shouldCreateCommandForCreateStream() {
    // Given:
    final CreateStream ddlStatement =
        new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true, withProperties, false);

    // When:
    final CreateStreamCommand result = createSourceFactory
        .createStreamCommand(ddlStatement, ksqlConfig);

    // Then:
    assertThat(result.getSourceName(), is(SOME_NAME));
    assertThat(result.getTopicName(), is(TOPIC_NAME));
    assertThat(result.getIsSource(), is(false));
  }

  @Test
  public void shouldCreateCommandForCreateSourceStream() {
    // Given:
    final CreateStream ddlStatement =
        new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true, withProperties, true);

    // When:
    final CreateStreamCommand result = createSourceFactory
        .createStreamCommand(ddlStatement, ksqlConfig);

    // Then:
    assertThat(result.getSourceName(), is(SOME_NAME));
    assertThat(result.getTopicName(), is(TOPIC_NAME));
    assertThat(result.getIsSource(), is(true));
  }

  @Test
  public void shouldCreateTableCommandFromNodeOutput() {
// Given:
    final KsqlTopic ksqlTopic = mock(KsqlTopic.class);
    when(ksqlTopic.getKafkaTopicName()).thenReturn(TOPIC_NAME);
    when(ksqlTopic.getKeyFormat()).thenReturn(SOME_KEY_FORMAT);
    when(ksqlTopic.getValueFormat()).thenReturn(SOME_VALUE_FORMAT);

    final KsqlStructuredDataOutputNode outputNode = mock(KsqlStructuredDataOutputNode.class);
    when(outputNode.getSinkName()).thenReturn(Optional.of(SOME_NAME));
    when(outputNode.getSchema()).thenReturn(EXPECTED_SCHEMA);
    when(outputNode.getTimestampColumn()).thenReturn(Optional.of(TIMESTAMP_COLUMN));
    when(outputNode.getKsqlTopic()).thenReturn(ksqlTopic);


    // When:
    final CreateTableCommand result = createSourceFactory.createTableCommand(outputNode, Optional.empty());

    // Then:
    assertThat(result.getSourceName(), is(SOME_NAME));
    assertThat(result.getSchema(), is(EXPECTED_SCHEMA));
    assertThat(result.getTimestampColumn(), is(Optional.of(TIMESTAMP_COLUMN)));
    assertThat(result.getTopicName(), is(TOPIC_NAME));
    assertThat(result.getFormats(), is(Formats.from(ksqlTopic)));
    assertThat(result.getWindowInfo(), is(Optional.empty()));
    assertThat(result.isOrReplace(), is(false));
  }

  @Test
  public void shouldCreateCommandForCreateTable() {
    // Given:
    final CreateTable ddlStatement = new CreateTable(SOME_NAME,
        TableElements.of(
            tableElement("COL1", new Type(BIGINT), PRIMARY_KEY_CONSTRAINT),
            tableElement("COL2", new Type(SqlTypes.STRING))),
        false, true, withProperties, false);

    // When:
    final CreateTableCommand result = createSourceFactory
        .createTableCommand(ddlStatement, ksqlConfig);

    // Then:
    assertThat(result.getSourceName(), is(SOME_NAME));
    assertThat(result.getTopicName(), is(TOPIC_NAME));
    assertThat(result.getIsSource(), is(false));
  }

  @Test
  public void shouldCreateCommandForCreateSourceTable() {
    // Given:
    final CreateTable ddlStatement = new CreateTable(SOME_NAME,
        TableElements.of(
            tableElement("COL1", new Type(BIGINT), PRIMARY_KEY_CONSTRAINT),
            tableElement("COL2", new Type(SqlTypes.STRING))),
        false, true, withProperties, true);

    // When:
    final CreateTableCommand result = createSourceFactory
        .createTableCommand(ddlStatement, ksqlConfig);

    // Then:
    assertThat(result.getSourceName(), is(SOME_NAME));
    assertThat(result.getTopicName(), is(TOPIC_NAME));
    assertThat(result.getIsSource(), is(true));
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
        new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true, withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory
        .createStreamCommand(
            statement,
            ksqlConfig.cloneWithPropertyOverwrite(overrides)
        );

    // Then:
    assertThat(cmd.getFormats().getValueFeatures(),
        is(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true, withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory
        .createStreamCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getFormats().getValueFeatures(),
        is(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromDefaultConfig() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true, withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory
        .createStreamCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getFormats().getValueFeatures(), is(SerdeFeatures.of()));
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
        new CreateTable(SOME_NAME, TABLE_ELEMENTS_1_VALUE,
            false, true, withProperties, false);

    // When:
    final CreateTableCommand cmd = createSourceFactory
        .createTableCommand(
            statement,
            ksqlConfig.cloneWithPropertyOverwrite(overrides));

    // Then:
    assertThat(cmd.getFormats().getValueFeatures(),
        is(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromConfig() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    final CreateTable statement =
        new CreateTable(SOME_NAME, TABLE_ELEMENTS_1_VALUE,
            false, true, withProperties, false);

    // When:
    final CreateTableCommand cmd = createSourceFactory
        .createTableCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getFormats().getValueFeatures(),
        is(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromDefaultConfig() {
    // Given:
    final CreateTable statement =
        new CreateTable(SOME_NAME, TABLE_ELEMENTS_1_VALUE,
            false, true, withProperties, false);

    // When:
    final CreateTableCommand cmd = createSourceFactory
        .createTableCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getFormats().getValueFeatures(), is(SerdeFeatures.of()));
  }

  @Test
  public void shouldThrowOnNoElementsInCreateStream() {
    // Given:
    final CreateStream statement
        = new CreateStream(SOME_NAME, TableElements.of(), false, true, withProperties, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createSourceFactory.createStreamCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The statement does not define any columns."));
  }

  @Test
  public void shouldThrowOnNoElementsInCreateTable() {
    // Given:
    final CreateTable statement
        = new CreateTable(SOME_NAME, TableElements.of(),
        false, true, withProperties, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createSourceFactory.createTableCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The statement does not define any columns."));
  }

  @Test
  public void shouldNotThrowWhenThereAreElementsInCreateStream() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true, withProperties, false);

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);

    // Then: not exception thrown
  }

  @Test
  public void shouldNotThrowWhenThereAreElementsInCreateTable() {
    // Given:
    final CreateTable statement =
        new CreateTable(SOME_NAME, TABLE_ELEMENTS_1_VALUE,
            false, true, withProperties, false);

    // When:
    createSourceFactory.createTableCommand(statement, ksqlConfig);

    // Then: not exception thrown
  }

  @Test
  public void shouldThrowIfTopicDoesNotExistForStream() {
    // Given:
    when(topicClient.isTopicExists(any())).thenReturn(false);
    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true, withProperties, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createSourceFactory.createStreamCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Kafka topic does not exist: " + TOPIC_NAME));
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldNotThrowIfTopicDoesExist() {
    // Given:
    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true, withProperties, false);

    // When:
    createSourceFactory.createStreamCommand(statement, ksqlConfig);

    // Then:
    verify(topicClient).isTopicExists(TOPIC_NAME);
  }

  @Test
  public void shouldThrowIfTimestampColumnDoesNotExistForStream() {
    // Given:
    givenProperty(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY,
        new StringLiteral("`will-not-find-me`")
    );
    final CreateStream statement =
        new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true, withProperties, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createSourceFactory.createStreamCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The TIMESTAMP column set in the WITH clause does not exist in the schema: "
            + "'will-not-find-me'"));
  }

  @Test
  public void shouldBuildSerdeFeaturesForStream() {
    // Given:
    givenCommandFactoriesWithMocks();
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true,
        withProperties, false);
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
        .build();
    when(keyOptionsSupplier.build(any(), any(), any(), any()))
        .thenReturn(SerdeFeatures.of(SerdeFeature.WRAP_SINGLES));
    when(valOptionsSupplier.build(any(), any(), any(), any()))
        .thenReturn(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    verify(keyOptionsSupplier).build(
        schema,
        FormatFactory.of(SourcePropertiesUtil.getKeyFormat(statement.getProperties(), SOME_NAME)),
        SerdeFeatures.of(),
        ksqlConfig
    );
    verify(valOptionsSupplier).build(
        schema,
        FormatFactory.of(SourcePropertiesUtil.getValueFormat(statement.getProperties())),
        statement.getProperties().getValueSerdeFeatures(),
        ksqlConfig
    );
    assertThat(cmd.getFormats().getKeyFeatures(), is(SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)));
    assertThat(cmd.getFormats().getValueFeatures(),
        is(SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldBuildTimestampColumnForStream() {
    // Given:
    givenProperty(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY,
        new StringLiteral(quote(ELEMENT2.getName().text()))
    );
    final CreateStream statement =
        new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true, withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(
        cmd.getTimestampColumn(),
        is(Optional.of(
            new TimestampColumn(ELEMENT2.getName(), Optional.empty()))
        )
    );
  }

  @Test
  public void shouldBuildTimestampColumnForTable() {
    // Given:
    givenProperty(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY,
        new StringLiteral(quote(ELEMENT2.getName().text()))
    );
    final CreateTable statement =
        new CreateTable(SOME_NAME, TABLE_ELEMENTS,
            false, true, withProperties, false);

    // When:
    final CreateTableCommand cmd = createSourceFactory.createTableCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(
        cmd.getTimestampColumn(),
        is(Optional.of(
            new TimestampColumn(ELEMENT2.getName(), Optional.empty()))
        )
    );
  }

  @Test
  public void shouldBuildTimestampColumnWithFormat() {
    // Given:
    givenProperties(ImmutableMap.of(
        CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY,
        new StringLiteral(quote(ELEMENT1.getName().text())),
        CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY,
        new StringLiteral("%s")
    ));
    final CreateStream statement =
        new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true, withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(
        cmd.getTimestampColumn(),
        is(Optional.of(
            new TimestampColumn(ELEMENT1.getName(), Optional.of("%s")))
        )
    );
  }

  @Test
  public void shouldBuildSchemaWithImplicitKeyFieldForStream() {
    // Given:
    final CreateStream statement = new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true,
        withProperties, false);

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
            tableElement("k", new Type(SqlTypes.STRING), KEY_CONSTRAINT),
            ELEMENT1,
            ELEMENT2
        ),
        false,
        true,
        withProperties,
        false
    );

    // When:
    final CreateStreamCommand result = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(result.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("k"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("hojjat"), BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldValidateKeyFormatCanHandleKeySchema() {
    // Given:
    givenCommandFactoriesWithMocks();
    final CreateStream statement = new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true,
        withProperties, false);

    when(keySerdeFactory.create(
        FormatInfo.of(KAFKA.name()),
        PersistenceSchema.from(EXPECTED_SCHEMA.key(), SerdeFeatures.of()),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    )).thenThrow(new RuntimeException("Boom!"));

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> createSourceFactory.createStreamCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Boom!"));
  }

  @Test
  public void shouldCreateValueSerdeToValidateValueFormatCanHandleValueSchema() {
    // Given:
    givenCommandFactoriesWithMocks();
    final CreateTable statement = new CreateTable(SOME_NAME, TABLE_ELEMENTS, false, true,
        withProperties, false);

    when(valueSerdeFactory.create(
        FormatInfo.of(JSON.name()),
        PersistenceSchema.from(EXPECTED_SCHEMA.value(), SerdeFeatures.of()),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    )).thenThrow(new RuntimeException("Boom!"));

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> createSourceFactory.createTableCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Boom!"));
  }

  @Test
  public void shouldDefaultToKafkaKeySerdeForStream() {
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true,
        withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(cmd.getFormats().getKeyFormat(), is(FormatInfo.of(KAFKA.name())));
    assertThat(cmd.getWindowInfo(), is(Optional.empty()));
  }

  @Test
  public void shouldHandleValueAvroSchemaNameForStream() {
    // Given:
    givenCommandFactoriesWithMocks();
    givenProperty("VALUE_FORMAT", new StringLiteral("Avro"));
    givenProperty("value_avro_schema_full_name", new StringLiteral("full.schema.name"));
    final CreateStream statement = new CreateStream(SOME_NAME, ONE_KEY_ONE_VALUE, false, true,
        withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(
        cmd.getFormats().getValueFormat(),
        is(FormatInfo.of(AVRO.name(), ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "full.schema.name"))));
  }

  @Test
  public void shouldHandleSessionWindowedKeyForStream() {
    // Given:
    givenProperty("window_type", new StringLiteral("session"));
    final CreateStream statement = new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true, withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(cmd.getFormats().getKeyFormat(), is(FormatInfo.of(KAFKA.name())));
    assertThat(cmd.getWindowInfo(), is(Optional.of(WindowInfo.of(SESSION, Optional.empty(), Optional.empty()))));
  }

  @Test
  public void shouldHandleTumblingWindowedKeyForStream() {
    // Given:
    givenProperties(ImmutableMap.of(
        "window_type", new StringLiteral("tumbling"),
        "window_size", new StringLiteral("1 MINUTE")
    ));
    final CreateStream statement = new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true, withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(cmd.getFormats().getKeyFormat(), is(FormatInfo.of(KAFKA.name())));
    assertThat(
        cmd.getWindowInfo(),
        is(Optional.of(WindowInfo.of(TUMBLING, Optional.of(Duration.ofMinutes(1)), Optional.empty())))
    );
  }

  @Test
  public void shouldHandleHoppingWindowedKeyForStream() {
    // Given:
    givenProperties(ImmutableMap.of(
        "window_type", new StringLiteral("Hopping"),
        "window_size", new StringLiteral("2 SECONDS")
    ));
    final CreateStream statement = new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true, withProperties, false);

    // When:
    final CreateStreamCommand cmd = createSourceFactory.createStreamCommand(
        statement,
        ksqlConfig
    );

    // Then:
    assertThat(cmd.getFormats().getKeyFormat(), is(FormatInfo.of(KAFKA.name())));
    assertThat(
        cmd.getWindowInfo(),
        is(Optional.of(WindowInfo.of(HOPPING, Optional.of(Duration.ofSeconds(2)), Optional.empty())))
    );
  }

  @Test
  public void shouldThrowOnRowTimeValueColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(ROWTIME_NAME.text(), new Type(BIGINT))),
        false,
        true,
        withProperties,
        false
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createSourceFactory.createStreamCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "'ROWTIME' is a reserved column name."));
  }

  @Test
  public void shouldThrowOnRowTimeKeyColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(ROWTIME_NAME.text(), new Type(BIGINT), KEY_CONSTRAINT)),
        false,
        true,
        withProperties,
        false
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createSourceFactory.createStreamCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "'ROWTIME' is a reserved column name."));
  }

  @Test
  public void shouldThrowOnWindowStartValueColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(WINDOWSTART_NAME.text(), new Type(BIGINT))),
        false,
        true,
        withProperties,
        false
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createSourceFactory.createStreamCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "'WINDOWSTART' is a reserved column name."));
  }

  @Test
  public void shouldThrowOnWindowEndValueColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement(WINDOWEND_NAME.text(), new Type(BIGINT))),
        false,
        true,
        withProperties,
        false
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createSourceFactory.createStreamCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "'WINDOWEND' is a reserved column name."));
  }

  @Test
  public void shouldNotThrowOnRowKeyKeyColumn() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement("k", new Type(SqlTypes.STRING), KEY_CONSTRAINT)),
        false,
        true,
        withProperties,
        false
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
        TableElements.of(tableElement("k", new Type(SqlTypes.INTEGER), KEY_CONSTRAINT)),
        false,
        true,
        withProperties,
        false
    );

    // When:
    final CreateStreamCommand cmd = createSourceFactory
        .createStreamCommand(statement, ksqlConfig);

    // Then:
    assertThat(cmd.getSchema().key(), contains(
        keyColumn(ColumnName.of("k"), SqlTypes.INTEGER)
    ));
  }

  @Test
  public void shouldNotThrowOnKeyColumnThatIsNotCalledRowKey() {
    // Given:
    final CreateStream statement = new CreateStream(
        SOME_NAME,
        TableElements.of(tableElement("someKey", new Type(SqlTypes.STRING), KEY_CONSTRAINT)),
        false,
        true,
        withProperties,
        false
    );

    // When:
    final CreateStreamCommand result = createSourceFactory
        .createStreamCommand(statement, ksqlConfig);

    // Then:
    assertThat(result.getSchema().key(), contains(
        keyColumn(ColumnName.of("someKey"), SqlTypes.STRING)
    ));
  }

  @Test
  public void shouldThrowIfTableIsMissingPrimaryKey() {
    // Given:
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false
    ));

    final TableElements noKey = TableElements.of(ELEMENT1);

    final CreateTable statement =
        new CreateTable(SOME_NAME, noKey,
            false, true, withProperties, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createSourceFactory.createTableCommand(statement, ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Tables require a PRIMARY KEY. Please define the PRIMARY KEY."));
  }

  @Test
  public void shouldNotThrowOnCreateStreamIfNotExistsIsSet() {
    // Given:
    final CreateStream ddlStatement =
        new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, true, withProperties, false);

    // When:
    final CreateStreamCommand result = createSourceFactory
        .createStreamCommand(ddlStatement, ksqlConfig);

    // Then:
    assertThat(result.getSourceName(), is(SOME_NAME));
  }

  @Test
  public void shouldThrowIfStreamExists() {
    // Given:
    final CreateStream ddlStatement =
        new CreateStream(SOME_NAME, STREAM_ELEMENTS, false, false, withProperties, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class, () -> createSourceFactory
            .createStreamCommand(ddlStatement, ksqlConfig));

    // Then:
    assertThat(e.getMessage(),
        containsString("Cannot add stream 'bob': A stream with the same name already exists"));
  }

  @Test
  public void shouldNotThrowOnCreateTableIfNotExistsIsSet() {
    //Given
    final CreateTable ddlStatement = new CreateTable(TABLE_NAME,
        TableElements.of(
            tableElement("COL1", new Type(BIGINT), PRIMARY_KEY_CONSTRAINT),
            tableElement("COL2", new Type(SqlTypes.STRING))),
        false, true, withProperties, false);

    // When:
    final CreateTableCommand result = createSourceFactory
        .createTableCommand(ddlStatement, ksqlConfig);

    // Then:
    assertThat(result.getSourceName(), is(TABLE_NAME));
  }

  @Test
  public void shouldThrowIfTableExists() {
    //Given
    final CreateTable ddlStatement = new CreateTable(TABLE_NAME,
        TableElements.of(
            tableElement("COL1", new Type(BIGINT), PRIMARY_KEY_CONSTRAINT),
            tableElement("COL2", new Type(SqlTypes.STRING))),
        false, false, withProperties, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class, () -> createSourceFactory
            .createTableCommand(ddlStatement, ksqlConfig));

    // Then:
    assertThat(e.getMessage(),
        containsString("Cannot add table 'table_bob': A table with the same name already exists"));
  }

  @Test
  public void shouldThrowInCreateStreamOrReplaceSource() {
    // Given:
    final CreateStream ddlStatement =
        new CreateStream(SOME_NAME, STREAM_ELEMENTS, true, false, withProperties, true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class, () -> createSourceFactory
            .createStreamCommand(ddlStatement, ksqlConfig));

    // Then:
    assertThat(e.getMessage(),
        containsString(
            "Cannot add stream 'bob': CREATE OR REPLACE is not supported on "
                + "source streams."));
  }

  @Test
  public void shouldThrowInCreateStreamOrReplaceOnSourceStreams() {
    // Given:
    final SourceName existingStreamName = SourceName.of("existingStreamName");
    final KsqlStream existingStream = mock(KsqlStream.class);

    when(existingStream.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(existingStream.isSource()).thenReturn(true);
    when(metaStore.getSource(existingStreamName)).thenReturn(existingStream);

    final CreateStream ddlStatement =
        new CreateStream(existingStreamName,
            STREAM_ELEMENTS, true, false, withProperties, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class, () -> createSourceFactory
            .createStreamCommand(ddlStatement, ksqlConfig));

    // Then:
    assertThat(e.getMessage(),
        containsString(
            "Cannot add stream 'existingStreamName': CREATE OR REPLACE is not supported on "
                + "source streams."));
  }

  @Test
  public void shouldThrowInCreateTableOrReplaceSource() {
    // Given:
    final CreateTable ddlStatement = new CreateTable(TABLE_NAME,
        TableElements.of(
            tableElement("COL1", new Type(BIGINT), PRIMARY_KEY_CONSTRAINT),
            tableElement("COL2", new Type(SqlTypes.STRING))),
        true, false, withProperties, true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class, () -> createSourceFactory
            .createTableCommand(ddlStatement, ksqlConfig));

    // Then:
    assertThat(e.getMessage(),
        containsString(
            "Cannot add table 'table_bob': CREATE OR REPLACE is not supported on "
                + "source tables."));
  }

  @Test
  public void shouldThrowInCreateStreamOrReplaceOnSourceTables() {
    // Given:
    final SourceName existingTableName = SourceName.of("existingTableName");
    final KsqlTable existingTable = mock(KsqlTable.class);

    when(existingTable.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(existingTable.isSource()).thenReturn(true);
    when(metaStore.getSource(existingTableName)).thenReturn(existingTable);

    final CreateTable ddlStatement = new CreateTable(existingTableName,
        TableElements.of(
            tableElement("COL1", new Type(BIGINT), PRIMARY_KEY_CONSTRAINT),
            tableElement("COL2", new Type(SqlTypes.STRING))),
        true, false, withProperties, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class, () -> createSourceFactory
            .createTableCommand(ddlStatement, ksqlConfig));

    // Then:
    assertThat(e.getMessage(),
        containsString(
            "Cannot add table 'existingTableName': CREATE OR REPLACE is not supported on "
                + "source tables."));
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

  private static String quote(final String identifier) {
    return String.format("`%s`", identifier);
  }
}
