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

package io.confluent.ksql.schema.ksql.inference;

import static io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId.schemaAndId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.connect.ConnectFormat;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.services.KafkaConsumerGroupClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSchemaInjectorTest {
  private static final ColumnConstraints KEY_CONSTRAINT =
      new ColumnConstraints.Builder().key().build();

  private static final ColumnConstraints PRIMARY_KEY_CONSTRAINT =
      new ColumnConstraints.Builder().primaryKey().build();

  private static final ColumnConstraints HEADER_CONSTRAINT =
      new ColumnConstraints.Builder().header("header").build();

  private static final TableElements SOME_KEY_ELEMENTS_STREAM = TableElements.of(
      new TableElement(ColumnName.of("bob"), new Type(SqlTypes.STRING), KEY_CONSTRAINT));
  private static final TableElements HEADER_ELEMENTS = TableElements.of(
      new TableElement(ColumnName.of("head"), new Type(SqlTypes.BYTES), HEADER_CONSTRAINT));
  private static final TableElements HEADER_AND_VALUE = TableElements.of(
      new TableElement(ColumnName.of("head"), new Type(SqlTypes.BYTES), HEADER_CONSTRAINT),
      new TableElement(ColumnName.of("bob"), new Type(SqlTypes.STRING)));
  private static final TableElements SOME_KEY_ELEMENTS_TABLE = TableElements.of(
      new TableElement(ColumnName.of("bob"), new Type(SqlTypes.STRING), PRIMARY_KEY_CONSTRAINT));
  private static final TableElements SOME_VALUE_ELEMENTS = TableElements.of(
      new TableElement(ColumnName.of("bob"), new Type(SqlTypes.STRING)));
  private static final TableElements SOME_KEY_AND_VALUE_ELEMENTS_STREAM = TableElements.of(
      new TableElement(ColumnName.of("k"), new Type(SqlTypes.STRING), KEY_CONSTRAINT),
      new TableElement(ColumnName.of("bob"), new Type(SqlTypes.STRING)));
  private static final TableElements SOME_KEY_AND_VALUE_ELEMENTS_TABLE = TableElements.of(
      new TableElement(ColumnName.of("k"), new Type(SqlTypes.STRING), PRIMARY_KEY_CONSTRAINT),
      new TableElement(ColumnName.of("bob"), new Type(SqlTypes.STRING)));
  private static final String KAFKA_TOPIC = "some-topic";
  private static final Map<String, Literal> BASE_PROPS = ImmutableMap.of(
      "KAFKA_TOPIC", new StringLiteral(KAFKA_TOPIC)
  );

  private static final String SQL_TEXT = "Some SQL";

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("key"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("intField"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("bigIntField"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("doubleField"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("stringField"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("booleanField"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("arrayField"), SqlTypes.array(SqlTypes.INTEGER))
      .valueColumn(ColumnName.of("mapField"), SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT))
      .valueColumn(ColumnName.of("structField"), SqlTypes.struct()
          .field("s0", SqlTypes.BIGINT).build())
      .valueColumn(ColumnName.of("decimalField"), SqlTypes.decimal(4, 2))
      .build();

  private static final LogicalSchema LOGICAL_SCHEMA_EXTRA_KEY = LogicalSchema.builder()
      .keyColumn(ColumnName.of("key"), SqlTypes.STRING)
      .keyColumn(ColumnName.of("key1"), SqlTypes.STRING)
      .build();

   private static final LogicalSchema LOGICAL_SCHEMA_INT_KEY = LogicalSchema.builder()
      .keyColumn(ColumnName.of("key"), SqlTypes.INTEGER)
      .build();

  private static final LogicalSchema LOGICAL_SCHEMA_VALUE_REORDERED = LogicalSchema.builder()
      .valueColumn(ColumnName.of("bigIntField"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("intField"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("doubleField"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("stringField"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("booleanField"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("arrayField"), SqlTypes.array(SqlTypes.INTEGER))
      .valueColumn(ColumnName.of("mapField"), SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT))
      .valueColumn(ColumnName.of("structField"), SqlTypes.struct()
          .field("s0", SqlTypes.BIGINT).build())
      .valueColumn(ColumnName.of("decimalField"), SqlTypes.decimal(4, 2))
      .build();

  private static final LogicalSchema LOGICAL_SCHEMA_VALUE_MISSING = LogicalSchema.builder()
      .valueColumn(ColumnName.of("intField"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("bigIntField"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("doubleField"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("stringField"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("booleanField"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("arrayField"), SqlTypes.array(SqlTypes.INTEGER))
      .valueColumn(ColumnName.of("mapField"), SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT))
      .valueColumn(ColumnName.of("structField"), SqlTypes.struct()
          .field("s0", SqlTypes.BIGINT).build())
      .build();

  private static final List<? extends SimpleColumn> SR_KEY_SCHEMA = LOGICAL_SCHEMA.key();
  private static final List<? extends SimpleColumn> SR_VALUE_SCHEMA = LOGICAL_SCHEMA.value();

  private static final ParsedSchema KEY_AVRO_SCHEMA = new AvroSchema(new Schema.Parser().parse("{" +
        "    \"type\": \"record\"," +
        "    \"name\": \"myrecord\"," +
        "    \"fields\": [" +
        "        { \"name\": \"key\", \"type\": \"string\" }" +
        "    ]" +
        "}"));

  private static final ParsedSchema VALUE_AVRO_SCHEMA = new AvroSchema(new Schema.Parser().parse("{" +
        "    \"type\": \"record\"," +
        "    \"name\": \"myrecord\"," +
        "    \"fields\": [" +
        "        { \"name\": \"value\", \"type\": \"string\" }" +
        "    ]" +
        "}"));

  private static final TableElements INFERRED_KSQL_KEY_SCHEMA_STREAM = TableElements.of(
      new TableElement(ColumnName.of("key"), new Type(SqlTypes.STRING), KEY_CONSTRAINT)
  );
  private static final TableElements INFERRED_KSQL_KEY_SCHEMA_TABLE = TableElements.of(
      new TableElement(ColumnName.of("key"), new Type(SqlTypes.STRING), PRIMARY_KEY_CONSTRAINT)
  );
  private static final TableElements INFERRED_KSQL_VALUE_SCHEMA = TableElements.of(
      new TableElement(ColumnName.of("intField"), new Type(SqlTypes.INTEGER)),
      new TableElement(ColumnName.of("bigIntField"), new Type(SqlTypes.BIGINT)),
      new TableElement(ColumnName.of("doubleField"), new Type(SqlTypes.DOUBLE)),
      new TableElement(ColumnName.of("stringField"), new Type(SqlTypes.STRING)),
      new TableElement(ColumnName.of("booleanField"), new Type(SqlTypes.BOOLEAN)),
      new TableElement(ColumnName.of("arrayField"), new Type(SqlTypes.array(SqlTypes.INTEGER))),
      new TableElement(ColumnName.of("mapField"), new Type(SqlTypes.map(
          SqlTypes.STRING, SqlTypes.BIGINT
      ))),
      new TableElement(ColumnName.of("structField"), new Type(SqlStruct.builder()
          .field("s0", SqlTypes.BIGINT)
          .build())),
      new TableElement(ColumnName.of("decimalField"), new Type(SqlTypes.decimal(4, 2))
      ));

  private static final int KEY_SCHEMA_ID = 18;
  private static final int VALUE_SCHEMA_ID = 5;

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private KafkaConsumerGroupClient consumerGroupClient;
  @Mock
  private KsqlExecutionContext executionSandbox;
  @Mock
  private KsqlExecutionContext executionContext;
  @Mock
  private KsqlPlan ksqlPlan;
  @Mock
  private CreateSourceCommand ddlCommand;
  @Mock
  private Statement statement;
  @Mock
  private CreateStream cs;
  @Mock
  private CreateSourceProperties sourceProperties;
  @Mock
  private CreateTable ct;
  @Mock
  private CreateStreamAsSelect csas;
  @Mock
  private CreateTableAsSelect ctas;
  @Mock
  private TopicSchemaSupplier schemaSupplier;
  private ConfiguredStatement<CreateStream> csStatement;
  private ConfiguredStatement<CreateTable> ctStatement;
  private ConfiguredStatement<CreateStreamAsSelect> csasStatement;
  private ConfiguredStatement<CreateTableAsSelect> ctasStatement;

  private DefaultSchemaInjector injector;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    when(cs.getName()).thenReturn(SourceName.of("cs"));
    when(ct.getName()).thenReturn(SourceName.of("ct"));
    when(csas.getName()).thenReturn(SourceName.of("csas"));
    when(ctas.getName()).thenReturn(SourceName.of("ctas"));

    when(cs.copyWith(any(), any())).thenAnswer(inv -> setupCopy(inv, cs, mock(CreateStream.class)));
    when(ct.copyWith(any(), any())).thenAnswer(inv -> setupCopy(inv, ct, mock(CreateTable.class)));
    when(csas.copyWith(any())).thenAnswer(inv -> setupCopy(inv, csas, mock(CreateStreamAsSelect.class)));
    when(ctas.copyWith(any())).thenAnswer(inv -> setupCopy(inv, ctas, mock(CreateTableAsSelect.class)));

    final KsqlConfig config = new KsqlConfig(ImmutableMap.of());
    csStatement = ConfiguredStatement.of(PreparedStatement.of(SQL_TEXT, cs),
        SessionConfig.of(config, ImmutableMap.of()));
    ctStatement = ConfiguredStatement.of(PreparedStatement.of(SQL_TEXT, ct),
        SessionConfig.of(config, ImmutableMap.of()));
    csasStatement = ConfiguredStatement.of(PreparedStatement.of(SQL_TEXT, csas),
        SessionConfig.of(config, ImmutableMap.of()));
    ctasStatement = ConfiguredStatement.of(PreparedStatement.of(SQL_TEXT, ctas),
        SessionConfig.of(config, ImmutableMap.of()));

    when(cs.getProperties()).thenReturn(sourceProperties);
    when(ct.getProperties()).thenReturn(sourceProperties);

    when(schemaSupplier.getKeySchema(any(), any(), any(), any()))
        .thenAnswer(invocation -> {
          final Optional<Integer> id = (Optional<Integer>) invocation.getArguments()[1];
          return SchemaResult.success(schemaAndId(SR_KEY_SCHEMA, KEY_AVRO_SCHEMA, id.orElse(KEY_SCHEMA_ID)));
        });
    when(schemaSupplier.getValueSchema(any(), any(), any(), any()))
        .thenAnswer(invocation -> {
          final Optional<Integer> id = (Optional<Integer>) invocation.getArguments()[1];
          return SchemaResult.success(schemaAndId(SR_VALUE_SCHEMA, VALUE_AVRO_SCHEMA, id.orElse(VALUE_SCHEMA_ID)));
        });

    when(cs.getElements()).thenReturn(TableElements.of());
    when(ct.getElements()).thenReturn(TableElements.of());

    when(serviceContext.getSchemaRegistryClient()).thenReturn(schemaRegistryClient);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(serviceContext.getConsumerGroupClient()).thenReturn(consumerGroupClient);

    when(executionContext.createSandbox(any())).thenReturn(executionSandbox);

    when(executionSandbox.plan(any(), any())).thenReturn(ksqlPlan);
    when(ksqlPlan.getDdlCommand()).thenReturn(Optional.of(ddlCommand));

    injector = new DefaultSchemaInjector(schemaSupplier, executionContext, serviceContext);
  }

  @Test
  public void shouldReturnStatementUnchangedIfNotCreateStatement() {
    // Given:
    final ConfiguredStatement<?> prepared = ConfiguredStatement
        .of(PreparedStatement.of("sql", statement),
            SessionConfig.of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of())
        );

    // When:
    final ConfiguredStatement<?> result = injector.inject(prepared);

    // Then:
    assertThat(result, is(sameInstance(prepared)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCsasDoesnotHaveSchemaId() {
    // Given:
    givenKeyAndValueInferenceSupported();

    // When:
    final ConfiguredStatement<?> result = injector.inject(csasStatement);

    // Then:
    assertThat(result, is(sameInstance(csasStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCtasDoesnotHaveSchemaId() {
    // Given:
    givenKeyAndValueInferenceSupported();

    // When:
    final ConfiguredStatement<?> result = injector.inject(ctasStatement);

    // Then:
    assertThat(result, is(sameInstance(ctasStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCsAlreadyHasSchemas() {
    // Given:
    givenKeyAndValueInferenceSupported();
    when(cs.getElements()).thenReturn(SOME_KEY_AND_VALUE_ELEMENTS_STREAM);

    // When:
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then:
    assertThat(result, is(sameInstance(csStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCtAlreadyHasSchemas() {
    // Given:
    givenKeyAndValueInferenceSupported();
    when(ct.getElements()).thenReturn(SOME_KEY_AND_VALUE_ELEMENTS_TABLE);

    // When:
    final ConfiguredStatement<?> result = injector.inject(ctStatement);

    // Then:
    assertThat(result, is(sameInstance(ctStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCsFormatsDoNotSupportInference() {
    // Given:
    givenNeitherKeyNorValueInferenceSupported();

    // When:
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then:
    assertThat(result, is(sameInstance(csStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCtFormatsDoNotSupportInference() {
    // Given:
    givenNeitherKeyNorValueInferenceSupported();

    // When:
    final ConfiguredStatement<?> result = injector.inject(ctStatement);

    // Then:
    assertThat(result, is(sameInstance(ctStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfHasKeySchemaAndValueFormatNotSupported() {
    // Given:
    givenKeyButNotValueInferenceSupported();
    when(cs.getElements()).thenReturn(SOME_KEY_ELEMENTS_STREAM);

    // When:
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then:
    assertThat(result, is(sameInstance(csStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfHasValueSchemaAndKeyFormatNotSupported() {
    // Given:
    givenValueButNotKeyInferenceSupported();
    when(cs.getElements()).thenReturn(SOME_VALUE_ELEMENTS);

    // When:
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then:
    assertThat(result, is(sameInstance(csStatement)));
  }

  @Test
  public void shouldInjectForCsStatement() {
    // Given:
    givenKeyAndValueInferenceSupported();

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(INFERRED_KSQL_KEY_SCHEMA_STREAM, INFERRED_KSQL_VALUE_SCHEMA)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE STREAM `cs` ("
            + "`key` STRING KEY, "
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='protobuf', VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldInjectForCtStatement() {
    // Given:
    givenKeyAndValueInferenceSupported();

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(INFERRED_KSQL_KEY_SCHEMA_TABLE, INFERRED_KSQL_VALUE_SCHEMA)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE TABLE `ct` ("
            + "`key` STRING PRIMARY KEY, "
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='protobuf', VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldInjectValueAndMaintainKeyColumnsForCs() {
    // Given:
    givenValueButNotKeyInferenceSupported();
    when(cs.getElements()).thenReturn(SOME_KEY_ELEMENTS_STREAM);

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(SOME_KEY_ELEMENTS_STREAM, INFERRED_KSQL_VALUE_SCHEMA)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE STREAM `cs` ("
            + "`bob` STRING KEY, "
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='kafka', VALUE_FORMAT='json_sr');"
    ));
  }

  @Test
  public void shouldInjectValueAndMaintainKeyColumnsForCt() {
    // Given:
    givenValueButNotKeyInferenceSupported();
    when(ct.getElements()).thenReturn(SOME_KEY_ELEMENTS_TABLE);

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(SOME_KEY_ELEMENTS_TABLE, INFERRED_KSQL_VALUE_SCHEMA)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE TABLE `ct` ("
            + "`bob` STRING PRIMARY KEY, "
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='kafka', VALUE_FORMAT='json_sr');"
    ));
  }

  @Test
  public void shouldInjectKeyAndMaintainValueColumnsForCs() {
    // Given:
    givenKeyButNotValueInferenceSupported();
    when(cs.getElements()).thenReturn(SOME_VALUE_ELEMENTS);

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(INFERRED_KSQL_KEY_SCHEMA_STREAM, SOME_VALUE_ELEMENTS)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE STREAM `cs` ("
            + "`key` STRING KEY, "
            + "`bob` STRING) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='avro', VALUE_FORMAT='delimited');"
    ));
  }

  @Test
  public void shouldInjectKeyAndMaintainValueColumnsForCt() {
    // Given:
    givenKeyButNotValueInferenceSupported();
    when(ct.getElements()).thenReturn(SOME_VALUE_ELEMENTS);

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(INFERRED_KSQL_KEY_SCHEMA_TABLE, SOME_VALUE_ELEMENTS)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE TABLE `ct` ("
            + "`key` STRING PRIMARY KEY, "
            + "`bob` STRING) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='avro', VALUE_FORMAT='delimited');"
    ));
  }

  @Test
  public void shouldInjectKeyAndValuesForCs() {
    // Given:
    givenKeyAndValueInferenceSupported();
    when(cs.getElements()).thenReturn(HEADER_ELEMENTS);

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(HEADER_ELEMENTS, INFERRED_KSQL_KEY_SCHEMA_STREAM, INFERRED_KSQL_VALUE_SCHEMA)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE STREAM `cs` ("
            + "`head` BYTES HEADER('header'), "
            + "`key` STRING KEY, "
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='protobuf', VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldInjectKeyAndValuesForCt() {
    // Given:
    givenKeyAndValueInferenceSupported();
    when(ct.getElements()).thenReturn(HEADER_ELEMENTS);

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(HEADER_ELEMENTS, INFERRED_KSQL_KEY_SCHEMA_TABLE, INFERRED_KSQL_VALUE_SCHEMA)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE TABLE `ct` ("
            + "`head` BYTES HEADER('header'), "
            + "`key` STRING PRIMARY KEY, "
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='protobuf', VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldInjectValuesAndMaintainKeysAndHeadersForCs() {
    // Given:
    givenKeyAndValueInferenceSupported();
    when(cs.getElements()).thenReturn(HEADER_AND_VALUE);

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(HEADER_ELEMENTS, INFERRED_KSQL_KEY_SCHEMA_STREAM, SOME_VALUE_ELEMENTS)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE STREAM `cs` ("
            + "`head` BYTES HEADER('header'), "
            + "`key` STRING KEY, "
            + "`bob` STRING) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='protobuf', VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldInjectValuesAndMaintainKeysAndHeadersForCt() {
    // Given:
    givenKeyAndValueInferenceSupported();
    when(ct.getElements()).thenReturn(HEADER_AND_VALUE);

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getStatement().getElements(),
        is(combineElements(HEADER_ELEMENTS, INFERRED_KSQL_KEY_SCHEMA_TABLE, SOME_VALUE_ELEMENTS)));
    assertThat(result.getMaskedStatementText(), is(
        "CREATE TABLE `ct` ("
            + "`head` BYTES HEADER('header'), "
            + "`key` STRING PRIMARY KEY, "
            + "`bob` STRING) "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='protobuf', VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldInjectKeyForCsas() {
    // Given:
    givenFormatsAndProps("avro", "delimited",
        ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA, "avro", "delimited",
        SerdeFeature.UNWRAP_SINGLES, SerdeFeature.WRAP_SINGLES);

    // When:
    final ConfiguredStatement<CreateStreamAsSelect> result = injector.inject(csasStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), is(
        "CREATE STREAM `csas` "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='avro', KEY_SCHEMA_FULL_NAME='myrecord', "
            + "KEY_SCHEMA_ID=42, VALUE_FORMAT='delimited') AS SELECT *\nFROM TABLE `sink`"
    ));
  }

  @Test
  public void shouldInjectKeyForCtas() {
    // Given:
    givenFormatsAndProps("avro", "delimited",
        ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA, "avro", "delimited",
        SerdeFeature.UNWRAP_SINGLES, SerdeFeature.WRAP_SINGLES);

    // When:
    final ConfiguredStatement<CreateTableAsSelect> result = injector.inject(ctasStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), is(
        "CREATE TABLE `ctas` "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='avro', KEY_SCHEMA_FULL_NAME='myrecord', "
            + "KEY_SCHEMA_ID=42, VALUE_FORMAT='delimited') AS SELECT *\nFROM TABLE `sink`"
    ));
  }

  @Test
  public void shouldInjectValueForCsas() {
    // Given:
    givenFormatsAndProps("kafka", "protobuf",
        ImmutableMap.of("VALUE_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA_VALUE_MISSING, "kafka", "protobuf",
        SerdeFeature.WRAP_SINGLES, SerdeFeature.WRAP_SINGLES);

    // When:
    final ConfiguredStatement<CreateStreamAsSelect> result = injector.inject(csasStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), is(
        "CREATE STREAM `csas` "
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='kafka', VALUE_FORMAT='protobuf', "
            + "VALUE_SCHEMA_FULL_NAME='myrecord', VALUE_SCHEMA_ID=42) AS SELECT *\nFROM TABLE `sink`"
    ));
  }

  @Test
  public void shouldThrowIfCsasKeyTableElementsNotCompatibleExtraKey() {
    // Given:
    givenFormatsAndProps("protobuf", null,
        ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA_EXTRA_KEY, "protobuf", "avro",
        SerdeFeature.WRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(csasStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("The following key columns are changed, missing or reordered: "
            + "[`key1` STRING KEY]. Schema from schema registry is [`key` STRING KEY]"));
  }
  @Test
  public void shouldThrowIfCtasKeyTableElementsNotCompatibleExtraKey() {
    // Given:
    givenFormatsAndProps("protobuf", null,
        ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA_EXTRA_KEY, "protobuf", "avro",
        SerdeFeature.WRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(ctasStatement)
    );

    // Then:
    // Then:
    assertThat(e.getMessage(),
        containsString("The following key columns are changed, missing or reordered: "
            + "[`key1` STRING KEY]. Schema from schema registry is [`key` STRING KEY]"));
  }

  @Test
  public void shouldThrowIfCsasKeyTableElementsNotCompatibleWrongKeyType() {
    // Given:
    givenFormatsAndProps("protobuf", null,
        ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA_INT_KEY, "protobuf", "avro",
        SerdeFeature.WRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(csasStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("The following key columns are changed, missing or reordered: "
            + "[`key` INTEGER KEY]. Schema from schema registry is [`key` STRING KEY]"));
  }

  @Test
  public void shouldThrowIfCtasKeyTableElementsNotCompatibleWrongKeyType() {
    // Given:
    givenFormatsAndProps("protobuf", null,
        ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA_INT_KEY, "protobuf", "avro",
        SerdeFeature.WRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(ctasStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("The following key columns are changed, missing or reordered: "
            + "[`key` INTEGER KEY]. Schema from schema registry is [`key` STRING KEY]"));
  }

  @Test
  public void shouldThrowIfCsasKeyTableElementsNotCompatibleReorderedValue() {
    // Given:
    givenFormatsAndProps("kafka", "avro",
        ImmutableMap.of("VALUE_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA_VALUE_REORDERED, "kafka", "avro",
        SerdeFeature.UNWRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(csasStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("The following value columns are changed, missing or reordered: "
            + "[`bigIntField` BIGINT, `intField` INTEGER]. Schema from schema registry is ["
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)]"
        )
    );
  }

  @Test
  public void shouldThrowIfCtasKeyTableElementsNotCompatibleReorderedValue() {
    // Given:
    givenFormatsAndProps("kafka", "avro",
        ImmutableMap.of("VALUE_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA_VALUE_REORDERED, "kafka", "avro",
        SerdeFeature.UNWRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(ctasStatement)
    );

    // Then:
    // Then:
    assertThat(e.getMessage(),
        containsString("The following value columns are changed, missing or reordered: "
            + "[`bigIntField` BIGINT, `intField` INTEGER]. Schema from schema registry is ["
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)]"
        )
    );
  }

  @Test
  public void shouldThrowIfCsasKeyFormatDoesnotSupportInference() {
    // Given:
    givenFormatsAndProps("kafka", null,
        ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA, "kafka", "avro",
        SerdeFeature.WRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(csasStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("KEY_FORMAT should support schema inference when "
            + "KEY_SCHEMA_ID is provided. Current format is KAFKA."));
  }

  @Test
  public void shouldThrowIfCtasKeyFormatDoesnotSupportInference() {
    // Given:
    givenFormatsAndProps("kafka", null,
        ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA, "kafka", "avro",
        SerdeFeature.UNWRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(ctasStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("KEY_FORMAT should support schema inference when "
            + "KEY_SCHEMA_ID is provided. Current format is KAFKA."));
  }

  @Test
  public void shouldThrowIfCsasValueFormatDoesnotSupportInference() {
    // Given:
    givenFormatsAndProps(null, "kafka",
        ImmutableMap.of("VALUE_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA, "kafka", "delimited",
        SerdeFeature.UNWRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(csasStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("VALUE_FORMAT should support schema inference when "
            + "VALUE_SCHEMA_ID is provided. Current format is DELIMITED."));
  }

  @Test
  public void shouldThrowIfCtasValueFormatDoesnotSupportInference() {
    // Given:
    givenFormatsAndProps(null, "kafka",
        ImmutableMap.of("VALUE_SCHEMA_ID", new IntegerLiteral(42)));
    givenDDLSchemaAndFormats(LOGICAL_SCHEMA, "kafka", "delimited",
        SerdeFeature.UNWRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(ctasStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("VALUE_FORMAT should support schema inference when "
            + "VALUE_SCHEMA_ID is provided. Current format is DELIMITED."));
  }

  @Test
  public void shouldThrowWhenTableElementsAndValueSchemaIdPresent() {
    // Given:
    givenFormatsAndProps(
        "protobuf",
        "avro",
        ImmutableMap.of("VALUE_SCHEMA_ID", new IntegerLiteral(42)));
    when(ct.getElements()).thenReturn(SOME_VALUE_ELEMENTS);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(ctStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Table elements and VALUE_SCHEMA_ID cannot both exist for create statement."));
  }

  @Test
  public void shouldUseKeySchemaIdWhenTableElementsPresent() {
    // Given:
    givenFormatsAndProps(
        "protobuf",
        "avro",
        ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));
    when(ct.getElements()).thenReturn(SOME_KEY_ELEMENTS_TABLE);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(ctStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Table elements and KEY_SCHEMA_ID cannot both exist for create statement."));
  }

  @Test
  public void shouldGetSchemasWithoutSchemaId() {
    // Given:
    givenKeyAndValueInferenceSupported();

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), not(containsString("KEY_SCHEMA_ID=18")));
    assertThat(result.getMaskedStatementText(), not(containsString("VALUE_SCHEMA_ID=5")));

    verify(schemaSupplier).getKeySchema(Optional.of(KAFKA_TOPIC), Optional.empty(), FormatInfo.of("PROTOBUF"), SerdeFeatures.of());
    verify(schemaSupplier).getValueSchema(Optional.of(KAFKA_TOPIC), Optional.empty(), FormatInfo.of("AVRO"), SerdeFeatures.of());
  }

  @Test
  public void shouldGetValueSchemaWithSchemaId() {
    // Given:
    givenValueButNotKeyInferenceSupported(ImmutableMap.of("VALUE_SCHEMA_ID", new IntegerLiteral(42)));

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), is(
        "CREATE STREAM `cs` ("
        + "`intField` INTEGER, "
        + "`bigIntField` BIGINT, "
        + "`doubleField` DOUBLE, "
        + "`stringField` STRING, "
        + "`booleanField` BOOLEAN, "
        + "`arrayField` ARRAY<INTEGER>, "
        + "`mapField` MAP<STRING, BIGINT>, "
        + "`structField` STRUCT<`s0` BIGINT>, "
        + "`decimalField` DECIMAL(4, 2)) "
        + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='kafka', "
        + "VALUE_FORMAT='json_sr', VALUE_SCHEMA_FULL_NAME='myrecord', VALUE_SCHEMA_ID=42);"));

    assertThat(result.getSessionConfig().getOverrides(), hasKey(ConnectFormat.VALUE_SCHEMA_ID));
    SchemaAndId schemaAndId = (SchemaAndId) result.getSessionConfig().getOverrides().get(ConnectFormat.VALUE_SCHEMA_ID);
    assertThat(schemaAndId.id, is(42));
    assertThat(schemaAndId.rawSchema, sameInstance(VALUE_AVRO_SCHEMA));
    verify(schemaSupplier).getValueSchema(any(), eq(Optional.of(42)), any(), any());
  }

  @Test
  public void shouldGetKeySchemaWithSchemaId() {
    // Given:
    givenKeyButNotValueInferenceSupported(ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), is(
        "CREATE STREAM `cs` ("
            + "`key` STRING KEY) WITH ("
            + "KAFKA_TOPIC='some-topic', "
            + "KEY_FORMAT='avro', KEY_SCHEMA_FULL_NAME='myrecord', "
            + "KEY_SCHEMA_ID=42, VALUE_FORMAT='delimited');"
    ));
    verify(schemaSupplier).getKeySchema(any(), eq(Optional.of(42)), any(), any());
  }

  @Test
  public void shouldNotAddSchemaIdsIfNotPresentAlready() {
    // Given:
    givenKeyAndValueInferenceSupported();

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertFalse(result.getStatement().getProperties().getKeySchemaId().isPresent());
    assertFalse(result.getStatement().getProperties().getValueSchemaId().isPresent());
  }

  @Test
  public void shouldEscapeAvroSchemaThatHasReservedColumnName() {
    // Given:
    givenKeyAndValueInferenceSupported();

    reset(schemaSupplier);
    when(schemaSupplier.getKeySchema(any(), any(), any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SR_KEY_SCHEMA, KEY_AVRO_SCHEMA, KEY_SCHEMA_ID)));

    final SimpleColumn col0 = mock(SimpleColumn.class);
    when(col0.name()).thenReturn(ColumnName.of("CREATE"));
    when(col0.type()).thenReturn(SqlTypes.BIGINT);

    when(schemaSupplier.getValueSchema(any(), any(), any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(ImmutableList.of(col0), VALUE_AVRO_SCHEMA, VALUE_SCHEMA_ID)));

    // When:
    final ConfiguredStatement<CreateTable> inject = injector.inject(ctStatement);

    // Then:
    assertThat(inject.getMaskedStatementText(), containsString("`CREATE`"));
  }

  @Test
  public void shouldSetUnwrappingForKeySchemaIfSupported() {
    // Given:
    givenFormatsAndProps("avro", "delimited", ImmutableMap.of());

    // When:
    injector.inject(ctStatement);

    // Then:
    verify(schemaSupplier).getKeySchema(
        Optional.of(KAFKA_TOPIC),
        Optional.empty(),
        FormatInfo.of("AVRO", ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "io.confluent.ksql.avro_schemas.CtKey")),
        SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
    );
  }

  @Test
  public void shouldNotSetUnwrappingForKeySchemaIfUnsupported() {
    // Given:
    givenFormatsAndProps("protobuf", "delimited", ImmutableMap.of());

    // When:
    injector.inject(ctStatement);

    // Then:
    verify(schemaSupplier).getKeySchema(
        Optional.of(KAFKA_TOPIC),
        Optional.empty(),
        FormatInfo.of("PROTOBUF"),
        SerdeFeatures.of()
    );
  }

  @Test
  public void shouldSetUnwrappingForValueSchemaIfPresent() {
    // Given:
    givenFormatsAndProps(
        "delimited",
        "avro",
        ImmutableMap.of("WRAP_SINGLE_VALUE", new BooleanLiteral("false"))
    );

    // When:
    injector.inject(ctStatement);

    // Then:
    verify(schemaSupplier).getValueSchema(
        Optional.of(KAFKA_TOPIC),
        Optional.empty(),
        FormatInfo.of("AVRO"),
        SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
    );
  }

  @Test
  public void shouldThrowIfKeyFormatDoesNotSupportSchemaIdInference() {
    // Given
    givenValueButNotKeyInferenceSupported(
        ImmutableMap.of("key_schema_id", new IntegerLiteral(123)));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(csStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("KEY_FORMAT should support schema inference when KEY_SCHEMA_ID is provided. "
            + "Current format is KAFKA."));
  }

  @Test
  public void shouldThrowIfValueFormatDoesNotSupportSchemaIdInference() {
    // Given
    givenKeyButNotValueInferenceSupported(
        ImmutableMap.of("value_schema_id", new IntegerLiteral(123),
            "WRAP_SINGLE_VALUE", new BooleanLiteral(true)));
    when(cs.getElements()).thenReturn(SOME_KEY_ELEMENTS_STREAM);


    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(csStatement)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("VALUE_FORMAT should support schema inference when VALUE_SCHEMA_ID is provided. "
            + "Current format is DELIMITED."));
  }

  @Test
  public void shouldThrowIfSchemaSupplierThrows() {
    // Given:
    givenKeyAndValueInferenceSupported();

    reset(schemaSupplier);
    when(schemaSupplier.getKeySchema(any(), any(), any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SR_KEY_SCHEMA, KEY_AVRO_SCHEMA, KEY_SCHEMA_ID)));
    when(schemaSupplier.getValueSchema(any(), any(), any(), any()))
        .thenThrow(new KsqlException("Oh no!"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(csStatement)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Oh no"));
  }

  private void givenNeitherKeyNorValueInferenceSupported() {
    givenFormatsAndProps("delimited", "kafka", ImmutableMap.of());
  }

  private void givenKeyAndValueInferenceSupported() {
    givenFormatsAndProps("protobuf", "avro", ImmutableMap.of());
  }

  private void givenKeyButNotValueInferenceSupported() {
    givenKeyButNotValueInferenceSupported(ImmutableMap.of());
  }

  private void givenKeyButNotValueInferenceSupported(final Map<String, Literal> additionalProps) {
    givenFormatsAndProps("avro", "delimited", additionalProps);
  }

  private void givenValueButNotKeyInferenceSupported() {
    givenValueButNotKeyInferenceSupported(ImmutableMap.of());
  }

  private void givenValueButNotKeyInferenceSupported(final Map<String, Literal> additionalProps) {
    givenFormatsAndProps("kafka", "json_sr", additionalProps);
  }

  private Formats givenFormats(final String keyFormat, final String valueFormat,
      final SerdeFeature keyFeature, final SerdeFeature valueFeature) {
    return Formats.of(FormatInfo.of(keyFormat), FormatInfo.of(valueFormat),
        SerdeFeatures.of(keyFeature), SerdeFeatures.of(valueFeature));
  }

  private void givenDDLSchemaAndFormats(
      final LogicalSchema schema,
      final String keyFormat,
      final String valueFormat,
      final SerdeFeature keyFeature,
      final SerdeFeature valueFeature
  ) {
    when(ddlCommand.getSchema()).thenReturn(schema);
    when(ddlCommand.getFormats()).thenReturn(
        givenFormats(keyFormat, valueFormat, keyFeature, valueFeature));
  }

  private void givenFormatsAndProps(
      final String keyFormat,
      final String valueFormat,
      final Map<String, Literal> additionalProps) {
    final HashMap<String, Literal> props = new HashMap<>(BASE_PROPS);
    if (keyFormat != null) {
      props.put("KEY_FORMAT", new StringLiteral(keyFormat));
    }
    if (valueFormat != null) {
      props.put("VALUE_FORMAT", new StringLiteral(valueFormat));
    }
    props.putAll(additionalProps);
    final CreateSourceProperties csProps = CreateSourceProperties.from(props);
    final CreateSourceAsProperties casProps = CreateSourceAsProperties.from(props);

    when(cs.getProperties()).thenReturn(csProps);
    when(ct.getProperties()).thenReturn(csProps);
    when(csas.getProperties()).thenReturn(casProps);
    when(ctas.getProperties()).thenReturn(casProps);

    /*
    when(csas.getSink()).thenReturn(
        Sink.of(SourceName.of("csas"), true, false, casProps));
    when(ctas.getSink()).thenReturn(
        Sink.of(SourceName.of("ctas"), true, false, casProps));
     */
  }

  private static Object setupCopy(
      final InvocationOnMock inv,
      final CreateSource source,
      final CreateSource mock
  ) {
    final SourceName name = source.getName();
    when(mock.getName()).thenReturn(name);
    when(mock.getElements()).thenReturn(inv.getArgument(0));
    when(mock.accept(any(), any())).thenCallRealMethod();
    when(mock.getProperties()).thenReturn(inv.getArgument(1));
    return mock;
  }

  private static Object setupCopy(
      final InvocationOnMock inv,
      final CreateAsSelect source,
      final CreateAsSelect mock
  ) {
    final SourceName name = source.getName();
    when(mock.getName()).thenReturn(name);
    when(mock.accept(any(), any())).thenCallRealMethod();
    when(mock.getProperties()).thenReturn(inv.getArgument(0));
    when(mock.getQuery()).thenReturn(
        new Query(
            Optional.empty(),
        new Select(ImmutableList.of(new AllColumns(Optional.empty()))),
        new Table(SourceName.of("sink")),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        OptionalInt.empty()
    ));
    return mock;
  }

  private static TableElements combineElements(
      final TableElements keyElems,
      final TableElements valueElems
  ) {
    return TableElements.of(
        Stream.concat(keyElems.stream(), valueElems.stream())
            .collect(Collectors.toList())
    );
  }

  private static TableElements combineElements(
      final TableElements headerElems,
      final TableElements keyElems,
      final TableElements valueElems
  ) {
    Stream.of(headerElems.stream(), keyElems.stream(), valueElems.stream()).flatMap(s -> s);
    return TableElements.of(
        Stream.of(headerElems.stream(), keyElems.stream(), valueElems.stream())
            .flatMap(s -> s)
            .collect(Collectors.toList())
    );
  }
}