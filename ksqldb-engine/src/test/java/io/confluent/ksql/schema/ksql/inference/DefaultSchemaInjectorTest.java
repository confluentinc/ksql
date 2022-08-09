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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSchemaInjectorTest {

  private static final TableElements SOME_KEY_ELEMENTS_STREAM = TableElements.of(
      new TableElement(Namespace.KEY, ColumnName.of("bob"), new Type(SqlTypes.STRING)));
  private static final TableElements SOME_KEY_ELEMENTS_TABLE = TableElements.of(
      new TableElement(Namespace.PRIMARY_KEY, ColumnName.of("bob"), new Type(SqlTypes.STRING)));
  private static final TableElements SOME_VALUE_ELEMENTS = TableElements.of(
      new TableElement(Namespace.VALUE, ColumnName.of("bob"), new Type(SqlTypes.STRING)));
  private static final TableElements SOME_KEY_AND_VALUE_ELEMENTS_STREAM = TableElements.of(
      new TableElement(Namespace.KEY, ColumnName.of("k"), new Type(SqlTypes.STRING)),
      new TableElement(Namespace.VALUE, ColumnName.of("bob"), new Type(SqlTypes.STRING)));
  private static final TableElements SOME_KEY_AND_VALUE_ELEMENTS_TABLE = TableElements.of(
      new TableElement(Namespace.PRIMARY_KEY, ColumnName.of("k"), new Type(SqlTypes.STRING)),
      new TableElement(Namespace.VALUE, ColumnName.of("bob"), new Type(SqlTypes.STRING)));

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
  private static final List<? extends SimpleColumn> SR_KEY_SCHEMA = LOGICAL_SCHEMA.key();
  private static final List<? extends SimpleColumn> SR_VALUE_SCHEMA = LOGICAL_SCHEMA.value();

  private static final TableElements INFERRED_KSQL_KEY_SCHEMA_STREAM = TableElements.of(
      new TableElement(Namespace.KEY, ColumnName.of("key"), new Type(SqlTypes.STRING))
  );
  private static final TableElements INFERRED_KSQL_KEY_SCHEMA_TABLE = TableElements.of(
      new TableElement(Namespace.PRIMARY_KEY, ColumnName.of("key"), new Type(SqlTypes.STRING))
  );
  private static final TableElements INFERRED_KSQL_VALUE_SCHEMA = TableElements.of(
      new TableElement(Namespace.VALUE, ColumnName.of("intField"), new Type(SqlTypes.INTEGER)),
      new TableElement(Namespace.VALUE, ColumnName.of("bigIntField"), new Type(SqlTypes.BIGINT)),
      new TableElement(Namespace.VALUE, ColumnName.of("doubleField"), new Type(SqlTypes.DOUBLE)),
      new TableElement(Namespace.VALUE, ColumnName.of("stringField"), new Type(SqlTypes.STRING)),
      new TableElement(Namespace.VALUE, ColumnName.of("booleanField"), new Type(SqlTypes.BOOLEAN)),
      new TableElement(Namespace.VALUE, ColumnName.of("arrayField"), new Type(SqlTypes.array(SqlTypes.INTEGER))),
      new TableElement(Namespace.VALUE, ColumnName.of("mapField"), new Type(SqlTypes.map(
          SqlTypes.STRING, SqlTypes.BIGINT
      ))),
      new TableElement(Namespace.VALUE, ColumnName.of("structField"), new Type(SqlStruct.builder()
          .field("s0", SqlTypes.BIGINT)
          .build())),
      new TableElement(Namespace.VALUE,
          ColumnName.of("decimalField"), new Type(SqlTypes.decimal(4, 2))
      ));

  private static final int KEY_SCHEMA_ID = 18;
  private static final int VALUE_SCHEMA_ID = 5;

  @Mock
  private Statement statement;
  @Mock
  private CreateStream cs;
  @Mock
  private CreateTable ct;
  @Mock
  private TopicSchemaSupplier schemaSupplier;
  private ConfiguredStatement<CreateStream> csStatement;
  private ConfiguredStatement<CreateTable> ctStatement;

  private DefaultSchemaInjector injector;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    when(cs.getName()).thenReturn(SourceName.of("cs"));
    when(ct.getName()).thenReturn(SourceName.of("ct"));

    when(cs.copyWith(any(), any())).thenAnswer(inv -> setupCopy(inv, cs, mock(CreateStream.class)));
    when(ct.copyWith(any(), any())).thenAnswer(inv -> setupCopy(inv, ct, mock(CreateTable.class)));

    final KsqlConfig config = new KsqlConfig(ImmutableMap.of());
    csStatement = ConfiguredStatement.of(PreparedStatement.of(SQL_TEXT, cs),
        SessionConfig.of(config, ImmutableMap.of()));
    ctStatement = ConfiguredStatement.of(PreparedStatement.of(SQL_TEXT, ct),
        SessionConfig.of(config, ImmutableMap.of()));

    when(schemaSupplier.getKeySchema(any(), any(), any(), any()))
        .thenAnswer(invocation -> {
          final Optional<Integer> id = (Optional<Integer>) invocation.getArguments()[1];
          return SchemaResult.success(schemaAndId(SR_KEY_SCHEMA, id.orElse(KEY_SCHEMA_ID)));
        });
    when(schemaSupplier.getValueSchema(any(), any(), any(), any()))
        .thenAnswer(invocation -> {
          final Optional<Integer> id = (Optional<Integer>) invocation.getArguments()[1];
          return SchemaResult.success(schemaAndId(SR_VALUE_SCHEMA, id.orElse(VALUE_SCHEMA_ID)));
        });

    when(cs.getElements()).thenReturn(TableElements.of());
    when(ct.getElements()).thenReturn(TableElements.of());

    injector = new DefaultSchemaInjector(schemaSupplier);
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
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='protobuf', KEY_SCHEMA_ID=18, VALUE_FORMAT='avro', VALUE_SCHEMA_ID=5);"
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
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='protobuf', KEY_SCHEMA_ID=18, VALUE_FORMAT='avro', VALUE_SCHEMA_ID=5);"
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
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='kafka', VALUE_FORMAT='json_sr', VALUE_SCHEMA_ID=5);"
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
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='kafka', VALUE_FORMAT='json_sr', VALUE_SCHEMA_ID=5);"
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
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='avro', KEY_SCHEMA_ID=18, VALUE_FORMAT='delimited');"
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
            + "WITH (KAFKA_TOPIC='some-topic', KEY_FORMAT='avro', KEY_SCHEMA_ID=18, VALUE_FORMAT='delimited');"
    ));
  }

  @Test
  public void shouldGetSchemasWithoutSchemaId() {
    // Given:
    givenKeyAndValueInferenceSupported();

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), containsString("KEY_SCHEMA_ID=18"));
    assertThat(result.getMaskedStatementText(), containsString("VALUE_SCHEMA_ID=5"));

    verify(schemaSupplier).getKeySchema(KAFKA_TOPIC, Optional.empty(), FormatInfo.of("PROTOBUF"), SerdeFeatures.of());
    verify(schemaSupplier).getValueSchema(KAFKA_TOPIC, Optional.empty(), FormatInfo.of("AVRO"), SerdeFeatures.of());
  }

  @Test
  public void shouldGetValueSchemaWithSchemaId() {
    // Given:
    givenValueButNotKeyInferenceSupported(ImmutableMap.of("VALUE_SCHEMA_ID", new IntegerLiteral(42)));

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), containsString("VALUE_SCHEMA_ID=42"));

    verify(schemaSupplier).getValueSchema(any(), eq(Optional.of(42)), any(), any());
  }

  @Test
  public void shouldGetKeySchemaWithSchemaId() {
    // Given:
    givenKeyButNotValueInferenceSupported(ImmutableMap.of("KEY_SCHEMA_ID", new IntegerLiteral(42)));

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), containsString("KEY_SCHEMA_ID=42"));

    verify(schemaSupplier).getKeySchema(any(), eq(Optional.of(42)), any(), any());
  }

  @Test
  public void shouldAddSchemaIdsIfNotPresentAlready() {
    // Given:
    givenKeyAndValueInferenceSupported();

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getStatement().getProperties().getKeySchemaId(), is(Optional.of(KEY_SCHEMA_ID)));
    assertThat(result.getStatement().getProperties().getValueSchemaId(), is(Optional.of(VALUE_SCHEMA_ID)));

    assertThat(result.getMaskedStatementText(), containsString("KEY_SCHEMA_ID=18"));
    assertThat(result.getMaskedStatementText(), containsString("VALUE_SCHEMA_ID=5"));
  }

  @Test
  public void shouldEscapeAvroSchemaThatHasReservedColumnName() {
    // Given:
    givenKeyAndValueInferenceSupported();

    reset(schemaSupplier);
    when(schemaSupplier.getKeySchema(any(), any(), any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SR_KEY_SCHEMA, KEY_SCHEMA_ID)));

    final SimpleColumn col0 = mock(SimpleColumn.class);
    when(col0.name()).thenReturn(ColumnName.of("CREATE"));
    when(col0.type()).thenReturn(SqlTypes.BIGINT);

    when(schemaSupplier.getValueSchema(any(), any(), any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(ImmutableList.of(col0), VALUE_SCHEMA_ID)));

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
        KAFKA_TOPIC,
        Optional.empty(),
        FormatInfo.of("AVRO", ImmutableMap.of(AvroFormat.FULL_SCHEMA_NAME, "io.confluent.ksql.avro_schemas.CtKey")),
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
        KAFKA_TOPIC,
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
        KAFKA_TOPIC,
        Optional.empty(),
        FormatInfo.of("AVRO"),
        SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
    );
  }

  @Test
  public void shouldThrowIfSchemaSupplierThrows() {
    // Given:
    givenKeyAndValueInferenceSupported();

    reset(schemaSupplier);
    when(schemaSupplier.getKeySchema(any(), any(), any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SR_KEY_SCHEMA, KEY_SCHEMA_ID)));
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

  private void givenFormatsAndProps(
      final String keyFormat,
      final String valueFormat,
      final Map<String, Literal> additionalProps) {
    final HashMap<String, Literal> props = new HashMap<>(BASE_PROPS);
    props.put("KEY_FORMAT", new StringLiteral(keyFormat));
    props.put("VALUE_FORMAT", new StringLiteral(valueFormat));
    props.putAll(additionalProps);
    final CreateSourceProperties csProps = CreateSourceProperties.from(props);

    when(cs.getProperties()).thenReturn(csProps);
    when(ct.getProperties()).thenReturn(csProps);
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

  private static TableElements combineElements(
      final TableElements keyElems,
      final TableElements valueElems
  ) {
    return TableElements.of(
        Stream.concat(keyElems.stream(), valueElems.stream())
            .collect(Collectors.toList())
    );
  }
}