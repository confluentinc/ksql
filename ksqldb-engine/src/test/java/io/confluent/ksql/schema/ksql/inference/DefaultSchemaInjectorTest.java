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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSchemaInjectorTest {

  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(Namespace.VALUE, ColumnName.of("bob"), new Type(SqlTypes.STRING)));
  private static final String KAFKA_TOPIC = "some-topic";
  private static final Map<String, Literal> UNSUPPORTED_PROPS = ImmutableMap.of(
      "VALUE_FORMAT", new StringLiteral("delimited"),
      "KAFKA_TOPIC", new StringLiteral(KAFKA_TOPIC)
  );
  private static final Map<String, Literal> SUPPORTED_PROPS = ImmutableMap.of(
      "VALUE_FORMAT", new StringLiteral("avro"),
      "KAFKA_TOPIC", new StringLiteral(KAFKA_TOPIC)
  );

  private static final String SQL_TEXT = "Some SQL";

  private static final List<? extends SimpleColumn> SUPPORTED_SCHEMAS = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("intField"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("bigIntField"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("doubleField"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("stringField"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("booleanField"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("arrayField"), SqlTypes.array(SqlTypes.INTEGER))
      .valueColumn(ColumnName.of("mapField"), SqlTypes.map(SqlTypes.BIGINT))
      .valueColumn(ColumnName.of("structField"), SqlTypes.struct()
          .field("s0", SqlTypes.BIGINT).build())
      .valueColumn(ColumnName.of("decimalField"), SqlTypes.decimal(4, 2))
      .build()
      .value();

  private static final TableElements EXPECTED_KSQL_SCHEMA = TableElements.of(
      new TableElement(Namespace.VALUE, ColumnName.of("intField"), new Type(SqlTypes.INTEGER)),
      new TableElement(Namespace.VALUE, ColumnName.of("bigIntField"), new Type(SqlTypes.BIGINT)),
      new TableElement(Namespace.VALUE, ColumnName.of("doubleField"), new Type(SqlTypes.DOUBLE)),
      new TableElement(Namespace.VALUE, ColumnName.of("stringField"), new Type(SqlTypes.STRING)),
      new TableElement(Namespace.VALUE, ColumnName.of("booleanField"), new Type(SqlTypes.BOOLEAN)),
      new TableElement(Namespace.VALUE, ColumnName.of("arrayField"), new Type(SqlTypes.array(SqlTypes.INTEGER))),
      new TableElement(Namespace.VALUE, ColumnName.of("mapField"), new Type(SqlTypes.map(SqlTypes.BIGINT))),
      new TableElement(Namespace.VALUE, ColumnName.of("structField"), new Type(SqlStruct.builder()
          .field("s0", SqlTypes.BIGINT)
          .build())),
      new TableElement(Namespace.VALUE,
          ColumnName.of("decimalField"), new Type(SqlTypes.decimal(4, 2))
      ));

  private static final int SCHEMA_ID = 5;

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

  @Before
  public void setUp() {
    when(cs.getName()).thenReturn(SourceName.of("cs"));
    when(ct.getName()).thenReturn(SourceName.of("ct"));

    when(cs.getProperties()).thenReturn(CreateSourceProperties.from(SUPPORTED_PROPS));
    when(ct.getProperties()).thenReturn(CreateSourceProperties.from(SUPPORTED_PROPS));

    when(cs.copyWith(any(), any())).thenAnswer(inv -> setupCopy(inv, cs, mock(CreateStream.class)));
    when(ct.copyWith(any(), any())).thenAnswer(inv -> setupCopy(inv, ct, mock(CreateTable.class)));

    final KsqlConfig config = new KsqlConfig(ImmutableMap.of());
    csStatement = ConfiguredStatement.of(PreparedStatement.of(SQL_TEXT, cs), ImmutableMap.of(), config);
    ctStatement = ConfiguredStatement.of(PreparedStatement.of(SQL_TEXT, ct), ImmutableMap.of(), config);

    when(schemaSupplier.getValueSchema(eq(KAFKA_TOPIC), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMAS, SCHEMA_ID)));

    when(cs.getElements()).thenReturn(TableElements.of());
    when(ct.getElements()).thenReturn(TableElements.of());

    injector = new DefaultSchemaInjector(schemaSupplier);
  }

  @Test
  public void shouldReturnStatementUnchangedIfNotCreateStatement() {
    // Given:
    final ConfiguredStatement<?> prepared = ConfiguredStatement.of(
        PreparedStatement.of("sql", statement),
        ImmutableMap.of(),
        new KsqlConfig(ImmutableMap.of()));

    // When:
    final ConfiguredStatement<?> result = injector.inject(prepared);

    // Then:
    assertThat(result, is(sameInstance(prepared)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCsAlreadyHasSchema() {
    // Given:
    when(cs.getElements()).thenReturn(SOME_ELEMENTS);

    // When:
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then:
    assertThat(result, is(sameInstance(csStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCtAlreadyHasSchema() {
    // Given:
    when(ct.getElements()).thenReturn(SOME_ELEMENTS);

    // When:
    final ConfiguredStatement<?> result = injector.inject(ctStatement);

    // Then:
    assertThat(result, is(sameInstance(ctStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCsFormatDoesNotSupportInference() {
    // Given:
    when(cs.getProperties()).thenReturn(CreateSourceProperties.from(UNSUPPORTED_PROPS));

    // When:
    final ConfiguredStatement<?> result = injector.inject(csStatement);

    // Then:
    assertThat(result, is(sameInstance(csStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCtFormatDoesNotSupportInference() {
    // Given:
    when(ct.getProperties()).thenReturn(CreateSourceProperties.from(UNSUPPORTED_PROPS));

    // When:
    final ConfiguredStatement<?> result = injector.inject(ctStatement);

    // Then:
    assertThat(result, is(sameInstance(ctStatement)));
  }

  @Test
  public void shouldThrowIfSchemaNotRegisteredOrNotCompatible() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenReturn(SchemaResult.failure(new KsqlException("schema missing or incompatible")));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> injector.inject(ctStatement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "schema missing or incompatible"));
  }

  @Test
  public void shouldAddElementsToCsStatement() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMAS, SCHEMA_ID)));

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getStatement().getElements(), is(EXPECTED_KSQL_SCHEMA));
  }

  @Test
  public void shouldAddElementsToCtStatement() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMAS, SCHEMA_ID)));

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getStatement().getElements(), is(EXPECTED_KSQL_SCHEMA));
  }

  @Test
  public void shouldBuildNewCsStatementText() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMAS, SCHEMA_ID)));

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
            + "WITH (KAFKA_TOPIC='some-topic', SCHEMA_ID=5, VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldBuildNewCtStatementText() {
    // Given:
    when(schemaSupplier.getValueSchema(KAFKA_TOPIC, Optional.empty()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMAS, SCHEMA_ID)));

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), is(
        "CREATE TABLE `ct` ("
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)) "
            + "WITH (KAFKA_TOPIC='some-topic', SCHEMA_ID=5, VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldBuildNewCsStatementTextFromId() {
    // Given:
    when(cs.getProperties()).thenReturn(supportedPropsWith("SCHEMA_ID", "42"));

    when(schemaSupplier.getValueSchema(KAFKA_TOPIC, Optional.of(42)))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMAS, SCHEMA_ID)));

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
            + "WITH (KAFKA_TOPIC='some-topic', SCHEMA_ID='42', VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldBuildNewCtStatementTextFromId() {
    // Given:
    when(ct.getProperties()).thenReturn(supportedPropsWith("SCHEMA_ID", "42"));

    when(schemaSupplier.getValueSchema(KAFKA_TOPIC, Optional.of(42)))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMAS, SCHEMA_ID)));

    // When:
    final ConfiguredStatement<CreateTable> result = injector.inject(ctStatement);

    // Then:
    assertThat(result.getMaskedStatementText(), is(
        "CREATE TABLE `ct` ("
            + "`intField` INTEGER, "
            + "`bigIntField` BIGINT, "
            + "`doubleField` DOUBLE, "
            + "`stringField` STRING, "
            + "`booleanField` BOOLEAN, "
            + "`arrayField` ARRAY<INTEGER>, "
            + "`mapField` MAP<STRING, BIGINT>, "
            + "`structField` STRUCT<`s0` BIGINT>, "
            + "`decimalField` DECIMAL(4, 2)) "
            + "WITH (KAFKA_TOPIC='some-topic', SCHEMA_ID='42', VALUE_FORMAT='avro');"
    ));
  }

  @Test
  public void shouldAddSchemaIdIfNotPresentAlready() {
    // Given:
    when(schemaSupplier.getValueSchema(KAFKA_TOPIC, Optional.empty()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMAS, SCHEMA_ID)));

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getStatement().getProperties().getSchemaId(), is(Optional.of(SCHEMA_ID)));

    assertThat(result.getMaskedStatementText(), containsString("SCHEMA_ID=5"));
  }

  @Test
  public void shouldNotOverwriteExistingSchemaId() {
    // Given:
    when(cs.getProperties()).thenReturn(supportedPropsWith("SCHEMA_ID", "42"));

    // When:
    final ConfiguredStatement<CreateStream> result = injector.inject(csStatement);

    // Then:
    assertThat(result.getStatement().getProperties().getSchemaId(), is(Optional.of(42)));

    assertThat(result.getMaskedStatementText(), containsString("SCHEMA_ID='42'"));
  }

  @Test
  public void shouldEscapeAvroSchemaThatHasReservedColumnName() {
    // Given:
    final SimpleColumn col0 = mock(SimpleColumn.class);
    when(col0.name()).thenReturn(ColumnName.of("CREATE"));
    when(col0.type()).thenReturn(SqlTypes.BIGINT);

    when(schemaSupplier.getValueSchema(any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(ImmutableList.of(col0), SCHEMA_ID)));

    // When:
    final ConfiguredStatement<CreateTable> inject = injector.inject(ctStatement);

    // Then:
    assertThat(inject.getMaskedStatementText(), containsString("`CREATE`"));
  }

  @Test
  public void shouldThrowIfSchemaSupplierThrows() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenThrow(new KsqlException("Oh no!"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> injector.inject(csStatement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Oh no"));
  }

  @SuppressWarnings("SameParameterValue")
  private static CreateSourceProperties supportedPropsWith(
      final String property,
      final String value
  ) {
    final HashMap<String, Literal> props = new HashMap<>(SUPPORTED_PROPS);
    props.put(property, new StringLiteral(value));
    return CreateSourceProperties.from(props);
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
}