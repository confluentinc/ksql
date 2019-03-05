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

package io.confluent.ksql.schema.inference;

import static io.confluent.ksql.schema.inference.TopicSchemaSupplier.SchemaAndId.schemaAndId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.schema.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSchemaInjectorTest {

  private static final List<TableElement> SOME_ELEMENTS = ImmutableList.of(
      new TableElement("bob", new PrimitiveType(SqlType.STRING)));
  private static final Map<String, Expression> UNSUPPORTED_PROPS = ImmutableMap.of(
      "VALUE_FORMAT", new StringLiteral("json")
  );
  private static final String KAFKA_TOPIC = "some-topic";
  private static final Map<String, Expression> SUPPORTED_PROPS = ImmutableMap.of(
      "VALUE_FORMAT", new StringLiteral("avro"),
      "KAFKA_TOPIC", new StringLiteral(KAFKA_TOPIC)
  );

  private static final String SQL_TEXT = "Some SQL";

  private static final List<Schema> UNSUPPORTED_SCHEMAS = ImmutableList.of(
      SchemaBuilder.struct().field("byte", Schema.INT8_SCHEMA).build(),
      SchemaBuilder.struct().field("short", Schema.INT16_SCHEMA).build(),
      SchemaBuilder.struct().field("bytes", Schema.BYTES_SCHEMA).build(),
      SchemaBuilder.struct().field("nonStringKeyMap", SchemaBuilder
          .map(Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)).build()
  );

  private static final Schema SUPPORTED_SCHEMA = SchemaBuilder.struct()
      .field("intField", Schema.OPTIONAL_INT32_SCHEMA)
      .field("bigIntField", Schema.OPTIONAL_INT64_SCHEMA)
      .field("floatField", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .field("doubleField", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("stringField", Schema.OPTIONAL_STRING_SCHEMA)
      .field("booleanField", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("arrayField", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA))
      .field("mapField", SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA))
      .field("structField", SchemaBuilder.struct()
          .field("s0", Schema.OPTIONAL_INT64_SCHEMA).build())
      .build();

  private static final List<TableElement> EXPECTED_KSQL_SCHEMA = ImmutableList
      .<TableElement>builder()
      .add(new TableElement("INTFIELD", new PrimitiveType(SqlType.INTEGER)))
      .add(new TableElement("BIGINTFIELD", new PrimitiveType(SqlType.BIGINT)))
      .add(new TableElement("FLOATFIELD", new PrimitiveType(SqlType.DOUBLE)))
      .add(new TableElement("DOUBLEFIELD", new PrimitiveType(SqlType.DOUBLE)))
      .add(new TableElement("STRINGFIELD", new PrimitiveType(SqlType.STRING)))
      .add(new TableElement("BOOLEANFIELD", new PrimitiveType(SqlType.BOOLEAN)))
      .add(new TableElement("ARRAYFIELD", new io.confluent.ksql.parser.tree.Array(
          new PrimitiveType(SqlType.INTEGER))))
      .add(new TableElement("MAPFIELD", new io.confluent.ksql.parser.tree.Map(
          new PrimitiveType(SqlType.BIGINT))))
      .add(new TableElement("STRUCTFIELD", new io.confluent.ksql.parser.tree.Struct(
          ImmutableList.of(new Pair<>("s0", new PrimitiveType(SqlType.BIGINT))))))
      .build();
  private static final int SCHEMA_ID = 5;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Statement statement;
  @Mock
  private CreateStream cs;
  @Mock
  private CreateTable ct;
  @Mock
  private TopicSchemaSupplier schemaSupplier;
  private PreparedStatement<CreateStream> csStatement;
  private PreparedStatement<CreateTable> ctStatement;

  private DefaultSchemaInjector injector;

  @Before
  public void setUp() {
    when(cs.getName()).thenReturn(QualifiedName.of("cs"));
    when(ct.getName()).thenReturn(QualifiedName.of("ct"));

    when(cs.getProperties()).thenReturn(SUPPORTED_PROPS);
    when(ct.getProperties()).thenReturn(SUPPORTED_PROPS);

    when(cs.copyWith(any(), any())).thenAnswer(inv -> setupCopy(inv, cs, mock(CreateStream.class)));
    when(ct.copyWith(any(), any())).thenAnswer(inv -> setupCopy(inv, ct, mock(CreateTable.class)));

    csStatement = PreparedStatement.of(SQL_TEXT, cs);
    ctStatement = PreparedStatement.of(SQL_TEXT, ct);

    when(schemaSupplier.getValueSchema(eq(KAFKA_TOPIC), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMA, SCHEMA_ID)));

    injector = new DefaultSchemaInjector(schemaSupplier);
  }

  @Test
  public void shouldReturnStatementUnchangedIfNotCreateStatement() {
    // Given:
    final PreparedStatement<?> prepared = PreparedStatement.of("sql", statement);

    // When:
    final PreparedStatement<?> result = injector.forStatement(prepared);

    // Then:
    assertThat(result, is(sameInstance(prepared)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCsAlreadyHasSchema() {
    // Given:
    when(cs.getElements()).thenReturn(SOME_ELEMENTS);

    // When:
    final PreparedStatement<?> result = injector.forStatement(csStatement);

    // Then:
    assertThat(result, is(sameInstance(csStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCtAlreadyHasSchema() {
    // Given:
    when(ct.getElements()).thenReturn(SOME_ELEMENTS);

    // When:
    final PreparedStatement<?> result = injector.forStatement(ctStatement);

    // Then:
    assertThat(result, is(sameInstance(ctStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCsFormatDoesNotSupportInference() {
    // Given:
    when(cs.getProperties()).thenReturn(UNSUPPORTED_PROPS);

    // When:
    final PreparedStatement<?> result = injector.forStatement(csStatement);

    // Then:
    assertThat(result, is(sameInstance(csStatement)));
  }

  @Test
  public void shouldReturnStatementUnchangedIfCtFormatDoesNotSupportInference() {
    // Given:
    when(ct.getProperties()).thenReturn(UNSUPPORTED_PROPS);

    // When:
    final PreparedStatement<?> result = injector.forStatement(ctStatement);

    // Then:
    assertThat(result, is(sameInstance(ctStatement)));
  }

  @Test
  public void shouldThrowIfMissingValueFormat() {
    // Given:
    when(cs.getProperties()).thenReturn(supportedPropsWithout("VALUE_FORMAT"));

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage(
        "VALUE_FORMAT should be set in WITH clause of CREATE STREAM/TABLE statement.");
    expectedException.expectMessage(SQL_TEXT);

    // When:
    injector.forStatement(csStatement);
  }

  @Test
  public void shouldThrowIfMissingKafkaTopicProperty() {
    // Given:
    when(ct.getProperties()).thenReturn(supportedPropsWithout("KAFKA_TOPIC"));

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage(
        "KAFKA_TOPIC should be set in WITH clause of CREATE STREAM/TABLE statement.");
    expectedException.expectMessage(SQL_TEXT);

    // When:
    injector.forStatement(ctStatement);
  }

  @Test
  public void shouldThrowIfSchemaNotRegisteredOrNotCompatible() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenReturn(SchemaResult.failure(new KsqlException("schema missing or incompatible")));

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("schema missing or incompatible");

    // When:
    injector.forStatement(ctStatement);
  }

  @Test
  public void shouldAddElementsToCsStatement() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMA, SCHEMA_ID)));

    // When:
    final PreparedStatement<CreateStream> result = injector.forStatement(csStatement);

    // Then:
    assertThat(result.getStatement().getElements(), is(EXPECTED_KSQL_SCHEMA));
  }

  @Test
  public void shouldAddElementsToCtStatement() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMA, SCHEMA_ID)));

    // When:
    final PreparedStatement<CreateTable> result = injector.forStatement(ctStatement);

    // Then:
    assertThat(result.getStatement().getElements(), is(EXPECTED_KSQL_SCHEMA));
  }

  @Test
  public void shouldBuildNewCsStatementText() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMA, SCHEMA_ID)));

    // When:
    final PreparedStatement<CreateStream> result = injector.forStatement(csStatement);

    // Then:
    assertThat(result.getStatementText(), is(
        "CREATE STREAM cs ("
            + "INTFIELD INTEGER, "
            + "BIGINTFIELD BIGINT, "
            + "FLOATFIELD DOUBLE, "
            + "DOUBLEFIELD DOUBLE, "
            + "STRINGFIELD STRING, "
            + "BOOLEANFIELD BOOLEAN, "
            + "ARRAYFIELD ARRAY<INTEGER>, "
            + "MAPFIELD MAP<VARCHAR, BIGINT>, "
            + "STRUCTFIELD STRUCT<s0 BIGINT>) "
            + "WITH (VALUE_FORMAT='avro', KAFKA_TOPIC='some-topic', AVRO_SCHEMA_ID='5');"
    ));
  }

  @Test
  public void shouldBuildNewCtStatementText() {
    // Given:
    when(schemaSupplier.getValueSchema(KAFKA_TOPIC, Optional.empty()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMA, SCHEMA_ID)));

    // When:
    final PreparedStatement<CreateTable> result = injector.forStatement(ctStatement);

    // Then:
    assertThat(result.getStatementText(), is(
        "CREATE TABLE ct ("
            + "INTFIELD INTEGER, "
            + "BIGINTFIELD BIGINT, "
            + "FLOATFIELD DOUBLE, "
            + "DOUBLEFIELD DOUBLE, "
            + "STRINGFIELD STRING, "
            + "BOOLEANFIELD BOOLEAN, "
            + "ARRAYFIELD ARRAY<INTEGER>, "
            + "MAPFIELD MAP<VARCHAR, BIGINT>, "
            + "STRUCTFIELD STRUCT<s0 BIGINT>) "
            + "WITH (VALUE_FORMAT='avro', KAFKA_TOPIC='some-topic', AVRO_SCHEMA_ID='5');"
    ));
  }

  @Test
  public void shouldBuildNewCsStatementTextFromId() {
    // Given:
    when(cs.getProperties()).thenReturn(supportedPropsWith("AVRO_SCHEMA_ID", "42"));

    when(schemaSupplier.getValueSchema(KAFKA_TOPIC, Optional.of(42)))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMA, SCHEMA_ID)));

    // When:
    final PreparedStatement<CreateStream> result = injector.forStatement(csStatement);

    // Then:
    assertThat(result.getStatementText(), is(
        "CREATE STREAM cs ("
            + "INTFIELD INTEGER, "
            + "BIGINTFIELD BIGINT, "
            + "FLOATFIELD DOUBLE, "
            + "DOUBLEFIELD DOUBLE, "
            + "STRINGFIELD STRING, "
            + "BOOLEANFIELD BOOLEAN, "
            + "ARRAYFIELD ARRAY<INTEGER>, "
            + "MAPFIELD MAP<VARCHAR, BIGINT>, "
            + "STRUCTFIELD STRUCT<s0 BIGINT>) "
            + "WITH (VALUE_FORMAT='avro', KAFKA_TOPIC='some-topic', AVRO_SCHEMA_ID='42');"
    ));
  }

  @Test
  public void shouldBuildNewCtStatementTextFromId() {
    // Given:
    when(ct.getProperties()).thenReturn(supportedPropsWith("AVRO_SCHEMA_ID", "42"));

    when(schemaSupplier.getValueSchema(KAFKA_TOPIC, Optional.of(42)))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMA, SCHEMA_ID)));

    // When:
    final PreparedStatement<CreateTable> result = injector.forStatement(ctStatement);

    // Then:
    assertThat(result.getStatementText(), is(
        "CREATE TABLE ct ("
            + "INTFIELD INTEGER, "
            + "BIGINTFIELD BIGINT, "
            + "FLOATFIELD DOUBLE, "
            + "DOUBLEFIELD DOUBLE, "
            + "STRINGFIELD STRING, "
            + "BOOLEANFIELD BOOLEAN, "
            + "ARRAYFIELD ARRAY<INTEGER>, "
            + "MAPFIELD MAP<VARCHAR, BIGINT>, "
            + "STRUCTFIELD STRUCT<s0 BIGINT>) "
            + "WITH (VALUE_FORMAT='avro', KAFKA_TOPIC='some-topic', AVRO_SCHEMA_ID='42');"
    ));
  }

  @Test
  public void shouldAddSchemaIdIfNotPresentAlready() {
    // Given:
    when(schemaSupplier.getValueSchema(KAFKA_TOPIC, Optional.empty()))
        .thenReturn(SchemaResult.success(schemaAndId(SUPPORTED_SCHEMA, SCHEMA_ID)));

    // When:
    final PreparedStatement<CreateStream> result = injector.forStatement(csStatement);

    // Then:
    assertThat(result.getStatement().getProperties().get("AVRO_SCHEMA_ID"),
        is(new StringLiteral(String.valueOf(SCHEMA_ID))));

    assertThat(result.getStatementText(), containsString("AVRO_SCHEMA_ID='5'"));
  }

  @Test
  public void shouldNotOverwriteExistingSchemaId() {
    // Given:
    when(cs.getProperties()).thenReturn(supportedPropsWith("AVRO_SCHEMA_ID", "42"));

    // When:
    final PreparedStatement<CreateStream> result = injector.forStatement(csStatement);

    // Then:
    assertThat(result.getStatement().getProperties().get("AVRO_SCHEMA_ID"),
        is(new StringLiteral("42")));

    assertThat(result.getStatementText(), containsString("AVRO_SCHEMA_ID='42'"));
  }

  @Test
  public void shouldThrowOnUnsupportedType() {
    for (final Schema unsupportedSchema : UNSUPPORTED_SCHEMAS) {
      // Given:
      when(schemaSupplier.getValueSchema(any(), any()))
          .thenReturn(SchemaResult.success(schemaAndId(unsupportedSchema, SCHEMA_ID)));

      try {
        // When:
        injector.forStatement(ctStatement);

        // Then:
        fail("Expected KsqlStatementException. schema: " + unsupportedSchema);
      } catch (final KsqlStatementException e) {
        assertThat(e.getRawMessage(),
            containsString("Failed to convert schema to KSQL model:"));

        assertThat(e.getSqlStatement(), is(csStatement.getStatementText()));
      }
    }
  }

  @Test
  public void shouldThrowIfSchemaSupplierThrows() {
    // Given:
    when(schemaSupplier.getValueSchema(any(), any()))
        .thenThrow(new KsqlException("Oh no!"));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expect(not(instanceOf(KsqlStatementException.class)));
    expectedException.expectMessage("Oh no");

    // When:
    injector.forStatement(csStatement);
  }

  @SuppressWarnings("SameParameterValue")
  private static Map<String, Expression> supportedPropsWith(
      final String property,
      final String value
  ) {
    final HashMap<String, Expression> props = new HashMap<>(SUPPORTED_PROPS);
    props.put(property, new StringLiteral(value));
    return props;
  }

  private static Map<String, Expression> supportedPropsWithout(final String property) {
    final HashMap<String, Expression> props = new HashMap<>(SUPPORTED_PROPS);
    assertThat("Invalid test", props.remove(property), is(notNullValue()));
    return props;
  }

  private static Object setupCopy(
      final InvocationOnMock inv,
      final AbstractStreamCreateStatement source,
      final AbstractStreamCreateStatement mock
  ) {
    final QualifiedName name = source.getName();
    when(mock.getName()).thenReturn(name);
    when(mock.getElements()).thenReturn(inv.getArgument(0));
    when(mock.accept(any(), any())).thenCallRealMethod();
    when(mock.getProperties()).thenReturn(inv.getArgument(1));
    return mock;
  }
}