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

package io.confluent.ksql.engine;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class InsertValuesExecutorTest {

  private static final ColumnName K0 = ColumnName.of("k0");
  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName COL1 = ColumnName.of("COL1");
  private static final ColumnName INT_COL = ColumnName.of("INT");

  private static final LogicalSchema SINGLE_VALUE_COLUMN_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING)
      .build();

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING)
      .valueColumn(COL1, SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema BIG_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING) // named COL0 for auto-ROWKEY
      .valueColumn(INT_COL, SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("BIGINT"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("DOUBLE"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("BOOLEAN"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("VARCHAR"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("DECIMAL"), SqlTypes.decimal(2, 1))
      .build();

  private static final byte[] KEY = new byte[]{1};
  private static final byte[] VALUE = new byte[]{2};

  private static final String TOPIC_NAME = "topic";

  @Mock
  private KsqlEngine engine;
  @Mock
  private Serde<Struct> keySerDe;
  @Mock
  private Serializer<Struct> keySerializer;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Serializer<GenericRow> valueSerializer;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private Future<?> producerResultFuture;
  @Mock
  private KafkaProducer<byte[], byte[]> producer;
  @Mock
  private LongSupplier clock;
  @Mock
  private ValueSerdeFactory valueSerdeFactory;
  @Mock
  private KeySerdeFactory keySerdeFactory;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  private InsertValuesExecutor executor;

  @Before
  public void setup() {
    when(keySerDe.serializer()).thenReturn(keySerializer);
    when(valueSerde.serializer()).thenReturn(valueSerializer);

    when(keySerializer.serialize(any(), any())).thenReturn(KEY);
    when(valueSerializer.serialize(any(), any())).thenReturn(VALUE);

    doReturn(producerResultFuture).when(producer).send(any());

    final KafkaClientSupplier kafkaClientSupplier = mock(KafkaClientSupplier.class);
    when(kafkaClientSupplier.getProducer(any())).thenReturn(producer);

    when(serviceContext.getKafkaClientSupplier()).thenReturn(kafkaClientSupplier);
    when(serviceContext.getSchemaRegistryClientFactory()).thenReturn(srClientFactory);

    givenSourceStreamWithSchema(SCHEMA, SerdeOptions.of());

    when(valueSerdeFactory.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(valueSerde);

    when(keySerdeFactory.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(keySerDe);

    when(clock.getAsLong()).thenReturn(1L);

    executor = new InsertValuesExecutor(clock, keySerdeFactory, valueSerdeFactory);
  }

  @Test
  public void shouldHandleFullRow() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allColumnNames(SCHEMA),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("key"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldInsertWrappedSingleField() {
    // Given:
    givenSourceStreamWithSchema(SINGLE_VALUE_COLUMN_SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        valueColumnNames(SINGLE_VALUE_COLUMN_SCHEMA),
        ImmutableList.of(new StringLiteral("new"))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct(null));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("new"));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldInsertUnwrappedSingleField() {
    // Given:
    givenSourceStreamWithSchema(
        SINGLE_VALUE_COLUMN_SCHEMA,
        SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES)
    );

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allColumnNames(SINGLE_VALUE_COLUMN_SCHEMA),
        ImmutableList.of(new StringLiteral("newKey"), new StringLiteral("newCol0"))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("newKey"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("newCol0"));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInRowtime() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, COL0, COL1),
        ImmutableList.of(
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleRowTimeWithoutKey() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(SystemColumns.ROWTIME_NAME, COL0, COL1),
        ImmutableList.of(
            new LongLiteral(1234L),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct(null));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1234L, KEY, VALUE));
  }

  @Test
  public void shouldFillInFullRowWithNoSchema() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(),
        ImmutableList.of(
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInMissingColumnsWithNulls() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, COL0),
        ImmutableList.of(
            new StringLiteral("str"),
            new StringLiteral("str"))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", null));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleOutOfOrderSchema() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL1, COL0, K0),
        ImmutableList.of(
            new LongLiteral(2L),
            new StringLiteral("str"),
            new StringLiteral("key")
        )
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("key"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleAllSortsOfLiterals() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL1, COL0),
        ImmutableList.of(
            new LongLiteral(2L),
            new StringLiteral("str"))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleNullKeyForSourceWithKeyField() {
    // Given:
    givenSourceStreamWithSchema(BIG_SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allAndPseudoColumnNames(BIG_SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new IntegerLiteral(0),
            new LongLiteral(2),
            new DoubleLiteral(3.0),
            new BooleanLiteral("TRUE"),
            new StringLiteral("str"),
            new DecimalLiteral(new BigDecimal("1.2")))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer)
        .serialize(TOPIC_NAME, genericRow(
            "str", 0, 2L, 3.0, true, "str", new BigDecimal("1.2", new MathContext(2)))
        );

    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleNegativeValueExpression() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL0, COL1),
        ImmutableList.of(
            new StringLiteral("str"),
            ArithmeticUnaryExpression.negative(Optional.empty(), new LongLiteral(1))
        )
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct(null));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", -1L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleUdfs() {
    // Given:
    givenSourceStreamWithSchema(SINGLE_VALUE_COLUMN_SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL0),
        ImmutableList.of(
            new FunctionCall(
                FunctionName.of("SUBSTRING"),
                ImmutableList.of(new StringLiteral("foo"), new IntegerLiteral(2))))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("oo"));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleNestedUdfs() {
    // Given:
    givenSourceStreamWithSchema(SINGLE_VALUE_COLUMN_SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL0),
        ImmutableList.of(
            new FunctionCall(
                FunctionName.of("SUBSTRING"),
                ImmutableList.of(
                    new FunctionCall(
                        FunctionName.of("SUBSTRING"),
                        ImmutableList.of(new StringLiteral("foo"), new IntegerLiteral(2))
                    ),
                    new IntegerLiteral(2))
            ))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("o"));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldAllowUpcast() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL0, COL1),
        ImmutableList.of(
            new StringLiteral("str"),
            new IntegerLiteral(1)
        )
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 1L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldThrowWhenInsertValuesOnReservedInternalTopic() {
    // Given
    givenDataSourceWithSchema("_confluent-ksql-default__command-topic", SCHEMA,
        SerdeOptions.of(), false);

    final ConfiguredStatement<InsertValues> statement = ConfiguredStatement.of(
        PreparedStatement.of(
            "",
            new InsertValues(SourceName.of("TOPIC"),
                allAndPseudoColumnNames(SCHEMA),
                ImmutableList.of(
                    new LongLiteral(1L),
                    new StringLiteral("str"),
                    new StringLiteral("str"),
                    new LongLiteral(2L)
                ))),
        ImmutableMap.of(),
        new KsqlConfig(ImmutableMap.of())
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot insert values into read-only topic: _confluent-ksql-default__command-topic"));
  }

  @Test
  public void shouldThrowWhenInsertValuesOnProcessingLogTopic() {
    // Given
    givenDataSourceWithSchema("default_ksql_processing_log", SCHEMA,
        SerdeOptions.of(), false);

    final ConfiguredStatement<InsertValues> statement = ConfiguredStatement.of(
        PreparedStatement.of(
            "",
            new InsertValues(SourceName.of("TOPIC"),
                allAndPseudoColumnNames(SCHEMA),
                ImmutableList.of(
                    new LongLiteral(1L),
                    new StringLiteral("str"),
                    new StringLiteral("str"),
                    new LongLiteral(2L)
                ))),
        ImmutableMap.of(),
        new KsqlConfig(ImmutableMap.of())
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot insert values into read-only topic: default_ksql_processing_log"));
  }

  @Test
  public void shouldThrowOnProducerSendError() throws ExecutionException, InterruptedException {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allAndPseudoColumnNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    final Future<?> failure = mock(Future.class);
    when(failure.get()).thenThrow(ExecutionException.class);
    doReturn(failure).when(producer).send(any());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Failed to insert values into "));
  }

  @Test
  public void shouldThrowOnSerializingKeyError() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allAndPseudoColumnNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );
    when(keySerializer.serialize(any(), any())).thenThrow(new SerializationException("Jibberish!"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString("Could not serialize key"))));
  }

  @Test
  public void shouldThrowOnSerializingValueError() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allAndPseudoColumnNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );
    when(valueSerializer.serialize(any(), any()))
        .thenThrow(new SerializationException("Jibberish!"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString("Could not serialize row"))));
  }

  @Test
  public void shouldThrowOnTopicAuthorizationException() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allAndPseudoColumnNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );
    doThrow(new TopicAuthorizationException(Collections.singleton("t1")))
        .when(producer).send(any());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(
        containsString("Authorization denied to Write on topic(s): [t1]"))));
  }

  @Test
  public void shouldThrowIfNotEnoughValuesSuppliedWithNoSchema() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(),
        ImmutableList.of(
            new LongLiteral(1L))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString("Expected a value for each column"))));
  }

  @Test
  public void shouldThrowIfColumnDoesNotExistInSchema() {
    // Given:
    givenSourceStreamWithSchema(SINGLE_VALUE_COLUMN_SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(
            K0,
            ColumnName.of("NONEXISTENT")),
        ImmutableList.of(
            new StringLiteral("foo"),
            new StringLiteral("bar"))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString("Column name `NONEXISTENT` does not exist."))));
  }

  @Test
  public void shouldFailOnDowncast() {
    // Given:
    givenSourceStreamWithSchema(BIG_SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(INT_COL),
        ImmutableList.of(
            new DoubleLiteral(1.1)
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString("Expected type INTEGER for field"))));
  }

  @Test
  public void shouldHandleStreamsWithNoKeyField() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, COL0, COL1),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("key"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleTablesWithNoKeyField() {
    // Given:
    givenSourceTableWithSchema(SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, COL0, COL1),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("key"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleStreamsWithNoKeyFieldAndNoRowKeyProvided() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA, SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL0, COL1),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct(null));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldThrowOnTablesWithNoKeyFieldAndNoRowKeyProvided() {
    // Given:
    givenSourceTableWithSchema(SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL0, COL1),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Failed to insert values into 'TOPIC'. Value for primary key column(s) k0 is required for tables"));
  }

  @Test
  public void shouldThrowOnTablesWithKeyFieldAndNullKeyFieldValueProvided() {
    // Given:
    givenSourceTableWithSchema(SerdeOptions.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL1),
        ImmutableList.of(
            new LongLiteral(2L))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Failed to insert values into 'TOPIC'. Value for primary key column(s) k0 is required for tables"));
  }

  @Test
  public void shouldBuildCorrectSerde() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        valueColumnNames(SCHEMA),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerdeFactory).create(
        FormatInfo.of(FormatFactory.KAFKA.name()),
        PersistenceSchema.from(SCHEMA.keyConnectSchema(), false),
        new KsqlConfig(ImmutableMap.of()),
        srClientFactory,
        "",
        NoopProcessingLogContext.INSTANCE
    );

    verify(valueSerdeFactory).create(
        FormatInfo.of(FormatFactory.JSON.name()),
        PersistenceSchema.from(SCHEMA.valueConnectSchema(), false),
        new KsqlConfig(ImmutableMap.of()),
        srClientFactory,
        "",
        NoopProcessingLogContext.INSTANCE
    );
  }

  private static ConfiguredStatement<InsertValues> givenInsertValues(
      final List<ColumnName> columns,
      final List<Expression> values
  ) {
    return ConfiguredStatement.of(
        PreparedStatement.of(
            "",
            new InsertValues(SourceName.of("TOPIC"), columns, values)),
        ImmutableMap.of(),
        new KsqlConfig(ImmutableMap.of())
    );
  }

  private void givenSourceStreamWithSchema(
      final LogicalSchema schema,
      final SerdeOptions serdeOptions
  ) {
    givenDataSourceWithSchema(TOPIC_NAME, schema, serdeOptions, false);
  }

  private void givenSourceTableWithSchema(
      final SerdeOptions serdeOptions
  ) {
    givenDataSourceWithSchema(TOPIC_NAME, SCHEMA, serdeOptions, true);
  }

  private void givenDataSourceWithSchema(
      final String topicName,
      final LogicalSchema schema,
      final SerdeOptions serdeOptions,
      final boolean table
  ) {
    final KsqlTopic topic = new KsqlTopic(
        topicName,
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()))
    );

    final DataSource dataSource;
    if (table) {
      dataSource = new KsqlTable<>(
          "",
          SourceName.of("TOPIC"),
          schema,
          serdeOptions,
          Optional.empty(),
          false,
          topic
      );
    } else {
      dataSource = new KsqlStream<>(
          "",
          SourceName.of("TOPIC"),
          schema,
          serdeOptions,
          Optional.empty(),
          false,
          topic
      );
    }

    final MetaStoreImpl metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    metaStore.putSource(dataSource, false);

    when(engine.getMetaStore()).thenReturn(metaStore);
  }

  private static Struct keyStruct(final String rowKey) {
    final Struct key = new Struct(SCHEMA.keyConnectSchema());
    key.put(Iterables.getOnlyElement(SCHEMA.key()).name().text(), rowKey);
    return key;
  }

  private static List<ColumnName> valueColumnNames(final LogicalSchema schema) {
    return schema.value().stream()
        .map(Column::name)
        .collect(ImmutableList.toImmutableList());
  }

  private static List<ColumnName> allColumnNames(final LogicalSchema schema) {
    return schema.columns().stream()
        .map(Column::name)
        .collect(ImmutableList.toImmutableList());
  }

  private static List<ColumnName> allAndPseudoColumnNames(final LogicalSchema schema) {
    final Builder<ColumnName> builder = ImmutableList.<ColumnName>builder()
        .add(SystemColumns.ROWTIME_NAME);

    schema.columns().stream()
        .map(Column::name)
        .forEach(builder::add);

    return builder.build();
  }
}
