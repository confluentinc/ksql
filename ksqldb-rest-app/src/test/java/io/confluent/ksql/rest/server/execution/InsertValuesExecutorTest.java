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

package io.confluent.ksql.rest.server.execution;

import static io.confluent.ksql.GenericKey.genericKey;
import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
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
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.avro.AvroProperties;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class InsertValuesExecutorTest {

  private static final ColumnName K0 = ColumnName.of("k0");
  private static final ColumnName K1 = ColumnName.of("k1");
  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName COL1 = ColumnName.of("COL1");
  private static final ColumnName INT_COL = ColumnName.of("INT");
  private static final ColumnName HEAD0 = ColumnName.of("HEAD0");
  private static final ColumnName HEAD1 = ColumnName.of("HEAD1");

  private static final LogicalSchema SINGLE_VALUE_COLUMN_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING)
      .build();

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING)
      .valueColumn(COL1, SqlTypes.BIGINT)
      .build();

  private static final String RAW_SCHEMA = "{\"type\":\"record\","
      + "\"name\":\"KsqlDataSourceSchema\","
      + "\"namespace\":\"io.confluent.ksql.avro_schemas\","
      + "\"fields\":["
      + "{\"name\":\"k0\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"k1\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

  private static final String AVRO_RAW_ONE_KEY_SCHEMA =
      "{\"type\":\"record\","
      + "\"name\":\"KsqlDataSourceSchema\","
      + "\"namespace\":\"io.confluent.ksql.avro_schemas\","
      + "\"fields\":["
      + "{\"name\":\"k0\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

  private static final String AVRO_RAW_ONE_KEY_SCHEMA_WITH_CUSTOM_METADATA =
      "{\"type\":\"record\","
          + "\"name\":\"MyRecord\","
          + "\"namespace\":\"io.xyz.records\","
          + "\"fields\":["
          + "{\"name\":\"k0\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

  private static final LogicalSchema SCHEMA_WITH_MUTI_KEYS = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .keyColumn(K1, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING)
      .valueColumn(COL1, SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema SCHEMA_WITH_HEADERS = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING)
      .valueColumn(COL1, SqlTypes.BIGINT)
      .headerColumn(HEAD0, Optional.empty())
      .build();

  private static final LogicalSchema SCHEMA_WITH_KEY_HEADERS = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING)
      .valueColumn(COL1, SqlTypes.BIGINT)
      .headerColumn(HEAD0, Optional.of("a"))
      .headerColumn(HEAD1, Optional.of("b"))
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
  private Serde<GenericKey> keySerDe;
  @Mock
  private Serializer<GenericKey> keySerializer;
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
  @Mock
  private SchemaRegistryClient srClient;

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
    when(serviceContext.getSchemaRegistryClient()).thenReturn(srClient);

    givenSourceStreamWithSchema(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

    when(valueSerdeFactory.create(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(valueSerde);

    when(keySerdeFactory.create(any(), any(), any(), any(), any(), any(), any()))
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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("key"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldInsertWrappedSingleField() {
    // Given:
    givenSourceStreamWithSchema(SINGLE_VALUE_COLUMN_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        valueColumnNames(SINGLE_VALUE_COLUMN_SCHEMA),
        ImmutableList.of(new StringLiteral("new"))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, genericKey((String) null));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("new"));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldInsertUnwrappedSingleField() {
    // Given:
    givenSourceStreamWithSchema(
        SINGLE_VALUE_COLUMN_SCHEMA,
        SerdeFeatures.of(),
        SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
    );

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allColumnNames(SINGLE_VALUE_COLUMN_SCHEMA),
        ImmutableList.of(new StringLiteral("newKey"), new StringLiteral("newCol0"))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("newKey"));
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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("str"));
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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey((String) null));
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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("str"));
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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("str"));
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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("key"));
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
    givenSourceStreamWithSchema(BIG_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("str"));
    verify(valueSerializer)
        .serialize(TOPIC_NAME, genericRow(
            "str", 0, 2L, 3.0, true, "str", new BigDecimal("1.2", new MathContext(2)))
        );

    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleNegativeValueExpression() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey((String) null));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", -1L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleUdfs() {
    // Given:
    givenSourceStreamWithSchema(SINGLE_VALUE_COLUMN_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

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
    givenSourceStreamWithSchema(SINGLE_VALUE_COLUMN_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

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
    givenSourceStreamWithSchema(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

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
        SerdeFeatures.of(), SerdeFeatures.of(), false, false);

    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
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
        SessionConfig.of(ksqlConfig, ImmutableMap.of())
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
        SerdeFeatures.of(), SerdeFeatures.of(), false, false);

    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
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
        SessionConfig.of(ksqlConfig, ImmutableMap.of())
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
    assertThat(e.getCause(), (hasMessage(containsString("Could not serialize value"))));
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
  public void shouldThrowOnClusterAuthorizationException() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allAndPseudoColumnNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );
    doThrow(new ClusterAuthorizationException("Cluster authorization failed"))
        .when(producer).send(any());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(
        containsString("Authorization denied to Write on topic(s): [" + TOPIC_NAME + "]. "
            + "Caused by: The producer is not authorized to do idempotent sends. "
            + "Check that you have write permissions to the specified topic, "
            + "and disable idempotent sends by setting 'enable.idempotent=false' "
            + " if necessary."))));
  }

  @Test
  public void shouldThrowOnClusterAuthorizationExceptionWrappedInKafkaException() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allAndPseudoColumnNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );
    doThrow(new KafkaException(
        "Cannot execute transactional method because we are in an error state",
        new ClusterAuthorizationException("Cluster authorization failed"))
    ).when(producer).send(any());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(
        containsString("Authorization denied to Write on topic(s): [" + TOPIC_NAME + "]. "
            + "Caused by: The producer is not authorized to do idempotent sends. "
            + "Check that you have write permissions to the specified topic, "
            + "and disable idempotent sends by setting 'enable.idempotent=false' "
            + " if necessary."))));
  }

  @Test
  public void shouldThrowOnOtherException() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allAndPseudoColumnNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );
    doThrow(new RuntimeException("boom"))
        .when(producer).send(any());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(containsString("boom"))));
  }

  @Test
  public void shouldThrowOnOtherExceptionWrappedInKafkaException() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allAndPseudoColumnNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );
    doThrow(new KafkaException(
        "Cannot execute transactional method because we are in an error state",
        new RuntimeException("boom"))
    ).when(producer).send(any());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(
        containsString("Cannot execute transactional method because we are in an error state"))));
  }

  @Test
  public void shouldThrowWhenInsertingValuesOnSourceTable() {
    // Given:
    givenDataSourceWithSchema("source_table_1", SCHEMA,
        SerdeFeatures.of(), SerdeFeatures.of(), true, true);
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
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
        SessionConfig.of(ksqlConfig, ImmutableMap.of())
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Cannot insert values into read-only table: TOPIC"));
  }

  @Test
  public void shouldThrowWhenInsertingValuesOnSourceStream() {
    // Given:
    givenDataSourceWithSchema("source_stream_1", SCHEMA,
        SerdeFeatures.of(), SerdeFeatures.of(), false, true);
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
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
        SessionConfig.of(ksqlConfig, ImmutableMap.of())
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Cannot insert values into read-only stream: TOPIC"));
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
    givenSourceStreamWithSchema(SINGLE_VALUE_COLUMN_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

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
    givenSourceStreamWithSchema(BIG_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

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
    givenSourceStreamWithSchema(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("key"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleTablesWithNoKeyField() {
    // Given:
    givenSourceTableWithSchema(SerdeFeatures.of(), SerdeFeatures.of());

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
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("key"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleStreamsWithNoKeyFieldAndNoRowKeyProvided() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(COL0, COL1),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, genericKey((String) null));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("str", 2L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldThrowOnTablesWithNoKeyFieldAndNoRowKeyProvided() {
    // Given:
    givenSourceTableWithSchema(SerdeFeatures.of(), SerdeFeatures.of());

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
    givenSourceTableWithSchema(SerdeFeatures.of(), SerdeFeatures.of());

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
  public void shouldAllowInsertOnMultipleKeySchemaDefinitions() throws Exception {
    final String protoMultiSchema = "syntax = \"proto3\";\n"
        + "package io.proto;\n"
        + "\n"
        + "message SingleKey {\n"
        + "  string k0 = 1;\n"
        + "}\n"
        + "message MultiKeys {\n"
        + "  string k0 = 1;\n"
        + "  string k1 = 2;\n"
        + "}\n";

    // Given:
    when(srClient.getLatestSchemaMetadata(Mockito.any()))
        .thenReturn(new SchemaMetadata(1, 1, protoMultiSchema));
    when(srClient.getSchemaById(1))
        .thenReturn(new ProtobufSchema(protoMultiSchema));
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA_WITH_MUTI_KEYS,
        SerdeFeatures.of(SerdeFeature.SCHEMA_INFERENCE),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.PROTOBUF.name(), ImmutableMap.of(
            AvroProperties.FULL_SCHEMA_NAME,"io.proto.MultiKeys",
            AvroProperties.SCHEMA_ID, "1"
        )),
        FormatInfo.of(FormatFactory.JSON.name()),
        false,
        false);

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, K1, COL0, COL1),
        ImmutableList.of(
            new StringLiteral("K0"),
            new StringLiteral("K1"),
            new StringLiteral("V0"),
            new LongLiteral(21))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("K0", "K1"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("V0", 21L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldNotAllowInsertOnMultipleKeySchemaDefinitionsWithDifferentOrder() throws Exception {
    final String protoMultiSchema = "syntax = \"proto3\";\n"
        + "package io.proto;\n"
        + "\n"
        + "message SingleKey {\n"
        + "  string k0 = 1;\n"
        + "}\n"
        + "message MultiKeys {\n"
        + "  string k1 = 1;\n"
        + "  string k0 = 2;\n"
        + "}\n";

    // Given:
    when(srClient.getLatestSchemaMetadata(Mockito.any()))
        .thenReturn(new SchemaMetadata(1, 1, protoMultiSchema));
    when(srClient.getSchemaById(1))
        .thenReturn(new ProtobufSchema(protoMultiSchema));
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA_WITH_MUTI_KEYS,
        SerdeFeatures.of(SerdeFeature.SCHEMA_INFERENCE),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.PROTOBUF.name(), ImmutableMap.of(
            AvroProperties.FULL_SCHEMA_NAME,"io.proto.MultiKeys",
            AvroProperties.SCHEMA_ID, "1"
        )),
        FormatInfo.of(FormatFactory.JSON.name()),
        false,
        false);

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K1, K0, COL0, COL1),
        ImmutableList.of(
            new StringLiteral("K1"),
            new StringLiteral("K0"),
            new StringLiteral("V0"),
            new LongLiteral(21))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString("ksqlDB generated schema would overwrite existing key schema"));
    assertThat(e.getMessage(), containsString("Existing Schema: [`K1` STRING, `K0` STRING]"));
    assertThat(e.getMessage(), containsString("ksqlDB Generated: [`k0` STRING KEY, `k1` STRING KEY]"));
  }

  @Test
  public void shouldNotAllowInsertWhenSchemaMetadataIsNull() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(Mockito.any()))
        .thenReturn(null);
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA_WITH_MUTI_KEYS,
        SerdeFeatures.of(SerdeFeature.SCHEMA_INFERENCE),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.PROTOBUF.name(), ImmutableMap.of(
            AvroProperties.FULL_SCHEMA_NAME,"io.proto.MultiKeys",
            AvroProperties.SCHEMA_ID, "1"
        )),
        FormatInfo.of(FormatFactory.JSON.name()),
        false,
        false);

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K1, K0, COL0, COL1),
        ImmutableList.of(
            new StringLiteral("K1"),
            new StringLiteral("K0"),
            new StringLiteral("V0"),
            new LongLiteral(21))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString("Failed to insert values into 'TOPIC'. Failed to fetch key schema (topic-key). " +
            "Please check if schema exists in Schema Registry and/or check connection with Schema Registry.")
    );
  }

  @Test
  public void shouldIgnoreConnectNameComparingKeySchema() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(Mockito.any()))
        .thenReturn(new SchemaMetadata(1, 1, RAW_SCHEMA));
    when(srClient.getSchemaById(1))
        .thenReturn(new AvroSchema(RAW_SCHEMA));
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA_WITH_MUTI_KEYS,
        SerdeFeatures.of(SerdeFeature.SCHEMA_INFERENCE),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.AVRO.name()),
        FormatInfo.of(FormatFactory.AVRO.name()),
        false,
        false);

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, K1, COL0, COL1),
        ImmutableList.of(
            new StringLiteral("k0"),
            new StringLiteral("k1"),
            new StringLiteral("v0"),
            new LongLiteral(21))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("k0", "k1"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("v0", 21L));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldSupportInsertIntoWithSchemaInferenceMatch() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(Mockito.any()))
        .thenReturn(new SchemaMetadata(1, 1, ""));
    when(srClient.getSchemaById(1))
        .thenReturn(new AvroSchema(AVRO_RAW_ONE_KEY_SCHEMA));
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA,
        SerdeFeatures.of(SerdeFeature.SCHEMA_INFERENCE, SerdeFeature.WRAP_SINGLES),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.AVRO.name()),
        FormatInfo.of(FormatFactory.AVRO.name()),
        false,
        false);

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, COL0),
        ImmutableList.of(
            new StringLiteral("foo"),
            new StringLiteral("bar"))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("foo"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("bar", null));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldSupportInsertIntoWithSchemaInferenceMatchAndCustomMetadata() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(Mockito.any()))
        .thenReturn(new SchemaMetadata(1, 1, ""));
    when(srClient.getSchemaById(1))
        .thenReturn(new AvroSchema(AVRO_RAW_ONE_KEY_SCHEMA_WITH_CUSTOM_METADATA));
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA,
        SerdeFeatures.of(SerdeFeature.SCHEMA_INFERENCE, SerdeFeature.WRAP_SINGLES),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.AVRO.name()),
        FormatInfo.of(FormatFactory.AVRO.name()),
        false,
        false);

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, COL0),
        ImmutableList.of(
            new StringLiteral("foo"),
            new StringLiteral("bar"))
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, genericKey("foo"));
    verify(valueSerializer).serialize(TOPIC_NAME, genericRow("bar", null));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldThrowOnSchemaInferenceMismatchForKey() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(Mockito.any()))
        .thenReturn(new SchemaMetadata(1, 1, ""));
    when(srClient.getSchemaById(1))
        .thenReturn(new AvroSchema(RAW_SCHEMA));
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA,
        SerdeFeatures.of(SerdeFeature.SCHEMA_INFERENCE),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.AVRO.name()),
        FormatInfo.of(FormatFactory.AVRO.name()),
        false,
        false);

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, COL0),
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
    assertThat(e.getMessage(), containsString("ksqlDB generated schema would overwrite existing key schema"));
    assertThat(e.getMessage(), containsString("Existing Schema: [`K0` STRING, `K1` STRING]"));
    assertThat(e.getMessage(), containsString("ksqlDB Generated: [`k0` STRING KEY]"));
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
        PersistenceSchema.from(SCHEMA.key(), SerdeFeatures.of()),
        new KsqlConfig(ImmutableMap.of()),
        srClientFactory,
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    );

    verify(valueSerdeFactory).create(
        FormatInfo.of(FormatFactory.JSON.name()),
        PersistenceSchema.from(SCHEMA.value(), SerdeFeatures.of()),
        new KsqlConfig(ImmutableMap.of()),
        srClientFactory,
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    );
  }

  @Test
  public void shouldThrowWhenNotAuthorizedToReadKeySchemaFromSR() throws Exception {
    // Given
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA,
        SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.AVRO.name()),
        FormatInfo.of(FormatFactory.AVRO.name()),
        false,
        false);
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allColumnNames(SCHEMA),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );
    when(srClient.getLatestSchemaMetadata(TOPIC_NAME + "-key")).thenThrow(
        new RestClientException("User is denied operation Read on topic-key", 401, 1)
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Authorization denied to Read on Schema Registry subject: ["
            + KsqlConstants.getSRSubject(TOPIC_NAME, true)));
  }

  @Test
  public void shouldThrowWhenNotAuthorizedToWriteKeySchemaToSR() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(Mockito.any()))
        .thenReturn(new SchemaMetadata(1, 1, AVRO_RAW_ONE_KEY_SCHEMA));
    when(srClient.getSchemaById(1))
        .thenReturn(new AvroSchema(AVRO_RAW_ONE_KEY_SCHEMA));
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA,
        SerdeFeatures.of(SerdeFeature.WRAP_SINGLES),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.AVRO.name()),
        FormatInfo.of(FormatFactory.AVRO.name()),
        false,
        false);
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allColumnNames(SCHEMA),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );
    when(keySerializer.serialize(any(), any())).thenThrow(
        new RuntimeException(new RestClientException("foo", 401, 1))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Authorization denied to Write on Schema Registry subject: ["
            + KsqlConstants.getSRSubject(TOPIC_NAME, true)));
  }

  @Test
  public void shouldThrowWhenNotAuthorizedToWriteValSchemaToSR() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(Mockito.any()))
        .thenReturn(new SchemaMetadata(1, 1, AVRO_RAW_ONE_KEY_SCHEMA));
    when(srClient.getSchemaById(1))
        .thenReturn(new AvroSchema(AVRO_RAW_ONE_KEY_SCHEMA));
    givenDataSourceWithSchema(
        TOPIC_NAME,
        SCHEMA,
        SerdeFeatures.of(SerdeFeature.WRAP_SINGLES),
        SerdeFeatures.of(),
        FormatInfo.of(FormatFactory.AVRO.name()),
        FormatInfo.of(FormatFactory.AVRO.name()),
        false,
        false);
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allColumnNames(SCHEMA),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );
    when(valueSerializer.serialize(any(), any())).thenThrow(
        new RuntimeException(new RestClientException("foo", 401, 1))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Authorization denied to Write on Schema Registry subject: ["
            + KsqlConstants.getSRSubject(TOPIC_NAME, false)));
  }

  @Test
  public void shouldThrowOnInsertHeaders() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA_WITH_HEADERS, SerdeFeatures.of(), SerdeFeatures.of());
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allColumnNames(SCHEMA_WITH_HEADERS),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L),
            new NullLiteral()
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), is("Cannot insert into HEADER columns: HEAD0"));
  }

  @Test
  public void shouldThrowOnInsertKeyHeaders() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA_WITH_KEY_HEADERS, SerdeFeatures.of(), SerdeFeatures.of());
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allColumnNames(SCHEMA_WITH_KEY_HEADERS),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L),
            new NullLiteral(),
            new NullLiteral()
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), is("Cannot insert into HEADER columns: HEAD0, HEAD1"));
  }

  @Test
  public void shouldInsertValuesIntoHeaderSchemaValueColumns() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA_WITH_HEADERS, SerdeFeatures.of(), SerdeFeatures.of());
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(K0, COL0, COL1),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, mock(SessionProperties.class), engine, serviceContext);

    // Then:
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldThrowOnInsertAllWithHeaders() {
    // Given:
    givenSourceStreamWithSchema(SCHEMA_WITH_HEADERS, SerdeFeatures.of(), SerdeFeatures.of());
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L),
            new NullLiteral()
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> executor.execute(statement, mock(SessionProperties.class), engine, serviceContext)
    );

    // Then:
    assertThat(e.getMessage(), is("Cannot insert into HEADER columns: HEAD0"));
  }

  private static ConfiguredStatement<InsertValues> givenInsertValues(
      final List<ColumnName> columns,
      final List<Expression> values
  ) {
    return ConfiguredStatement.of(PreparedStatement.of(
            "",
            new InsertValues(SourceName.of("TOPIC"), columns, values)), SessionConfig.of(
        new KsqlConfig(ImmutableMap.of()), ImmutableMap.of()));
  }

  private void givenSourceStreamWithSchema(
      final LogicalSchema schema,
      final SerdeFeatures keyFeatures,
      final SerdeFeatures valFeatures
  ) {
    givenDataSourceWithSchema(TOPIC_NAME, schema, keyFeatures, valFeatures, false, false);
  }

  private void givenSourceTableWithSchema(
      final SerdeFeatures keyFeatures,
      final SerdeFeatures valFeatures
  ) {
    givenDataSourceWithSchema(TOPIC_NAME, SCHEMA, keyFeatures, valFeatures, true, false);
  }

  private void givenDataSourceWithSchema(
      final String topicName,
      final LogicalSchema schema,
      final SerdeFeatures keyFeatures,
      final SerdeFeatures valFeatures,
      final boolean table,
      final boolean isSource
  ) {
    givenDataSourceWithSchema(
        topicName,
        schema,
        keyFeatures,
        valFeatures,
        FormatInfo.of(FormatFactory.KAFKA.name()),
        FormatInfo.of(FormatFactory.JSON.name()),
        table,
        isSource
    );
  }

  private void givenDataSourceWithSchema(
      final String topicName,
      final LogicalSchema schema,
      final SerdeFeatures keyFeatures,
      final SerdeFeatures valFeatures,
      final FormatInfo keyFormat,
      final FormatInfo valueFormat,
      final boolean table,
      final boolean isSource
  ) {
    final KsqlTopic topic = new KsqlTopic(
        topicName,
        KeyFormat.of(keyFormat, keyFeatures, Optional.empty()),
        ValueFormat.of(valueFormat, valFeatures)
    );

    final DataSource dataSource;
    if (table) {
      dataSource = new KsqlTable<>(
          "",
          SourceName.of("TOPIC"),
          schema,
          Optional.empty(),
          false,
          topic,
          isSource
      );
    } else {
      dataSource = new KsqlStream<>(
          "",
          SourceName.of("TOPIC"),
          schema,
          Optional.empty(),
          false,
          topic,
          isSource
      );
    }

    final MetaStoreImpl metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    metaStore.putSource(dataSource, false);

    when(engine.getMetaStore()).thenReturn(metaStore);
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
