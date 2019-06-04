/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InsertValuesExecutorTest {

  private static final KsqlSchema SCHEMA = KsqlSchema.of(SchemaBuilder.struct()
      .field("ROWTIME", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ROWKEY", Schema.OPTIONAL_INT64_SCHEMA)
      .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
      .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
      .build());

  private static final KsqlSchema BIG_SCHEMA = KsqlSchema.of(SchemaBuilder.struct()
      .field("ROWTIME", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ROWKEY", Schema.OPTIONAL_INT64_SCHEMA)
      .field("INT", Schema.OPTIONAL_INT32_SCHEMA)
      .field("COL0", Schema.OPTIONAL_INT64_SCHEMA) // named COL0 for auto-ROWKEY
      .field("DOUBLE", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("BOOLEAN", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("VARCHAR", Schema.OPTIONAL_STRING_SCHEMA)
      .build());

  private static final byte[] KEY = new byte[]{1};
  private static final byte[] VALUE = new byte[]{2};

  private static final String TOPIC_NAME = "topic";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlEngine engine;
  @Mock
  private KsqlSerdeFactory valueSerde;
  @Mock
  private Serde<String> keySerDe;
  @Mock
  private Serializer<String> keySerializer;
  @Mock
  private Serde<Object> rowSerDe;
  @Mock
  private Serializer<Object> rowSerializer;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private Future<?> producerResultFuture;
  @Mock
  private KafkaProducer<byte[], byte[]> producer;
  private Struct expectedRow;

  @Before
  public void setup() {
    when(valueSerde.createSerde(any(), any(), any(), any(), any())).thenReturn(rowSerDe);

    when(keySerDe.serializer()).thenReturn(keySerializer);
    when(rowSerDe.serializer()).thenReturn(rowSerializer);

    when(keySerializer.serialize(any(), any())).thenReturn(KEY);
    when(rowSerializer.serialize(any(), any())).thenReturn(VALUE);

    doReturn(producerResultFuture).when(producer).send(any());

    final KafkaClientSupplier kafkaClientSupplier = mock(KafkaClientSupplier.class);
    when(kafkaClientSupplier.getProducer(any())).thenReturn(producer);

    when(serviceContext.getKafkaClientSupplier()).thenReturn(kafkaClientSupplier);

    givenDataSourceWithSchema(SCHEMA);

    expectedRow = new Struct(SCHEMA.withoutImplicitFields().getSchema())
        .put("COL0", 2L)
        .put("COL1", "str");
  }

  @Test
  public void shouldHandleFullRow() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        SCHEMA.withoutImplicitFields().fields().stream().map(Field::name).collect(Collectors.toList()),
        ImmutableList.of(
            new LongLiteral(2L),
            new StringLiteral("str"))
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, expectedRow);
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInRowtime() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWKEY", "COL0", "COL1"),
        ImmutableList.of(
            new LongLiteral(2L),
            new LongLiteral(2L),
            new StringLiteral("str"))
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, expectedRow);
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInRowKeyFromSpecifiedKey() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("COL0", "COL1"),
        ImmutableList.of(
            new LongLiteral(2L),
            new StringLiteral("str"))
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, expectedRow);
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInFullRowWithNoSchema() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(),
        ImmutableList.of(
            new LongLiteral(2L),
            new LongLiteral(2L),
            new StringLiteral("str"))
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, expectedRow);
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInMissingColumnsWithNulls() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWKEY", "COL0"),
        ImmutableList.of(
            new LongLiteral(2L),
            new LongLiteral(2L))
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, expectedRow.put("COL1", null));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInKeyFromRowKey() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWKEY", "COL1"),
        ImmutableList.of(
            new LongLiteral(2L),
            new StringLiteral("str"))
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, expectedRow);
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleOutOfOrderSchema() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("COL1", "COL0"),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L))
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, expectedRow);
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleAllSortsOfLiterals() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("COL1", "COL0"),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L))
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, expectedRow);
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleNullKey() {
    // Given:
    givenDataSourceWithSchema(BIG_SCHEMA);
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        BIG_SCHEMA.fields().stream().map(Field::name).collect(Collectors.toList()),
        ImmutableList.of(
            new LongLiteral(1L),
            new LongLiteral(2L),
            new IntegerLiteral(0),
            new LongLiteral(2),
            new DoubleLiteral("3.0"),
            new BooleanLiteral("TRUE"),
            new StringLiteral("str"))
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, new Struct(BIG_SCHEMA.withoutImplicitFields().getSchema())
        .put("INT", 0)
        .put("COL0", 2L)
        .put("DOUBLE", 3.0)
        .put("BOOLEAN", true)
        .put("VARCHAR", "str")
    );

    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldAllowUpcast() {
    // Given:
    givenDataSourceWithSchema(SCHEMA);

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("COL0"),
        ImmutableList.of(
            new IntegerLiteral(1)
        )
    );

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);

    // Then:
    verify(rowSerializer).serialize(TOPIC_NAME, expectedRow
        .put("COL0", 1L)
        .put("COL1", null));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldThrowOnProducerSendError() throws ExecutionException, InterruptedException {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        SCHEMA.fields().stream().map(Field::name).collect(Collectors.toList()),
        ImmutableList.of(
            new LongLiteral(1L),
            new LongLiteral(2L),
            new LongLiteral(2L),
            new StringLiteral("str"))
    );

    final Future<?> failure = mock(Future.class);
    when(failure.get()).thenThrow(ExecutionException.class);
    doReturn(failure).when(producer).send(any());

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to insert values into stream/table: ");

    // When:
    new InsertValuesExecutor(() -> -1L).execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldThrowOnSerializingKeyError() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        SCHEMA.fields().stream().map(Field::name).collect(Collectors.toList()),
        ImmutableList.of(
            new LongLiteral(1L),
            new LongLiteral(2L),
            new LongLiteral(2L),
            new StringLiteral("str"))
    );
    when(keySerializer.serialize(any(), any())).thenThrow(new SerializationException("Jibberish!"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not serialize key");

    // When:
    new InsertValuesExecutor(() -> -1L).execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldThrowOnSerializingValueError() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        SCHEMA.fields().stream().map(Field::name).collect(Collectors.toList()),
        ImmutableList.of(
            new LongLiteral(1L),
            new LongLiteral(2L),
            new LongLiteral(2L),
            new StringLiteral("str"))
    );
    when(rowSerializer.serialize(any(), any())).thenThrow(new SerializationException("Jibberish!"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not serialize row");

    // When:
    new InsertValuesExecutor(() -> -1L).execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldThrowIfRowKeyAndKeyDoNotMatch() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWKEY", "COL0"),
        ImmutableList.of(
            new LongLiteral(1L),
            new LongLiteral(2L))
    );

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Expected ROWKEY and COL0 to match");

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldThrowIfNotEnoughValuesSuppliedWithNoSchema() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of(),
        ImmutableList.of(
            new LongLiteral(1L))
    );

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Expected a value for each column");

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldFailOnDowncast() {
    // Given:
    givenDataSourceWithSchema(BIG_SCHEMA);

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("INT"),
        ImmutableList.of(
            new DoubleLiteral("1.1")
        )
    );

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Expected type INT32 for field");

    // When:
    new InsertValuesExecutor(() -> 1L).execute(statement, engine, serviceContext);
  }

  private ConfiguredStatement<InsertValues> givenInsertValues(
      final List<String> columns,
      final List<Expression> values
  ) {
    return ConfiguredStatement.of(
        PreparedStatement.of(
            "",
            new InsertValues(QualifiedName.of("TOPIC"), columns, values)),
        ImmutableMap.of(),
        new KsqlConfig(ImmutableMap.of())
    );
  }

  private void givenDataSourceWithSchema(final KsqlSchema schema) {
    final KsqlTopic topic = new KsqlTopic("TOPIC", TOPIC_NAME, valueSerde, false);
    final DataSource<?> dataSource = new KsqlStream<>(
        "",
        "TOPIC",
        schema,
        KeyField.of(Optional.of("COL0"), Optional.of(schema.getSchema().field("COL0"))),
        new MetadataTimestampExtractionPolicy(),
        topic,
        () -> keySerDe
    );

    final MetaStoreImpl metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    metaStore.putSource(dataSource);

    when(engine.getMetaStore()).thenReturn(metaStore);
  }

}