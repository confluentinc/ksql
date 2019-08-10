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

import static org.hamcrest.Matchers.containsString;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
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
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
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

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class InsertValuesExecutorTest {

  private static final LogicalSchema SINGLE_FIELD_SCHEMA = LogicalSchema.of(SchemaBuilder.struct()
      .field("COL0", Schema.OPTIONAL_STRING_SCHEMA)
      .build());

  private static final LogicalSchema SCHEMA = LogicalSchema.of(SchemaBuilder.struct()
      .field("COL0", Schema.OPTIONAL_STRING_SCHEMA)
      .field("COL1", Schema.OPTIONAL_INT64_SCHEMA)
      .build());

  private static final LogicalSchema BIG_SCHEMA = LogicalSchema.of(SchemaBuilder.struct()
      .field("COL0", Schema.OPTIONAL_STRING_SCHEMA) // named COL0 for auto-ROWKEY
      .field("INT", Schema.OPTIONAL_INT32_SCHEMA)
      .field("BIGINT", Schema.OPTIONAL_INT64_SCHEMA)
      .field("DOUBLE", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("BOOLEAN", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("VARCHAR", Schema.OPTIONAL_STRING_SCHEMA)
      .field("DECIMAL", DecimalUtil.builder(2, 1).build())
      .build());

  private static final byte[] KEY = new byte[]{1};
  private static final byte[] VALUE = new byte[]{2};

  private static final String TOPIC_NAME = "topic";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlEngine engine;
  @Mock
  private KeySerde<Struct> keySerDe;
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

    givenDataSourceWithSchema(SCHEMA, SerdeOption.none(), Optional.of("COL0"));

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
        valueFieldNames(SCHEMA),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldInsertWrappedSingleField() {
    // Given:
    givenDataSourceWithSchema(SINGLE_FIELD_SCHEMA, SerdeOption.none(), Optional.of("COL0"));

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        valueFieldNames(SINGLE_FIELD_SCHEMA),
        ImmutableList.of(new StringLiteral("new"))
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("new"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("new")));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldInsertUnwrappedSingleField() {
    // Given:
    givenDataSourceWithSchema(SINGLE_FIELD_SCHEMA,
        SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES), Optional.of("COL0"));

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        valueFieldNames(SINGLE_FIELD_SCHEMA),
        ImmutableList.of(new StringLiteral("new"))
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("new"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("new")));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInRowtime() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWKEY", "COL0", "COL1"),
        ImmutableList.of(
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleRowTimeWithoutRowKey() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWTIME", "COL0", "COL1"),
        ImmutableList.of(
            new LongLiteral(1234L),
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1234L, KEY, VALUE));
  }

  @Test
  public void shouldFillInRowKeyFromSpecifiedKey() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("COL0", "COL1"),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
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
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInMissingColumnsWithNulls() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWKEY", "COL0"),
        ImmutableList.of(
            new StringLiteral("str"),
            new StringLiteral("str"))
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(Arrays.asList("str", null)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldFillInKeyFromRowKey() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWKEY", "COL1"),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleOutOfOrderSchema() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("COL1", "COL0"),
        ImmutableList.of(
            new LongLiteral(2L),
            new StringLiteral("str")
        )
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleAllSortsOfLiterals() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("COL1", "COL0"),
        ImmutableList.of(
            new LongLiteral(2L),
            new StringLiteral("str"))
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleNullKey() {
    // Given:
    givenDataSourceWithSchema(BIG_SCHEMA, SerdeOption.none(), Optional.of("COL0"));
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allFieldNames(BIG_SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new IntegerLiteral(0),
            new LongLiteral(2),
            new DoubleLiteral(3.0),
            new BooleanLiteral("TRUE"),
            new StringLiteral("str"),
            new StringLiteral("1.2"))
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer)
        .serialize(TOPIC_NAME, new GenericRow(ImmutableList.of(
            "str", 0, 2L, 3.0, true, "str", new BigDecimal(1.2, new MathContext(2))))
        );

    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldAllowUpcast() {
    // Given:
    givenDataSourceWithSchema(SCHEMA, SerdeOption.none(), Optional.of("COL0"));

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("COL0", "COL1"),
        ImmutableList.of(
            new StringLiteral("str"),
            new IntegerLiteral(1)
        )
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("str"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 1L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldThrowOnProducerSendError() throws ExecutionException, InterruptedException {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allFieldNames(SCHEMA),
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

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to insert values into stream/table: ");

    // When:
    executor.execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldThrowOnSerializingKeyError() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allFieldNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );
    when(keySerializer.serialize(any(), any())).thenThrow(new SerializationException("Jibberish!"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectCause(hasMessage(containsString("Could not serialize key")));

    // When:
    executor.execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldThrowOnSerializingValueError() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        allFieldNames(SCHEMA),
        ImmutableList.of(
            new LongLiteral(1L),
            new StringLiteral("str"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );
    when(valueSerializer.serialize(any(), any()))
        .thenThrow(new SerializationException("Jibberish!"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectCause(hasMessage(containsString("Could not serialize row")));

    // When:
    executor.execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldThrowIfRowKeyAndKeyDoNotMatch() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWKEY", "COL0"),
        ImmutableList.of(
            new StringLiteral("foo"),
            new StringLiteral("bar"))
    );

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectCause(hasMessage(containsString("Expected ROWKEY and COL0 to match")));

    // When:
    executor.execute(statement, engine, serviceContext);
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
    expectedException.expectCause(hasMessage(containsString("Expected a value for each column")));

    // When:
    executor.execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldFailOnDowncast() {
    // Given:
    givenDataSourceWithSchema(BIG_SCHEMA, SerdeOption.none(), Optional.of("COL0"));

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("INT"),
        ImmutableList.of(
            new DoubleLiteral(1.1)
        )
    );

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectCause(hasMessage(containsString("Expected type INTEGER for field")));

    // When:
    executor.execute(statement, engine, serviceContext);
  }

  @Test
  public void shouldHandleSourcesWithNoKeyField() {
    // Given:
    givenDataSourceWithSchema(SCHEMA, SerdeOption.none(), Optional.empty());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("ROWKEY", "COL0", "COL1"),
        ImmutableList.of(
            new StringLiteral("key"),
            new StringLiteral("str"),
            new LongLiteral(2L))
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct("key"));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldHandleSourcesWithNoKeyFieldAndNoRowKeyProvided() {
    // Given:
    givenDataSourceWithSchema(SCHEMA, SerdeOption.none(), Optional.empty());

    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        ImmutableList.of("COL0", "COL1"),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L))
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerializer).serialize(TOPIC_NAME, keyStruct(null));
    verify(valueSerializer).serialize(TOPIC_NAME, new GenericRow(ImmutableList.of("str", 2L)));
    verify(producer).send(new ProducerRecord<>(TOPIC_NAME, null, 1L, KEY, VALUE));
  }

  @Test
  public void shouldBuildCorrectSerde() {
    // Given:
    final ConfiguredStatement<InsertValues> statement = givenInsertValues(
        valueFieldNames(SCHEMA),
        ImmutableList.of(
            new StringLiteral("str"),
            new LongLiteral(2L)
        )
    );

    // When:
    executor.execute(statement, engine, serviceContext);

    // Then:
    verify(keySerdeFactory).create(
        FormatInfo.of(Format.KAFKA, Optional.empty()),
        PersistenceSchema.from(SCHEMA.keySchema(), false),
        new KsqlConfig(ImmutableMap.of()),
        srClientFactory,
        "",
        NoopProcessingLogContext.INSTANCE
    );

    verify(valueSerdeFactory).create(
        FormatInfo.of(Format.JSON, Optional.empty()),
        PersistenceSchema.from(SCHEMA.valueSchema(), false),
        new KsqlConfig(ImmutableMap.of()),
        srClientFactory,
        "",
        NoopProcessingLogContext.INSTANCE
    );
  }

  private static ConfiguredStatement<InsertValues> givenInsertValues(
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

  private void givenDataSourceWithSchema(
      final LogicalSchema schema,
      final Set<SerdeOption> serdeOptions,
      final Optional<String> keyField
  ) {
    final KsqlTopic topic = new KsqlTopic(
        TOPIC_NAME,
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.JSON)),
        false
    );

    final KeyField valueKeyField = keyField
        .map(kf -> KeyField.of(kf, schema.findValueField(kf).get()))
        .orElse(KeyField.none());
    final DataSource<?> dataSource = new KsqlStream<>(
        "",
        "TOPIC",
        schema,
        serdeOptions,
        valueKeyField,
        new MetadataTimestampExtractionPolicy(),
        topic
    );

    final MetaStoreImpl metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    metaStore.putSource(dataSource);

    when(engine.getMetaStore()).thenReturn(metaStore);
  }

  private static Struct keyStruct(final String rowKey) {
    final Struct key = new Struct(SCHEMA.keySchema());
    key.put("ROWKEY", rowKey);
    return key;
  }

  private static List<String> valueFieldNames(final LogicalSchema schema) {
    return schema.valueFields().stream().map(Field::name).collect(Collectors.toList());
  }

  private static List<String> allFieldNames(final LogicalSchema schema) {
    return schema.fields().stream().map(Field::name).collect(Collectors.toList());
  }
}
